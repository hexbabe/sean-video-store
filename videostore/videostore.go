// Package videostore contains the implementation of the video storage camera component.
package videostore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"go.viam.com/rdk/components/camera"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/resource"
	rutils "go.viam.com/rdk/utils"
	"go.viam.com/rdk/utils/diskusage"
	"go.viam.com/utils"
)

// Allows for mocking in tests.
var duStatfs = diskusage.Statfs

// Model is the model for the video storage camera component.
var Model = resource.ModelNamespace("viam").WithFamily("video").WithModel("storage")

const (
	// Constant values for the video storage camera component.
	segmentSeconds = 30 // seconds
	videoFormat    = "mp4"

	retryInterval = 1  // seconds
	asyncTimeout  = 60 // seconds
	tempPath      = "/tmp"

	// TimeFormat is how we format the timestamp in output filenames and do commands.
	TimeFormat = "2006-01-02_15-04-05"
)

var presets = map[string]struct{}{
	"ultrafast": {},
	"superfast": {},
	"veryfast":  {},
	"faster":    {},
	"fast":      {},
	"medium":    {},
	"slow":      {},
	"slower":    {},
	"veryslow":  {},
}

type videostore struct {
	latestFrame *atomic.Value
	typ         SourceType
	config      Config
	logger      logging.Logger

	workers *utils.StoppableWorkers

	rawSegmenter *RawSegmenter
	concater     *concater
	indexer      *indexer
}

// VideoStore stores video and provides APIs to request the stored video.
type VideoStore interface {
	Fetch(ctx context.Context, r *FetchRequest) (*FetchResponse, error)
	Save(ctx context.Context, r *SaveRequest) (*SaveResponse, error)
	Close()
	GetStorageState(ctx context.Context) (*StorageState, error)
}

// CodecType repreasents a codec.
type CodecType int

const (
	// CodecTypeUnknown is an invalid type.
	CodecTypeUnknown CodecType = iota
	// CodecTypeH264 represents h264 codec.
	CodecTypeH264
	// CodecTypeH265 represents h265 codec.
	CodecTypeH265
)

func (t CodecType) String() string {
	switch t {
	case CodecTypeUnknown:
		return "CodecTypeUnknown"
	case CodecTypeH264:
		return "CodecTypeH264"
	case CodecTypeH265:
		return "CodecTypeH265"
	default:
		return "CodecTypeUnknown"
	}
}

// RTPVideoStore stores video derived from RTP packets and provides APIs to request the stored video.
type RTPVideoStore interface {
	VideoStore
	Segmenter() *RawSegmenter
}

// SaveRequest is the request to the Save method.
type SaveRequest struct {
	From     time.Time
	To       time.Time
	Metadata string
	Async    bool
}

// SaveResponse is the response to the Save method.
type SaveResponse struct {
	Filename string
}

// Validate returns an error if the SaveRequest is invalid.
func (r *SaveRequest) Validate() error {
	if r.From.After(r.To) {
		return errors.New("'from' timestamp is after 'to' timestamp")
	}
	if r.To.After(time.Now()) {
		return errors.New("'to' timestamp is in the future")
	}
	return nil
}

// FetchRequest is the request to the Fetch method.
type FetchRequest struct {
	From time.Time
	To   time.Time
}

// FetchResponse is the resonse to the Fetch method.
type FetchResponse struct {
	Video []byte
}

// Validate returns an error if the FetchRequest is invalid.
func (r *FetchRequest) Validate() error {
	if r.From.After(r.To) {
		return errors.New("'from' timestamp is after 'to' timestamp")
	}
	return nil
}

// DiskUsageState represents disk usage information.
type DiskUsageState struct {
	StorageUsedGB            float64
	StorageLimitGB           float64
	DeviceStorageRemainingGB float64
}

// StorageState summarizes the state of the stored video segments and storage config info.
type StorageState struct {
	videoRanges
	StorageLimitGB           int
	DeviceStorageRemainingGB float64
	StoragePath              string
}

// NewFramePollingVideoStore returns a VideoStore that stores video it encoded from polling frames from a camera.Camera.
func NewFramePollingVideoStore(config Config, logger logging.Logger) (VideoStore, error) {
	if config.Type != SourceTypeFrame {
		return nil, fmt.Errorf("config type must be %s", SourceTypeFrame)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}

	vs := &videostore{
		latestFrame: &atomic.Value{},
		typ:         config.Type,
		logger:      logger,
		config:      config,
		workers:     utils.NewBackgroundStoppableWorkers(),
	}
	if err := createDir(config.Storage.StoragePath); err != nil {
		return nil, err
	}
	err := createDir(vs.config.Storage.UploadPath)
	if err != nil {
		return nil, err
	}

	// Create concater to handle concatenation of video clips when requested.
	vs.concater, err = newConcater(
		config.Storage.StoragePath,
		config.Storage.UploadPath,
		logger,
	)
	if err != nil {
		return nil, err
	}

	encoder, err := newEncoder(
		vs.config.Encoder,
		vs.config.FramePoller.Framerate,
		vs.config.Storage.StoragePath,
		logger,
	)
	if err != nil {
		return nil, err
	}

	if err := encoder.initialize(); err != nil {
		logger.Warnf("encoder init failed: %s", err.Error())
		return nil, err
	}

	vs.indexer = newIndexer(config.Storage.StoragePath, config.Storage.SizeGB, logger)
	err = vs.indexer.setup()
	if err != nil {
		return nil, fmt.Errorf("failed to set up indexer: %w", err)
	}

	vs.workers.Add(func(ctx context.Context) { vs.indexer.run(ctx) })
	vs.workers.Add(func(ctx context.Context) {
		vs.fetchFrames(
			ctx,
			config.FramePoller)
	})
	vs.workers.Add(func(ctx context.Context) {
		vs.processFrames(
			ctx,
			config.FramePoller.Framerate,
			encoder)
	})

	return vs, nil
}

// NewReadOnlyVideoStore returns a VideoStore that can return stored video but doesn't create new video segements.
func NewReadOnlyVideoStore(config Config, logger logging.Logger) (VideoStore, error) {
	if config.Type != SourceTypeReadOnly {
		return nil, fmt.Errorf("config type must be %s", SourceTypeReadOnly)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}

	if err := createDir(config.Storage.StoragePath); err != nil {
		return nil, err
	}
	if err := createDir(config.Storage.UploadPath); err != nil {
		return nil, err
	}

	concater, err := newConcater(
		config.Storage.StoragePath,
		config.Storage.UploadPath,
		logger,
	)
	if err != nil {
		return nil, err
	}

	indexer := newIndexer(config.Storage.StoragePath, config.Storage.SizeGB, logger)
	err = indexer.setup()
	if err != nil {
		return nil, fmt.Errorf("failed to set up indexer for read-only videostore: %w", err)
	}

	return &videostore{
		typ:      config.Type,
		concater: concater,
		indexer:  indexer,
		logger:   logger,
		config:   config,
		workers:  utils.NewBackgroundStoppableWorkers(),
	}, nil
}

// NewRTPVideoStore returns a VideoStore that stores video it receives from the caller.
func NewRTPVideoStore(config Config, logger logging.Logger) (RTPVideoStore, error) {
	if config.Type != SourceTypeRTP {
		return nil, fmt.Errorf("config type must be %s", SourceTypeRTP)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}

	if err := createDir(config.Storage.StoragePath); err != nil {
		return nil, err
	}
	if err := createDir(config.Storage.UploadPath); err != nil {
		return nil, err
	}

	concater, err := newConcater(
		config.Storage.StoragePath,
		config.Storage.UploadPath,
		logger,
	)
	if err != nil {
		return nil, err
	}

	rawSegmenter, err := newRawSegmenter(
		config.Storage.StoragePath,
		logger,
	)
	if err != nil {
		return nil, err
	}

	indexer := newIndexer(config.Storage.StoragePath, config.Storage.SizeGB, logger)
	err = indexer.setup()
	if err != nil {
		return nil, fmt.Errorf("failed to set up indexer: %w", err)
	}

	vs := &videostore{
		typ:          config.Type,
		concater:     concater,
		rawSegmenter: rawSegmenter,
		indexer:      indexer,
		logger:       logger,
		config:       config,
		workers:      utils.NewBackgroundStoppableWorkers(),
	}

	vs.workers.Add(func(ctx context.Context) { vs.indexer.run(ctx) })
	return vs, nil
}

func (vs *videostore) Segmenter() *RawSegmenter {
	return vs.rawSegmenter
}

func (vs *videostore) Fetch(_ context.Context, r *FetchRequest) (*FetchResponse, error) {
	// Convert incoming local times to UTC for consistent timestamp handling
	// All internal operations and segmenter timestamps are in UTC
	r.From = r.From.UTC()
	r.To = r.To.UTC()
	if err := r.Validate(); err != nil {
		return nil, err
	}
	vs.logger.Debug("fetch command received and validated")
	fetchFilePath := generateOutputFilePath(
		vs.config.Storage.OutputFileNamePrefix,
		r.From,
		"",
		tempPath)

	// Always attempt to remove the concat file after the operation.
	// This handles error cases in Concat where it fails in the middle
	// of writing.
	defer func() {
		if _, statErr := os.Stat(fetchFilePath); os.IsNotExist(statErr) {
			vs.logger.Debugf("temporary file (%s) does not exist, skipping removal", fetchFilePath)
			return
		}
		if err := os.Remove(fetchFilePath); err != nil {
			vs.logger.Warnf("failed to delete temporary file (%s): %v", fetchFilePath, err)
		}
	}()
	if err := vs.concater.Concat(r.From, r.To, fetchFilePath); err != nil {
		vs.logger.Error("failed to concat files ", err)
		return nil, err
	}
	videoBytes, err := readVideoFile(fetchFilePath)
	if err != nil {
		return nil, err
	}
	return &FetchResponse{Video: videoBytes}, nil
}

func (vs *videostore) Save(_ context.Context, r *SaveRequest) (*SaveResponse, error) {
	// Convert incoming local times to UTC for consistent timestamp handling
	// All internal operations and segmenter timestamps are in UTC
	r.From = r.From.UTC()
	r.To = r.To.UTC()
	if err := r.Validate(); err != nil {
		return nil, err
	}
	vs.logger.Debug("save command received and validated")
	uploadFilePath := generateOutputFilePath(
		vs.config.Storage.OutputFileNamePrefix,
		r.From,
		r.Metadata,
		vs.config.Storage.UploadPath,
	)
	uploadFileName := filepath.Base(uploadFilePath)
	if r.Async {
		vs.logger.Debug("running save command asynchronously")
		vs.workers.Add(func(ctx context.Context) {
			vs.asyncSave(ctx, r.From, r.To, uploadFilePath)
		})
		return &SaveResponse{Filename: uploadFileName}, nil
	}

	if err := vs.concater.Concat(r.From, r.To, uploadFilePath); err != nil {
		vs.logger.Error("failed to concat files ", err)
		return nil, err
	}
	return &SaveResponse{Filename: uploadFileName}, nil
}

func (vs *videostore) fetchFrames(ctx context.Context, framePoller FramePollerConfig,
) {
	frameInterval := time.Second / time.Duration(framePoller.Framerate)
	ticker := time.NewTicker(frameInterval)
	defer ticker.Stop()
	var (
		data     []byte
		metadata camera.ImageMetadata
		err      error
	)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			data, metadata, err = framePoller.Camera.Image(ctx, rutils.MimeTypeJPEG, nil)
			if err != nil {
				vs.logger.Warn("failed to get frame from camera: ", err)
				time.Sleep(retryInterval * time.Second)
				continue
			}
			if actualMimeType, _ := rutils.CheckLazyMIMEType(metadata.MimeType); actualMimeType != rutils.MimeTypeJPEG {
				vs.logger.Warnf("expected image in mime type %s got %s: ", rutils.MimeTypeJPEG, actualMimeType)
				continue
			}
			vs.latestFrame.Store(data)
		}
	}
}

func (vs *videostore) processFrames(
	ctx context.Context,
	framerate int,
	encoder *encoder,
) {
	defer encoder.close()
	frameInterval := time.Second / time.Duration(framerate)
	ticker := time.NewTicker(frameInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			frame, ok := vs.latestFrame.Load().([]byte)
			if ok && frame != nil {
				encoder.encode(frame)
			}
		}
	}
}

// asyncSave command will run the concat operation in the background.
// It waits for the segment duration before running to ensure the last segment
// is written to storage before concatenation.
// TODO: (seanp) Optimize this to immediately run as soon as the current segment is completed.
func (vs *videostore) asyncSave(ctx context.Context, from, to time.Time, path string) {
	segmentDur := time.Duration(segmentSeconds) * time.Second
	totalTimeout := time.Duration(asyncTimeout)*time.Second + segmentDur
	ctx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()
	timer := time.NewTimer(segmentDur)
	defer timer.Stop()
	select {
	case <-timer.C:
		vs.logger.Debugf("executing concat for %s", path)
		err := vs.concater.Concat(from, to, path)
		if err != nil {
			vs.logger.Error("failed to concat files ", err)
		}
		return
	case <-ctx.Done():
		vs.logger.Error("asyncSave operation cancelled or timed out")
		return
	}
}

// Close closes the video storage camera component.
func (vs *videostore) Close() {
	if vs.workers != nil {
		vs.workers.Stop()
	}

	if vs.indexer != nil {
		if err := vs.indexer.close(); err != nil {
			vs.logger.Errorw("error closing indexer", "error", err)
		}
	}

	if vs.rawSegmenter != nil {
		if err := vs.rawSegmenter.Close(); err != nil {
			vs.logger.Errorw("error closing raw segmenter", "error", err)
		}
	}
}

// GetStorageState returns the current storage state for this VideoStore using the indexer.
func (vs *videostore) GetStorageState(ctx context.Context) (*StorageState, error) {
	if vs.indexer == nil {
		return nil, errors.New("indexer not initialized")
	}

	videoRangesResult, err := vs.indexer.getVideoList(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage state from indexer: %w", err)
	}

	storageState := StorageState{
		videoRanges: videoRangesResult,
	}
	storageState.StorageLimitGB = vs.config.Storage.SizeGB
	storageState.StoragePath = vs.config.Storage.StoragePath

	fsUsage, err := duStatfs(vs.config.Storage.StoragePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get filesystem stats for remaining space: %w", err)
	}
	storageState.DeviceStorageRemainingGB = float64(fsUsage.AvailableBytes) / float64(gigabyte)

	return &storageState, nil
}
