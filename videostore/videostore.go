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
	"go.viam.com/utils"
)

// Model is the model for the video storage camera component.
var Model = resource.ModelNamespace("viam").WithFamily("video").WithModel("storage")

const (
	// Constant values for the video storage camera component.
	segmentSeconds = 30 // seconds
	videoFormat    = "mp4"

	deleterInterval = 1  // minutes
	retryInterval   = 1  // seconds
	asyncTimeout    = 60 // seconds
	tempPath        = "/tmp"

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
	indexer      *Indexer
}

// VideoStore stores video and provides APIs to request the stored video.
type VideoStore interface {
	Fetch(ctx context.Context, r *FetchRequest) (*FetchResponse, error)
	Save(ctx context.Context, r *SaveRequest) (*SaveResponse, error)
	Close()
	GetStorageState() (*StorageState, error)
	GetConfig() Config
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
	StorageUsedGB            float64 `json:"storage_used_gb"`
	StorageLimitGB           float64 `json:"storage_limit_gb"`
	DeviceStorageRemainingGB float64 `json:"device_storage_remaining_gb"`
}

// VideoTimeRange represents a time range for stored video.
type VideoTimeRange struct {
	From string `json:"from"`
	To   string `json:"to"`
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

	vs.indexer, err = NewIndexer(config.Storage.StoragePath, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create indexer: %w", err)
	}

	vs.workers.Add(func(ctx context.Context) { vs.indexer.Run(ctx) })
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
	vs.workers.Add(vs.deleter)

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

	indexer, err := NewIndexer(config.Storage.StoragePath, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create indexer for read-only videostore: %w", err)
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

	indexer, err := NewIndexer(config.Storage.StoragePath, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create indexer: %w", err)
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

	vs.workers.Add(func(ctx context.Context) { vs.indexer.Run(ctx) })
	vs.workers.Add(vs.deleter)
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

// deleter is a go routine that cleans up old clips if storage is full. Runs on interval
// and deletes the oldest clip until the storage size is below the configured max.
func (vs *videostore) deleter(ctx context.Context) {
	ticker := time.NewTicker(deleterInterval * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Perform the deletion of the oldest clips
			if err := vs.cleanupStorage(); err != nil {
				vs.logger.Errorw("failed to clean up storage using indexer", "error", err)
				continue
			}
		}
	}
}

func (vs *videostore) cleanupStorage() error {
	maxStorageSizeBytes := int64(vs.config.Storage.SizeGB) * gigabyte
	currentSizeBytes, err := getDirectorySize(vs.config.Storage.StoragePath)
	if err != nil {
		return fmt.Errorf("failed to get directory size: %w", err)
	}
	if currentSizeBytes < maxStorageSizeBytes {
		return nil
	}

	bytesToDelete := currentSizeBytes - maxStorageSizeBytes
	var bytesDeleted int64
	var deletedFilePaths []string // accumulate paths successfully deleted from disk to be removed from index

	allSegments, err := vs.indexer.GetSegmentsAscTime()
	if err != nil {
		return fmt.Errorf("failed to get segments from indexer: %w", err)
	}

	if len(allSegments) == 0 {
		vs.logger.Warn("cleanup required based on disk size, but indexer found no segments. Index might be corrupted or empty.")
		return nil
	}

	vs.logger.Debugf("retrieved %d segments from indexer, starting deletion process", len(allSegments))

	// Iterate over the segments, deleting oldest first until enough space is freed
	for _, segment := range allSegments {
		vs.logger.Debugf("Attempting to delete oldest segment: %s (size: %d bytes)", segment.FilePath, segment.SizeBytes)
		err := os.Remove(segment.FilePath)
		if err != nil {
			if os.IsNotExist(err) {
				vs.logger.Warnf("Segment file %s not found on disk, but was in index. Marking for index removal.", segment.FilePath)
				deletedFilePaths = append(deletedFilePaths, segment.FilePath)
			} else {
				vs.logger.Errorw("Failed to delete segment file from disk", "path", segment.FilePath, "error", err)
				continue
			}
		} else { // deletion successful
			vs.logger.Infof("Deleted segment file: %s", segment.FilePath)
			bytesDeleted += segment.SizeBytes
			deletedFilePaths = append(deletedFilePaths, segment.FilePath)
		}

		if bytesDeleted >= bytesToDelete {
			vs.logger.Debugf("deleted enough bytes (%d >= %d), stopping deletion loop", bytesDeleted, bytesToDelete)
			break
		}
	}

	if len(deletedFilePaths) > 0 {
		vs.logger.Debugf("removing %d segments from index immediately after disk deletion attempts", len(deletedFilePaths))
		if removeErr := vs.indexer.RemoveIndexedFiles(deletedFilePaths); removeErr != nil {
			return fmt.Errorf("failed to update index after deleting/checking files: %w", removeErr)
		}
	}

	vs.logger.Debugf(
		"cleanup finished. Targeted %d bytes for deletion. "+
			"Actually deleted approx %d bytes from disk over %d files.",
		bytesToDelete,
		bytesDeleted,
		len(deletedFilePaths),
	)
	return nil
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

// Close stops all background processes and releases resources.
func (vs *videostore) Close() {
	var errs []error

	if vs.workers != nil {
		vs.workers.Stop()
	}

	if vs.indexer != nil {
		if err := vs.indexer.Close(); err != nil {
			vs.logger.Errorw("error closing indexer", "error", err)
			errs = append(errs, fmt.Errorf("indexer close: %w", err))
		}
	}

	if vs.rawSegmenter != nil {
		if err := vs.rawSegmenter.Close(); err != nil {
			vs.logger.Errorw("error closing raw segmenter", "error", err)
			errs = append(errs, fmt.Errorf("raw segmenter close: %w", err))
		}
	}

	// Log composite error for debugging but don't return since this is a terminal operation
	if len(errs) > 0 {
		errMsg := "errors during close: "
		for i, err := range errs {
			if i > 0 {
				errMsg += ", "
			}
			errMsg += err.Error()
		}
		vs.logger.Errorw(errMsg)
	}
}

// GetStorageState returns the current storage state for this VideoStore using the indexer.
func (vs *videostore) GetStorageState() (*StorageState, error) {
	if vs.indexer == nil {
		return nil, errors.New("indexer not initialized")
	}

	indexerState, err := vs.indexer.GetStorageState()
	if err != nil {
		return nil, fmt.Errorf("failed to get storage state from indexer: %w", err)
	}

	return &indexerState, nil
}

// GetConfig returns the configuration for the VideoStore.
func (vs *videostore) GetConfig() Config {
	return vs.config
}
