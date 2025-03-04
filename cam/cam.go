// Package videostore contains the implementation of the video storage camera component.
package videostore

/*
#include <libavutil/frame.h>
*/
import "C"

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"time"

	"go.viam.com/rdk/components/camera"
	"go.viam.com/rdk/gostream"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/pointcloud"
	"go.viam.com/rdk/resource"
	rutils "go.viam.com/rdk/utils"
	"go.viam.com/utils"
)

// Model is the model for the video storage camera component.
var Model = resource.ModelNamespace("viam").WithFamily("video").WithModel("storage")

const (
	// Default values for the video storage camera component.
	defaultFramerate      = 20 // frames per second
	defaultSegmentSeconds = 30 // seconds
	defaultStorageSize    = 10 // GB
	defaultVideoCodec     = codecH264
	defaultVideoBitrate   = 1000000
	defaultVideoPreset    = "medium"
	defaultVideoFormat    = "mp4"
	defaultUploadPath     = ".viam/capture/video-upload"
	defaultStoragePath    = ".viam/video-storage"
	defaultLogLevel       = "error"

	maxGRPCSize           = 1024 * 1024 * 32 // bytes
	deleterInterval       = 10               // minutes
	retryInterval         = 1                // seconds
	asyncTimeout          = 60               // seconds
	numFetchFrameAttempts = 3                // iterations
	tempPath              = "/tmp"

	mimeTypeYUYV = "image/yuyv422"
)

type videostore struct {
	resource.AlwaysRebuild

	name   resource.Name
	conf   *Config
	logger logging.Logger

	cam         camera.Camera
	latestFrame atomic.Pointer[C.AVFrame]
	workers     *utils.StoppableWorkers

	enc  *encoder
	mh   *mimeHandler
	seg  *segmenter
	conc *concater

	storagePath string
	uploadPath  string
	framerate   int
	yuyv        bool
}

type storage struct {
	SegmentSeconds int    `json:"segment_seconds,omitempty"`
	SizeGB         int    `json:"size_gb"`
	UploadPath     string `json:"upload_path,omitempty"`
	StoragePath    string `json:"storage_path,omitempty"`
}

type video struct {
	Codec   string `json:"codec,omitempty"`
	Bitrate int    `json:"bitrate,omitempty"`
	Preset  string `json:"preset,omitempty"`
	Format  string `json:"format,omitempty"`
}

// Config is the configuration for the video storage camera component.
type Config struct {
	Camera    string  `json:"camera,omitempty"`
	Sync      string  `json:"sync"`
	Storage   storage `json:"storage"`
	Video     video   `json:"video,omitempty"`
	Framerate int     `json:"framerate,omitempty"`
	YUYV      bool    `json:"yuyv,omitempty"`
}

// Validate validates the configuration for the video storage camera component.
func (cfg *Config) Validate(path string) ([]string, error) {
	if cfg.Storage == (storage{}) {
		return nil, utils.NewConfigValidationFieldRequiredError(path, "storage")
	}
	if cfg.Storage.SizeGB == 0 {
		return nil, utils.NewConfigValidationFieldRequiredError(path, "size_gb")
	}
	if cfg.Sync == "" {
		return nil, utils.NewConfigValidationFieldRequiredError(path, "sync")
	}
	if cfg.Framerate < 0 {
		return nil, fmt.Errorf("invalid framerate %d, must be greater than 0", cfg.Framerate)
	}
	// This allows for an implicit camera dependency so we do not need to explicitly
	// add the camera dependency in the config.
	if cfg.Camera != "" {
		return []string{cfg.Camera}, nil
	}
	return []string{}, nil
}

func init() {
	resource.RegisterComponent(
		camera.API,
		Model,
		resource.Registration[camera.Camera, *Config]{
			Constructor: newvideostore,
		})
}

func newvideostore(
	_ context.Context,
	deps resource.Dependencies,
	conf resource.Config,
	logger logging.Logger,
) (camera.Camera, error) {
	newConf, err := resource.NativeConfig[*Config](conf)
	if err != nil {
		return nil, err
	}

	vs := &videostore{
		name:   conf.ResourceName(),
		conf:   newConf,
		logger: logger,
	}

	// Source camera that provides the frames to be processed.
	// If camera is not available, the component will start
	// without processing frames.
	cameraAvailable := true
	vs.cam, err = camera.FromDependencies(deps, newConf.Camera)
	if err != nil {
		vs.logger.Error("failed to get camera from dependencies, video-store will not be storing video", err)
		cameraAvailable = false
	}

	// TODO(seanp): make this configurable
	logLevel := lookupLogID(defaultLogLevel)
	ffmppegLogLevel(logLevel)

	// Create encoder to handle encoding of frames.
	// TODO(seanp): Ignoring codec and using h264 for now until h265 is supported.
	bitrate := defaultVideoBitrate
	preset := defaultVideoPreset
	format := defaultVideoFormat
	vs.framerate = defaultFramerate
	if newConf.Video.Bitrate != 0 {
		bitrate = newConf.Video.Bitrate
	}
	if newConf.Video.Preset != "" {
		preset = newConf.Video.Preset
	}
	if newConf.Video.Format != "" {
		format = newConf.Video.Format
	}
	if newConf.Framerate != 0 {
		vs.framerate = newConf.Framerate
	}
	if newConf.YUYV {
		vs.yuyv = newConf.YUYV
	}

	// Create segmenter to handle segmentation of video stream into clips.
	sizeGB := newConf.Storage.SizeGB
	segmentSeconds := defaultSegmentSeconds
	uploadPath := filepath.Join(getHomeDir(), defaultUploadPath, vs.name.Name)
	storagePath := filepath.Join(getHomeDir(), defaultStoragePath, vs.name.Name)
	if newConf.Storage.SegmentSeconds != 0 {
		segmentSeconds = newConf.Storage.SegmentSeconds
	}
	if newConf.Storage.UploadPath != "" {
		uploadPath = newConf.Storage.UploadPath
	}
	if newConf.Storage.StoragePath != "" {
		storagePath = newConf.Storage.StoragePath
	}

	// Check for data_manager service dependency.
	// TODO(seanp): Check custom_sync_paths if not using default upload_path in config.
	syncFound := false
	for key, dep := range deps {
		if key.Name == newConf.Sync {
			if dep.Name().API.Type.String() != "rdk:service" {
				return nil, fmt.Errorf("sync service %s is not a service", newConf.Sync)
			}
			if dep.Name().API.SubtypeName != "data_manager" {
				return nil, fmt.Errorf("sync service %s is not a data_manager service", newConf.Sync)
			}
			logger.Debugf("found sync service: %s", key.Name)
			syncFound = true
			break
		}
	}
	if !syncFound {
		return nil, fmt.Errorf("sync service %s not found", newConf.Sync)
	}

	// Create concater to handle concatenation of video clips when requested.
	vs.uploadPath = uploadPath
	err = createDir(vs.uploadPath)
	if err != nil {
		return nil, err
	}
	vs.conc, err = newConcater(
		logger,
		storagePath,
		vs.uploadPath,
		segmentSeconds,
	)
	if err != nil {
		return nil, err
	}

	// Only initialize mime handler, encoder, segmenter, and frame processing routines
	// if the source camera is available.
	if cameraAvailable {
		vs.mh = newMimeHandler(logger)
		vs.enc, err = newEncoder(
			logger,
			bitrate,
			preset,
			vs.framerate,
		)
		if err != nil {
			return nil, err
		}
		vs.seg, err = newSegmenter(
			logger,
			sizeGB,
			segmentSeconds,
			storagePath,
			format,
		)
		if err != nil {
			return nil, err
		}
		// Start workers to process frames and clean up storage.
		vs.workers = utils.NewBackgroundStoppableWorkers(vs.fetchFrames, vs.processFrames, vs.deleter)
	}

	return vs, nil
}

func (vs *videostore) Name() resource.Name {
	return vs.name
}

// DoCommand processes the commands for the video storage camera component.
func (vs *videostore) DoCommand(_ context.Context, command map[string]interface{}) (map[string]interface{}, error) {
	cmd, ok := command["command"].(string)
	if !ok {
		return nil, errors.New("invalid command type")
	}

	switch cmd {
	// Save command is used to concatenate video clips between the given timestamps.
	// The concatenated video file is then uploaded to the cloud the upload path.
	// The response contains the name of the uploaded file.
	case "save":
		vs.logger.Debug("save command received")
		from, to, metadata, async, err := validateSaveCommand(command)
		if err != nil {
			return nil, err
		}
		uploadFilePath := generateOutputFilePath(vs.name.Name, formatDateTimeToString(from), metadata, vs.uploadPath)
		uploadFileName := filepath.Base(uploadFilePath)
		switch async {
		case true:
			vs.logger.Debug("running save command asynchronously")
			vs.workers.Add(func(ctx context.Context) {
				vs.asyncSave(ctx, from, to, uploadFilePath)
			})
			return map[string]interface{}{
				"command":  "save",
				"filename": uploadFileName,
				"status":   "async",
			}, nil
		default:
			err = vs.conc.concat(from, to, uploadFilePath)
			if err != nil {
				vs.logger.Error("failed to concat files ", err)
				return nil, err
			}
			return map[string]interface{}{
				"command":  "save",
				"filename": uploadFileName,
			}, nil
		}
	case "fetch":
		vs.logger.Debug("fetch command received")
		from, to, err := validateFetchCommand(command)
		if err != nil {
			return nil, err
		}
		fetchFilePath := generateOutputFilePath(vs.name.Name, formatDateTimeToString(from), "", tempPath)
		err = vs.conc.concat(from, to, fetchFilePath)
		if err != nil {
			vs.logger.Error("failed to concat files ", err)
			return nil, err
		}
		videoSize, err := getFileSize(fetchFilePath)
		if err != nil {
			return nil, err
		}
		if videoSize > maxGRPCSize {
			return nil, errors.New("video file size exceeds max grpc size")
		}
		videoBytes, err := readVideoFile(fetchFilePath)
		if err != nil {
			return nil, err
		}
		vs.logger.Debug("video bytes: ", len(videoBytes))
		// TODO(seanp): Do we need to encode the video bytes to base64?
		videoBytesBase64 := base64.StdEncoding.EncodeToString(videoBytes)
		return map[string]interface{}{
			"command": "fetch",
			"video":   videoBytesBase64,
		}, nil
	default:
		return nil, errors.New("invalid command")
	}
}

func (vs *videostore) Properties(_ context.Context) (camera.Properties, error) {
	return camera.Properties{}, nil
}

// fetchFrames reads frames from the camera at the framerate interval
// and stores the decoded image in the latestFrame atomic pointer.
func (vs *videostore) fetchFrames(ctx context.Context) {
	frameInterval := time.Second / time.Duration(vs.framerate)
	ticker := time.NewTicker(frameInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var mimeTypeReq string
			if vs.yuyv {
				mimeTypeReq = mimeTypeYUYV
			} else {
				mimeTypeReq = rutils.MimeTypeJPEG
			}
			bytes, metadata, err := vs.cam.Image(ctx, mimeTypeReq, nil)
			if err != nil {
				vs.logger.Warn("failed to get frame from camera", err)
				time.Sleep(retryInterval * time.Second)
				continue
			}
			var frame *C.AVFrame
			switch metadata.MimeType {
			case mimeTypeYUYV, mimeTypeYUYV + "+" + rutils.MimeTypeSuffixLazy:
				frame, err = vs.mh.yuyvToYUV420p(bytes)
				if err != nil {
					vs.logger.Error("failed to convert yuyv422 to yuv420p", err)
					continue
				}
			case rutils.MimeTypeJPEG, rutils.MimeTypeJPEG + "+" + rutils.MimeTypeSuffixLazy:
				frame, err = vs.mh.decodeJPEG(bytes)
				if err != nil {
					vs.logger.Error("failed to decode jpeg", err)
					continue
				}
			default:
				vs.logger.Warn("unsupported image format", metadata.MimeType)
				continue
			}
			vs.latestFrame.Store(frame)
		}
	}
}

// processFrames grabs the latest frame, encodes, and writes to the segmenter
// which chunks video stream into clip files inside the storage directory.
func (vs *videostore) processFrames(ctx context.Context) {
	frameInterval := time.Second / time.Duration(vs.framerate)
	ticker := time.NewTicker(frameInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			latestFrame := vs.latestFrame.Load()
			if latestFrame == nil {
				vs.logger.Debug("latest frame is not available yet")
				continue
			}
			result, err := vs.enc.encode(latestFrame)
			if err != nil {
				vs.logger.Debug("failed to encode frame", err)
				continue
			}
			if result.frameDimsChanged {
				vs.logger.Info("reinitializing segmenter due to encoder refresh")
				err = vs.seg.initialize(vs.enc.codecCtx)
				if err != nil {
					vs.logger.Debug("failed to reinitialize segmenter", err)
					// Hack that flags the encoder to reinitialize if segmenter fails to
					// ensure that encoder and segmenter inits are in sync.
					vs.enc.codecCtx = nil
					continue
				}
			}
			err = vs.seg.writeEncodedFrame(result.encodedData, result.pts, result.dts)
			if err != nil {
				vs.logger.Debug("failed to segment frame", err)
				continue
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
			// Perform the deletion of the oldest clip
			err := vs.seg.cleanupStorage()
			if err != nil {
				vs.logger.Error("failed to clean up storage", err)
				continue
			}
		}
	}
}

// asyncSave command will run the concat operation in the background.
// It waits for the segment duration before running to ensure the last segment
// is written to storage before concatenation.
// TODO: (seanp) Optimize this to immediately run as soon as the current segment is completed.
func (vs *videostore) asyncSave(ctx context.Context, from, to time.Time, path string) {
	totalTimeout := time.Duration(asyncTimeout)*time.Second + vs.conc.segmentDur
	ctx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()
	timer := time.NewTimer(vs.conc.segmentDur)
	defer timer.Stop()
	select {
	case <-timer.C:
		vs.logger.Debugf("executing concat for %s", path)
		err := vs.conc.concat(from, to, path)
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
func (vs *videostore) Close(_ context.Context) error {
	if vs.workers != nil {
		vs.workers.Stop()
	}
	if vs.enc != nil {
		vs.enc.close()
	}
	if vs.seg != nil {
		vs.seg.close()
	}
	if vs.mh != nil {
		vs.mh.close()
	}
	return nil
}

// Unimplemented methods for the video storage camera component.
func (vs *videostore) Stream(_ context.Context, _ ...gostream.ErrorHandler) (gostream.VideoStream, error) {
	return nil, errors.New("not implemented")
}

func (vs *videostore) Image(_ context.Context, _ string, _ map[string]interface{}) ([]byte, camera.ImageMetadata, error) {
	// Return a single empty byte to satisfy the Image method
	// avoiding errors from the RenderFrame GRPC call.
	return []byte{0}, camera.ImageMetadata{}, nil
}

func (vs *videostore) Images(_ context.Context) ([]camera.NamedImage, resource.ResponseMetadata, error) {
	return nil, resource.ResponseMetadata{}, errors.New("not implemented")
}

func (vs *videostore) NextPointCloud(_ context.Context) (pointcloud.PointCloud, error) {
	return nil, errors.New("not implemented")
}
