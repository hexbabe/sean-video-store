// Package camera contains the implementation of the video storage camera component.
package camera

import (
	"context"
	"encoding/base64"
	"errors"

	"github.com/viam-modules/video-store/videostore"
	"go.viam.com/rdk/components/camera"
	"go.viam.com/rdk/logging"
	"go.viam.com/rdk/pointcloud"
	"go.viam.com/rdk/resource"
)

func init() {
	resource.RegisterComponent(
		camera.API,
		Model,
		resource.Registration[camera.Camera, *Config]{Constructor: newComponent})
}

// Model is the model for the video storage camera component.
var Model = resource.ModelNamespace("viam").WithFamily("video").WithModel("storage")

const (
	// Default values for the video storage camera component.
	defaultFramerate      = 20 // frames per second
	defaultSegmentSeconds = 30 // seconds
	defaultStorageSize    = 10 // GB
	defaultVideoBitrate   = 1000000
	defaultVideoPreset    = "medium"
	defaultVideoFormat    = "mp4"
	defaultUploadPath     = ".viam/capture/video-upload"
	defaultStoragePath    = ".viam/video-storage"

	maxGRPCSize     = 1024 * 1024 * 32 // bytes
	deleterInterval = 10               // minutes
	retryInterval   = 1                // seconds
	asyncTimeout    = 60               // seconds
	tempPath        = "/tmp"
	gigabyte        = 1024 * 1024 * 1024
)

type component struct {
	resource.AlwaysRebuild
	name       resource.Name
	logger     logging.Logger
	videostore videostore.VideoStore
}

func newComponent(
	_ context.Context,
	deps resource.Dependencies,
	conf resource.Config,
	logger logging.Logger,
) (camera.Camera, error) {
	config, err := resource.NativeConfig[*Config](conf)
	if err != nil {
		return nil, err
	}

	if err := checkDeps(deps, config, logger); err != nil {
		return nil, err
	}

	// Source camera that provides the frames to be processed.
	// If camera is not available, the component will start
	// without processing frames.
	c, err := camera.FromDependencies(deps, config.Camera)
	if err != nil {
		logger.Errorf("failed to get camera from dependencies, video-store will not be storing video: %s", err)
		c = nil
	}

	vsConfig, err := ToFrameVideoStoreVideoConfig(config, conf.ResourceName().Name, c)
	if err != nil {
		return nil, err
	}
	var vs videostore.VideoStore
	if vsConfig.FramePoller.Camera != nil {
		vs, err = videostore.NewFramePollingVideoStore(vsConfig, logger)
		if err != nil {
			return nil, err
		}
	} else {
		vsConfig.Type = videostore.SourceTypeReadOnly
		vs, err = videostore.NewReadOnlyVideoStore(vsConfig, logger)
		if err != nil {
			return nil, err
		}
	}

	return &component{
		name:       conf.ResourceName(),
		videostore: vs,
		logger:     logger,
	}, nil
}

func (c *component) Name() resource.Name {
	return c.name
}

// DoCommand processes the commands for the video storage camera component.
func (c *component) DoCommand(ctx context.Context, command map[string]interface{}) (map[string]interface{}, error) {
	cmd, ok := command["command"].(string)
	if !ok {
		return nil, errors.New("invalid command type")
	}

	switch cmd {
	// Save command is used to concatenate video clips between the given timestamps.
	// The concatenated video file is then uploaded to the cloud the upload path.
	// The response contains the name of the uploaded file.
	case "save":
		c.logger.Debug("save command received")
		req, err := ToSaveCommand(command)
		if err != nil {
			return nil, err
		}

		res, err := c.videostore.Save(ctx, req)
		if err != nil {
			return nil, err
		}

		ret := map[string]interface{}{
			"command":  "save",
			"filename": res.Filename,
		}

		if req.Async {
			ret["status"] = "async"
		}
		return ret, nil
	case "fetch":
		c.logger.Debug("fetch command received")
		req, err := ToFetchCommand(command)
		if err != nil {
			return nil, err
		}
		res, err := c.videostore.Fetch(ctx, req)
		if err != nil {
			return nil, err
		}
		if len(res.Video) > maxGRPCSize {
			return nil, errors.New("video file size exceeds max grpc size")
		}
		// TODO(seanp): Do we need to encode the video bytes to base64?
		videoBytesBase64 := base64.StdEncoding.EncodeToString(res.Video)
		return map[string]interface{}{
			"command": "fetch",
			"video":   videoBytesBase64,
		}, nil
	case "get-storage-state":
		c.logger.Debug("get-storage-state command received")
		state, err := c.videostore.GetStorageState(ctx)
		if err != nil {
			return nil, err
		}

		disk := map[string]interface{}{
			"storage_used_gb":             float64(state.TotalSizeBytes) / float64(gigabyte),
			"storage_limit_gb":            float64(state.StorageLimitGB),
			"device_storage_remaining_gb": state.DeviceStorageRemainingGB,
			"storage_path":                state.StoragePath,
		}

		videoList := make([]map[string]interface{}, 0, len(state.Ranges))
		for _, timeRange := range state.Ranges {
			fromStr := videostore.FormatDateTimeString(timeRange.Start)
			toStr := videostore.FormatDateTimeString(timeRange.End)
			videoList = append(videoList, map[string]interface{}{
				"from": fromStr,
				"to":   toStr,
			})
		}

		return map[string]interface{}{
			"command":      "get-storage-state",
			"disk_usage":   disk,
			"stored_video": videoList,
		}, nil
	default:
		return nil, errors.New("invalid command")
	}
}

func (c *component) Properties(_ context.Context) (camera.Properties, error) {
	return camera.Properties{}, nil
}

// Close closes the video storage camera component.
func (c *component) Close(_ context.Context) error {
	c.videostore.Close()
	return nil
}

func (c *component) Image(_ context.Context, _ string, _ map[string]interface{}) ([]byte, camera.ImageMetadata, error) {
	return nil, camera.ImageMetadata{}, errors.New("camera.Image not implemented")
}

func (c *component) Images(_ context.Context) ([]camera.NamedImage, resource.ResponseMetadata, error) {
	return nil, resource.ResponseMetadata{}, errors.New("camera.Images not implemented")
}

func (c *component) NextPointCloud(_ context.Context) (pointcloud.PointCloud, error) {
	return nil, errors.New("camera.NextPointCloud not implemented")
}
