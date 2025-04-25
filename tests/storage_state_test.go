package videostore_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/viam-modules/video-store/videostore"
	"go.viam.com/rdk/components/camera"
	"go.viam.com/test"
)

func TestGetStorageStateDoCommand(t *testing.T) {
	storagePath, err := filepath.Abs(artifactStoragePath)
	test.That(t, err, test.ShouldBeNil)

	fullModuleBinPath, err := getModuleBinPath()
	test.That(t, err, test.ShouldBeNil)

	config := fmt.Sprintf(`
	{
		"components": [
			{
				"name": "%s",
				"namespace": "rdk",
				"type": "camera",
				"model": "viam:video:storage",
				"attributes": {
					"camera": "fake-cam-1",
					"sync": "data_manager-1",
					"storage": {
						"size_gb": 10,
						"upload_path": "%s",
						"storage_path": "%s"
					},
					"framerate": 30,
					"video": {
						"codec": "h264",
						"bitrate": 1000000,
						"preset": "ultrafast",
						"format": "mp4"
					}
				},
				"depends_on": [
					"fake-cam-1",
					"data_manager-1"
				]
			},
			{
				"name": "fake-cam-1",
				"namespace": "rdk",
				"type": "camera",
				"model": "fake",
				"attributes": {}
			}
		],
		"services": [
			{
				"name": "data_manager-1",
				"namespace": "rdk",
				"type": "data_manager",
				"attributes": {
					"additional_sync_paths": [],
					"capture_disabled": true,
					"sync_interval_mins": 0.1,
					"capture_dir": "",
					"tags": []
				}
			}
		],
		"modules": [
			{
				"type": "local",
				"name": "video-storage",
				"executable_path": "%s",
				"log_level": "debug"
			}
		]
	}`, videoStoreComponentName, testUploadPath, storagePath, fullModuleBinPath)

	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	r, err := setupViamServer(timeoutCtx, config)
	test.That(t, err, test.ShouldBeNil)
	defer r.Close(timeoutCtx)

	vs, err := camera.FromRobot(r, videoStoreComponentName)
	test.That(t, err, test.ShouldBeNil)

	// Allow segmenter to write at least one file
	time.Sleep(5 * time.Second)

	// Call get-storage-state
	cmd := map[string]interface{}{"command": "get-storage-state"}
	res, err := vs.DoCommand(timeoutCtx, cmd)
	test.That(t, err, test.ShouldBeNil)

	// Validate disk_usage
	disk, ok := res["disk_usage"].(map[string]interface{})
	test.That(t, ok, test.ShouldBeTrue)
	used, ok := disk["storage_used_gb"].(float64)
	test.That(t, ok, test.ShouldBeTrue)
	test.That(t, used, test.ShouldBeGreaterThan, 0.0)
	limit, ok := disk["storage_limit_gb"].(float64)
	test.That(t, ok, test.ShouldBeTrue)
	test.That(t, limit, test.ShouldEqual, float64(10))
	remaining, ok := disk["device_storage_remaining_gb"].(float64)
	test.That(t, ok, test.ShouldBeTrue)
	test.That(t, remaining, test.ShouldBeGreaterThan, 0.0)

	// Validate stored_video ranges
	videoList, ok := res["stored_video"].([]interface{})
	test.That(t, ok, test.ShouldBeTrue)
	test.That(t, len(videoList), test.ShouldBeGreaterThan, 0)
	for _, item := range videoList {
		entry, ok := item.(map[string]interface{})
		test.That(t, ok, test.ShouldBeTrue)
		fromStr, ok := entry["from"].(string)
		test.That(t, ok, test.ShouldBeTrue)
		toStr, ok := entry["to"].(string)
		test.That(t, ok, test.ShouldBeTrue)

		// Must be UTC strings ending with Z
		test.That(t, strings.HasSuffix(fromStr, "Z"), test.ShouldBeTrue)
		test.That(t, strings.HasSuffix(toStr, "Z"), test.ShouldBeTrue)

		// Parse times and ensure from < to
		fromTime, err := time.Parse(videostore.TimeFormat, strings.TrimSuffix(fromStr, "Z"))
		test.That(t, err, test.ShouldBeNil)
		toTime, err := time.Parse(videostore.TimeFormat, strings.TrimSuffix(toStr, "Z"))
		test.That(t, err, test.ShouldBeNil)
		test.That(t, fromTime.Before(toTime), test.ShouldBeTrue)
	}
}
