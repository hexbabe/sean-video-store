package videostore_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	vsutils "github.com/viam-modules/video-store/videostore/utils"
	"go.viam.com/rdk/components/camera"
	"go.viam.com/test"
)

// TODO(RSDK-10317): tests only pass on machines with UTC timezone.
const (
	validFromTimestamp         = "2024-09-06_15-00-33"
	validToTimestamp           = "2024-09-06_15-01-33"
	validFromTimestampStrftime = "2024-09-06_15-00-02"
	validToTimestampUnixInt    = "2024-09-06_15-00-04"
	invalidFromTimestamp       = "2024-09-06_14-00-03"
	invalidToTimestamp         = "3024-09-06_15-00-33"
	invalidDatetimeFormat      = "2024/09/06 15:01:33"
	validFromTimestampUTC      = "2024-09-06_15-00-33Z"
	validToTimestampUTC        = "2024-09-06_15-01-33Z"
)

func TestSaveDoCommand(t *testing.T) {
	storagePath, err := filepath.Abs(artifactStoragePath)
	test.That(t, err, test.ShouldBeNil)
	fullModuleBinPath, err := getModuleBinPath()
	test.That(t, err, test.ShouldBeNil)

	config1 := fmt.Sprintf(`
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

	// Valid time range
	saveCmd1 := map[string]interface{}{
		"command":  "save",
		"from":     validFromTimestamp,
		"to":       validToTimestamp,
		"metadata": "test-metadata",
	}

	// Invalid time range
	saveCmd2 := map[string]interface{}{
		"command":  "save",
		"from":     invalidFromTimestamp,
		"to":       validToTimestamp,
		"metadata": "test-metadata",
	}

	// Invalid datetime format
	saveCmd3 := map[string]interface{}{
		"command":  "save",
		"from":     validFromTimestamp,
		"to":       invalidDatetimeFormat,
		"metadata": "test-metadata",
	}

	// Valid async save
	saveCmd4 := map[string]interface{}{
		"command":  "save",
		"from":     validFromTimestamp,
		"to":       validToTimestamp,
		"metadata": "test-metadata",
		"async":    true,
	}

	// Invalid async save with future timestamp
	saveCmd5 := map[string]interface{}{
		"command":  "save",
		"from":     validFromTimestamp,
		"to":       invalidToTimestamp,
		"metadata": "test-metadata",
		"async":    true,
	}

	// Valid UTC time range
	saveCmdUTC := map[string]interface{}{
		"command":  "save",
		"from":     validFromTimestampUTC,
		"to":       validToTimestampUTC,
		"metadata": "test-metadata-utc",
	}

	t.Run("Test Save DoCommand Valid Range", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		r, err := setupViamServer(timeoutCtx, config1)
		test.That(t, err, test.ShouldBeNil)
		defer r.Close(timeoutCtx)
		vs, err := camera.FromRobot(r, videoStoreComponentName)
		test.That(t, err, test.ShouldBeNil)
		res, err := vs.DoCommand(timeoutCtx, saveCmd1)
		test.That(t, err, test.ShouldBeNil)
		filename, ok := res["filename"].(string)
		test.That(t, ok, test.ShouldBeTrue)

		// Calculate expected timestamp from fromTime
		fromTime, err := time.Parse(vsutils.TimeFormat, validFromTimestamp)
		test.That(t, err, test.ShouldBeNil)
		expectedFilename := fmt.Sprintf("%s_%s_%s.mp4",
			videoStoreComponentName,
			fromTime.Format(vsutils.TimeFormat),
			"test-metadata")
		test.That(t, filename, test.ShouldEqual, expectedFilename)
		test.That(t, filename, test.ShouldContainSubstring, "test-metadata")

		filePath := filepath.Join(testUploadPath, filename)
		testVideoPlayback(t, filePath)
		testVideoDuration(t, filePath, 60)
	})

	t.Run("Test Save DoCommand Invalid Range", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		r, err := setupViamServer(timeoutCtx, config1)
		test.That(t, err, test.ShouldBeNil)
		defer r.Close(timeoutCtx)
		vs, err := camera.FromRobot(r, videoStoreComponentName)
		test.That(t, err, test.ShouldBeNil)
		_, err = vs.DoCommand(timeoutCtx, saveCmd2)
		test.That(t, err, test.ShouldNotBeNil)
		test.That(t, err.Error(), test.ShouldContainSubstring, "range")
	})

	t.Run("Test Save DoCommand Invalid Datetime Format", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		r, err := setupViamServer(timeoutCtx, config1)
		test.That(t, err, test.ShouldBeNil)
		defer r.Close(timeoutCtx)
		vs, err := camera.FromRobot(r, videoStoreComponentName)
		test.That(t, err, test.ShouldBeNil)
		_, err = vs.DoCommand(timeoutCtx, saveCmd3)
		test.That(t, err, test.ShouldNotBeNil)
		test.That(t, err.Error(), test.ShouldContainSubstring, "parsing time")
	})

	t.Run("Test Save DoCommand Async", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		r, err := setupViamServer(timeoutCtx, config1)
		test.That(t, err, test.ShouldBeNil)
		defer r.Close(timeoutCtx)
		vs, err := camera.FromRobot(r, videoStoreComponentName)
		test.That(t, err, test.ShouldBeNil)
		res, err := vs.DoCommand(timeoutCtx, saveCmd4)
		test.That(t, err, test.ShouldBeNil)
		_, ok := res["filename"].(string)
		test.That(t, ok, test.ShouldBeTrue)
	})

	t.Run("Test Save DoCommand Async With Future Timestamp", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		r, err := setupViamServer(timeoutCtx, config1)
		test.That(t, err, test.ShouldBeNil)
		defer r.Close(timeoutCtx)
		vs, err := camera.FromRobot(r, videoStoreComponentName)
		test.That(t, err, test.ShouldBeNil)
		_, err = vs.DoCommand(timeoutCtx, saveCmd5)
		test.That(t, err, test.ShouldNotBeNil)
		test.That(t, err.Error(), test.ShouldContainSubstring, "'to' timestamp is in the future")
	})

	t.Run("Test Save DoCommand Valid UTC Range", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		r, err := setupViamServer(timeoutCtx, config1)
		test.That(t, err, test.ShouldBeNil)
		defer r.Close(timeoutCtx)
		vs, err := camera.FromRobot(r, videoStoreComponentName)
		test.That(t, err, test.ShouldBeNil)
		res, err := vs.DoCommand(timeoutCtx, saveCmdUTC)
		test.That(t, err, test.ShouldBeNil)
		filename, ok := res["filename"].(string)
		test.That(t, ok, test.ShouldBeTrue)

		// Calculate expected timestamp from fromTime (parsed as UTC, converted to local for filename)
		fromTimeUTC, err := time.Parse(vsutils.TimeFormat, "2024-09-06_15-00-33") // Parse without Z
		test.That(t, err, test.ShouldBeNil)
		fromTimeLocal := fromTimeUTC.Local() // Convert to local time for filename generation
		expectedFilename := fmt.Sprintf("%s_%s_%s.mp4",
			videoStoreComponentName,
			fromTimeLocal.Format(vsutils.TimeFormat),
			"test-metadata-utc")
		test.That(t, filename, test.ShouldEqual, expectedFilename)
		test.That(t, filename, test.ShouldContainSubstring, "test-metadata-utc")

		filePath := filepath.Join(testUploadPath, filename)
		testVideoPlayback(t, filePath)
		testVideoDuration(t, filePath, 60)
	})

	t.Run("Test leftover concat txt files are cleaned up", func(t *testing.T) {
		leftoverConcatTxtPath := filepath.Join("/tmp", "concat_test1.txt")
		file, err := os.Create(leftoverConcatTxtPath)
		test.That(t, err, test.ShouldBeNil)
		file.Close()
		_, err = os.Stat(leftoverConcatTxtPath)
		test.That(t, os.IsNotExist(err), test.ShouldBeFalse)
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		r, err := setupViamServer(timeoutCtx, config1)
		test.That(t, err, test.ShouldBeNil)
		defer r.Close(timeoutCtx)
		_, err = camera.FromRobot(r, videoStoreComponentName)
		test.That(t, err, test.ShouldBeNil)
		_, err = os.Stat(leftoverConcatTxtPath)
		test.That(t, os.IsNotExist(err), test.ShouldBeTrue)
	})

	t.Run("Test Async Save DoCommand from most recent video segment", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		r, err := setupViamServer(timeoutCtx, config1)
		test.That(t, err, test.ShouldBeNil)
		defer r.Close(timeoutCtx)
		vs, err := camera.FromRobot(r, videoStoreComponentName)
		test.That(t, err, test.ShouldBeNil)

		// Wait for the first video segment to be created.
		time.Sleep(10 * time.Second)

		now := time.Now()
		fromTime := now.Add(-5 * time.Second)
		toTime := now

		fromTimeStr := fromTime.Format(vsutils.TimeFormat)
		toTimeStr := toTime.Format(vsutils.TimeFormat)

		saveCmdNow := map[string]interface{}{
			"command":  "save",
			"from":     fromTimeStr,
			"to":       toTimeStr,
			"metadata": "test-metadata",
			"async":    true,
		}
		res, err := vs.DoCommand(timeoutCtx, saveCmdNow)
		test.That(t, err, test.ShouldBeNil)
		filename, ok := res["filename"].(string)
		test.That(t, ok, test.ShouldBeTrue)
		// Wait for async save to complete.
		time.Sleep(35 * time.Second)

		expectedFilename := fmt.Sprintf("%s_%s_%s.mp4",
			videoStoreComponentName,
			fromTime.Format(vsutils.TimeFormat),
			"test-metadata")
		test.That(t, filename, test.ShouldEqual, expectedFilename)

		concatPath := filepath.Join(testUploadPath, filename)
		_, err = os.Stat(concatPath)
		test.That(t, err, test.ShouldBeNil)
		testVideoPlayback(t, concatPath)
	})

	t.Run("Test Save DoCommand across strftime and unix int segments", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		r, err := setupViamServer(timeoutCtx, config1)
		test.That(t, err, test.ShouldBeNil)
		defer r.Close(timeoutCtx)
		vs, err := camera.FromRobot(r, videoStoreComponentName)
		test.That(t, err, test.ShouldBeNil)

		saveCmd := map[string]interface{}{
			"command":  "save",
			"from":     validFromTimestampStrftime,
			"to":       validToTimestampUnixInt,
			"metadata": "test-metadata",
		}

		res, err := vs.DoCommand(timeoutCtx, saveCmd)
		test.That(t, err, test.ShouldBeNil)
		filename, ok := res["filename"].(string)
		test.That(t, ok, test.ShouldBeTrue)

		expectedFilename := fmt.Sprintf("%s_%s_%s.mp4",
			videoStoreComponentName,
			validFromTimestampStrftime,
			"test-metadata")
		test.That(t, filename, test.ShouldEqual, expectedFilename)

		concatPath := filepath.Join(testUploadPath, filename)
		_, err = os.Stat(concatPath)
		test.That(t, err, test.ShouldBeNil)
		testVideoPlayback(t, concatPath)
	})
}
