package vsutils

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"go.viam.com/rdk/logging"
	"go.viam.com/test"
)

const (
	artifactStoragePath = "../../.artifact/data/"

	// Sequential segments from actual video files (30 second intervals).
	segmentUnix1 int64 = 1725634803 // First segment
	segmentUnix2 int64 = 1725634833 // +30s
	segmentUnix3 int64 = 1725634863 // +30s
	segmentUnix4 int64 = 1725634893 // +30s
	segmentUnix5 int64 = 1725634923 // +30s
)

// Helper function to create filenames.
func unixToFilename(unix int64) string {
	return fmt.Sprintf("%d.mp4", unix)
}

func unixToDatetimeFilename(unix int64) string {
	return time.Unix(unix, 0).In(time.Local).Format("2006-01-02_15-04-05.mp4")
}

func TestGetVideoInfo(t *testing.T) {
	t.Run("Valid video file succeeds", func(t *testing.T) {
		info, err := GetVideoInfo(artifactStoragePath + unixToFilename(segmentUnix1))
		test.That(t, err, test.ShouldBeNil)
		test.That(t, info.Duration.Seconds(), test.ShouldEqual, 30.0)
		test.That(t, info.Width, test.ShouldEqual, 640)
		test.That(t, info.Height, test.ShouldEqual, 480)
		test.That(t, info.Codec, test.ShouldEqual, "h264")
	})
	t.Run("No duration video file errors", func(t *testing.T) {
		info, err := GetVideoInfo(artifactStoragePath + "zero_duration_video.mp4")
		test.That(t, err, test.ShouldNotBeNil)
		test.That(t, err.Error(), test.ShouldContainSubstring, "get_video_info failed for file")
		test.That(t, info.Duration.Seconds(), test.ShouldEqual, 0.0)
		test.That(t, info.Width, test.ShouldEqual, 0)
		test.That(t, info.Height, test.ShouldEqual, 0)
		test.That(t, info.Codec, test.ShouldEqual, "")
	})
	t.Run("Not a video file errors", func(t *testing.T) {
		info, err := GetVideoInfo(artifactStoragePath + "not_video_file.txt")
		test.That(t, err, test.ShouldNotBeNil)
		test.That(t, err.Error(), test.ShouldContainSubstring, "Invalid data found when processing input")
		test.That(t, info.Duration.Seconds(), test.ShouldEqual, 0.0)
		test.That(t, info.Width, test.ShouldEqual, 0)
		test.That(t, info.Height, test.ShouldEqual, 0)
		test.That(t, info.Codec, test.ShouldEqual, "")
	})
	t.Run("Broken video file errors", func(t *testing.T) {
		info, err := GetVideoInfo(artifactStoragePath + "no_moov.mp4")
		test.That(t, err, test.ShouldNotBeNil)
		test.That(t, err.Error(), test.ShouldContainSubstring, "Invalid data found when processing input")
		test.That(t, info.Duration.Seconds(), test.ShouldEqual, 0.0)
		test.That(t, info.Width, test.ShouldEqual, 0)
		test.That(t, info.Height, test.ShouldEqual, 0)
		test.That(t, info.Codec, test.ShouldEqual, "")
	})
	t.Run("Nonexistent file errors", func(t *testing.T) {
		info, err := GetVideoInfo(artifactStoragePath + "nonexistent.mp4")
		test.That(t, err, test.ShouldNotBeNil)
		test.That(t, err.Error(), test.ShouldContainSubstring, "No such file or directory")
		test.That(t, info.Duration.Seconds(), test.ShouldEqual, 0.0)
		test.That(t, info.Width, test.ShouldEqual, 0)
		test.That(t, info.Height, test.ShouldEqual, 0)
		test.That(t, info.Codec, test.ShouldEqual, "")
	})
}

func TestMatchStorageToRange(t *testing.T) {
	logger := logging.NewTestLogger(t)
	t.Run("Match request within one segment", func(t *testing.T) {
		fileList := []string{
			artifactStoragePath + unixToFilename(segmentUnix1),
			artifactStoragePath + unixToFilename(segmentUnix2),
			artifactStoragePath + unixToFilename(segmentUnix3),
		}
		fileWithDateList := CreateAndSortFileWithDateList(fileList)
		startTime := time.Unix(segmentUnix1+10, 0) // +10 seconds from first file
		endTime := time.Unix(segmentUnix1+20, 0)   // +20 seconds from first file
		matchedFiles := MatchStorageToRange(fileWithDateList, startTime, endTime, logger)
		expected := []ConcatFileEntry{
			{FilePath: artifactStoragePath + unixToFilename(segmentUnix1), Inpoint: float64Ptr(10.00), Outpoint: float64Ptr(20.00)},
		}
		test.That(t, matchedFiles, test.ShouldResemble, expected)
	})

	t.Run("Match request spanning multiple segments", func(t *testing.T) {
		fileList := []string{
			artifactStoragePath + unixToFilename(segmentUnix1),
			artifactStoragePath + unixToFilename(segmentUnix2),
			artifactStoragePath + unixToFilename(segmentUnix3),
		}
		fileWithDateList := CreateAndSortFileWithDateList(fileList)
		startTime := time.Unix(segmentUnix1+7, 0) // +7 seconds from first file
		endTime := time.Unix(segmentUnix2+27, 0)  // +27 seconds from second file
		expected := []ConcatFileEntry{
			{FilePath: artifactStoragePath + unixToFilename(segmentUnix1), Inpoint: float64Ptr(7.00)},
			{FilePath: artifactStoragePath + unixToFilename(segmentUnix2), Outpoint: float64Ptr(27.00)},
		}
		matchedFiles := MatchStorageToRange(fileWithDateList, startTime, endTime, logger)
		test.That(t, matchedFiles, test.ShouldResemble, expected)
	})

	t.Run("Match request along segment boundary", func(t *testing.T) {
		fileList := []string{
			artifactStoragePath + unixToFilename(segmentUnix1),
			artifactStoragePath + unixToFilename(segmentUnix2),
			artifactStoragePath + unixToFilename(segmentUnix3),
			artifactStoragePath + unixToFilename(segmentUnix4),
		}
		fileWithDateList := CreateAndSortFileWithDateList(fileList)
		startTime := time.Unix(segmentUnix2, 0) // Start at second segment
		endTime := time.Unix(segmentUnix4, 0)   // End at fourth segment
		expected := []ConcatFileEntry{
			{FilePath: artifactStoragePath + unixToFilename(segmentUnix2)},
			{FilePath: artifactStoragePath + unixToFilename(segmentUnix3)},
		}
		matchedFiles := MatchStorageToRange(fileWithDateList, startTime, endTime, logger)
		test.That(t, matchedFiles, test.ShouldResemble, expected)
	})

	t.Run("Match request within gap in data", func(t *testing.T) {
		fileList := []string{
			artifactStoragePath + unixToFilename(segmentUnix1), // ends at +30s
			artifactStoragePath + unixToFilename(segmentUnix3), // starts at +60s
		}
		fileWithDateList := CreateAndSortFileWithDateList(fileList)
		// Look for a range entirely within the gap between segmentUnix1 and segmentUnix3
		startTime := time.Unix(segmentUnix1+35, 0) // 5s after first segment ends
		endTime := time.Unix(segmentUnix1+40, 0)   // 10s after first segment ends
		matchedFiles := MatchStorageToRange(fileWithDateList, startTime, endTime, logger)
		test.That(t, matchedFiles, test.ShouldBeEmpty)
	})

	t.Run("Match request spanning gap in segment data", func(t *testing.T) {
		fileList := []string{
			artifactStoragePath + "2025-03-05_16-36-20.mp4",
			artifactStoragePath + "2025-03-05_16-36-59.mp4",
			artifactStoragePath + "2025-03-05_16-37-38.mp4",
		}
		fileWithDateList := CreateAndSortFileWithDateList(fileList)
		startTime, err := ParseDateTimeString("2025-03-05_16-36-40")
		test.That(t, err, test.ShouldBeNil)
		endTime, err := ParseDateTimeString("2025-03-05_16-37-10")
		test.That(t, err, test.ShouldBeNil)
		expected := []ConcatFileEntry{
			{FilePath: artifactStoragePath + "2025-03-05_16-36-20.mp4", Inpoint: float64Ptr(20.00)},
			{FilePath: artifactStoragePath + "2025-03-05_16-36-59.mp4", Outpoint: float64Ptr(11.00)},
		}
		matchedFiles := MatchStorageToRange(fileWithDateList, startTime, endTime, logger)
		test.That(t, matchedFiles, test.ShouldResemble, expected)
	})

	t.Run("Match request start range in data gap", func(t *testing.T) {
		fileList := []string{
			artifactStoragePath + "2025-03-05_16-36-20.mp4",
			artifactStoragePath + "2025-03-05_16-36-59.mp4",
		}
		fileWithDateList := CreateAndSortFileWithDateList(fileList)
		startTime, err := ParseDateTimeString("2025-03-05_16-36-54")
		test.That(t, err, test.ShouldBeNil)
		endTime, err := ParseDateTimeString("2025-03-05_16-37-20")
		test.That(t, err, test.ShouldBeNil)
		expected := []ConcatFileEntry{
			{FilePath: artifactStoragePath + "2025-03-05_16-36-59.mp4", Outpoint: float64Ptr(21.00)},
		}
		matchedFiles := MatchStorageToRange(fileWithDateList, startTime, endTime, logger)
		test.That(t, matchedFiles, test.ShouldResemble, expected)
	})

	t.Run("Match request end range in data gap", func(t *testing.T) {
		fileList := []string{
			artifactStoragePath + "2025-03-05_16-36-20.mp4",
			artifactStoragePath + "2025-03-05_16-36-59.mp4",
		}
		fileWithDateList := CreateAndSortFileWithDateList(fileList)
		startTime, err := ParseDateTimeString("2025-03-05_16-36-30")
		test.That(t, err, test.ShouldBeNil)
		endTime, err := ParseDateTimeString("2025-03-05_16-36-55")
		test.That(t, err, test.ShouldBeNil)
		expected := []ConcatFileEntry{
			{FilePath: artifactStoragePath + "2025-03-05_16-36-20.mp4", Inpoint: float64Ptr(10.00)},
		}
		matchedFiles := MatchStorageToRange(fileWithDateList, startTime, endTime, logger)
		test.That(t, matchedFiles, test.ShouldResemble, expected)
	})

	t.Run("Match request spanning size change boundary", func(t *testing.T) {
		fileList := []string{
			artifactStoragePath + "2025-03-11_11-49-37.mp4",
			artifactStoragePath + "2025-03-11_11-49-58.mp4",
		}
		fileWithDateList := CreateAndSortFileWithDateList(fileList)
		startTime, err := ParseDateTimeString("2025-03-11_11-49-47")
		test.That(t, err, test.ShouldBeNil)
		endTime, err := ParseDateTimeString("2025-03-11_11-50-47")
		test.That(t, err, test.ShouldBeNil)
		expected := []ConcatFileEntry{
			{FilePath: artifactStoragePath + "2025-03-11_11-49-37.mp4", Inpoint: float64Ptr(10.00)},
		}
		matchedFiles := MatchStorageToRange(fileWithDateList, startTime, endTime, logger)
		test.That(t, matchedFiles, test.ShouldResemble, expected)
	})

	t.Run("Match request spanning codec change boundary", func(t *testing.T) {
		fileList := []string{
			artifactStoragePath + "2025-03-11_16-14-40.mp4",
			artifactStoragePath + "2025-03-11_16-15-10.mp4",
		}
		fileWithDateList := CreateAndSortFileWithDateList(fileList)
		startTime, err := ParseDateTimeString("2025-03-11_16-14-50")
		test.That(t, err, test.ShouldBeNil)
		endTime, err := ParseDateTimeString("2025-03-11_16-15-30")
		test.That(t, err, test.ShouldBeNil)
		expected := []ConcatFileEntry{
			{FilePath: artifactStoragePath + "2025-03-11_16-14-40.mp4", Inpoint: float64Ptr(10.00)},
		}
		matchedFiles := MatchStorageToRange(fileWithDateList, startTime, endTime, logger)
		test.That(t, matchedFiles, test.ShouldResemble, expected)
	})
}

// float64Ptr returns a pointer to the given float64 value.
func float64Ptr(f float64) *float64 {
	return &f
}

func TestExtractDateTimeFromFilename(t *testing.T) {
	tests := []struct {
		name             string
		filename         string
		expectedDateTime time.Time
		shouldErrOnParse bool
	}{
		{
			name:             "Unix timestamp format",
			filename:         unixToFilename(segmentUnix1),
			expectedDateTime: time.Unix(segmentUnix1, 0),
		},
		{
			name:             "Legacy datetime format (local time)",
			filename:         unixToDatetimeFilename(segmentUnix1),
			expectedDateTime: time.Unix(segmentUnix1, 0),
		},
		{
			name:             "Invalid format",
			filename:         "invalid.mp4",
			shouldErrOnParse: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dateTime, err := ExtractDateTimeFromFilename(tt.filename)
			if tt.shouldErrOnParse {
				test.That(t, err, test.ShouldNotBeNil)
				var parseErr *time.ParseError
				isParseErr := errors.As(err, &parseErr)
				test.That(t, isParseErr, test.ShouldBeTrue)
				return
			}
			test.That(t, err, test.ShouldBeNil)
			test.That(t, dateTime.Equal(tt.expectedDateTime), test.ShouldBeTrue)
			test.That(t, dateTime.Location(), test.ShouldEqual, time.Local)
		})
	}
}
