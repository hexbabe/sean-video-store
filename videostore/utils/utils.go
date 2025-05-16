// Package vsutils provides shared utility functions for video store.
package vsutils

/*
#cgo CFLAGS: -I${SRCDIR}
#include "utils.h"
#include <libavutil/error.h>
#include <libavutil/opt.h>
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavcodec/avcodec.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"go.viam.com/rdk/logging"
)

// SetLibAVLogLevel sets the libav log level.
// this is global for the entire OS process.
// valid inputs are "info", "warn", "error", "debug"
// https://www.ffmpeg.org/doxygen/2.5/group__lavu__log__constants.html
func SetLibAVLogLevel(level string) {
	ffmpegLogLevel(lookupLogID(level))
}

type codecType int

const (
	// CodecUnknown represents an unknown codec type.
	codecUnknown codecType = iota
	// CodecH264 represents the H.264 codec type.
	codecH264
	// CodecH265 represents the H.265 codec type.
	codecH265

	// TimeFormat is how we format the timestamp in output filenames and do commands.
	TimeFormat = "2006-01-02_15-04-05"
	// Gigabyte is the number of bytes in a gigabyte.
	Gigabyte = 1024 * 1024 * 1024
)

// VideoInfo in Go, corresponding to the C VideoInfo struct
type VideoInfo struct {
	Duration time.Duration
	Width    int
	Height   int
	Codec    string
}

// FileWithDate stores a file name and its start time.
type FileWithDate struct {
	Name      string
	StartTime time.Time
}

// ConcatFileEntry represents an entry in an FFmpeg concat demuxer file
type ConcatFileEntry struct {
	FilePath string
	Inpoint  *float64 // Optional start time trim point
	Outpoint *float64 // Optional end time trim point
}

// String returns the FFmpeg concat demuxer compatible string representation
func (e ConcatFileEntry) String() []string {
	var lines []string
	lines = append(lines, fmt.Sprintf("file '%s'", e.FilePath))
	if e.Inpoint != nil {
		lines = append(lines, fmt.Sprintf("inpoint %.2f", *e.Inpoint))
	}
	if e.Outpoint != nil {
		lines = append(lines, fmt.Sprintf("outpoint %.2f", *e.Outpoint))
	}
	return lines
}

// fromCVideoInfo converts a C.VideoInfo struct to a Go VideoInfo struct
func fromCVideoInfo(cinfo C.video_store_video_info) VideoInfo {
	return VideoInfo{
		// FFmpeg stores AVFormatContext->duration in AV_TIME_BASE units (1,000,000 ticks per second),
		// so it effectively represents microseconds.
		Duration: time.Duration(cinfo.duration) * time.Microsecond,
		Width:    int(cinfo.width),
		Height:   int(cinfo.height),
		Codec:    C.GoString(&cinfo.codec[0]),
	}
}

func (c codecType) String() string {
	switch c {
	case codecH264:
		return "h264"
	case codecH265:
		return "h265"
	default:
		return "unknown"
	}
}

// FFmpegError returns a string representation of the ffmpeg error code.
func FFmpegError(ret int) string {
	const errbufSize = 256
	var errbuf [errbufSize]C.char
	C.av_strerror(C.int(ret), &errbuf[0], errbufSize)
	if errbuf[0] == 0 {
		return "unknown ffmpeg error"
	}
	return C.GoString(&errbuf[0])
}

// ffmpegLogLevel sets the log level for ffmpeg logger.
func ffmpegLogLevel(loglevel C.int) {
	C.av_log_set_level(loglevel)
}

// SetFFmpegLogCallback sets the custom log callback for ffmpeg.
func SetFFmpegLogCallback() {
	C.video_store_set_custom_av_log_callback()
}

// lookupLogID returns the log ID for the provided log level.
func lookupLogID(level string) C.int {
	switch level {
	case "error":
		return C.AV_LOG_ERROR
	case "warn":
		return C.AV_LOG_WARNING
	case "info":
		return C.AV_LOG_INFO
	case "debug":
		return C.AV_LOG_DEBUG
	default:
		return C.AV_LOG_INFO
	}
}

// CreateDir creates a directory at the provided path if it does not exist.
func CreateDir(path string) error {
	const dirPermissions = 0o755
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.MkdirAll(path, dirPermissions)
		if err != nil {
			return err
		}
	}
	return nil
}

// ReadVideoFile takes in a path to mp4 file and returns bytes of the file.
func ReadVideoFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// GetDirectorySize returns the size of a directory in bytes.
func GetDirectorySize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return size, err
}

// GetFileSize returns the size of a file in bytes.
func GetFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// GetSortedFiles returns a list of files in the provided directory sorted by creation time.
func GetSortedFiles(path string) ([]FileWithDate, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var filePaths []string
	for _, file := range files {
		filePaths = append(filePaths, filepath.Join(path, file.Name()))
	}
	return CreateAndSortFileWithDateList(filePaths), nil
}

// CreateAndSortFileWithDateList takes a list of file paths, extracts the date from each file name,
// and returns a sorted list of FileWithDate.
func CreateAndSortFileWithDateList(filePaths []string) []FileWithDate {
	var validFiles []FileWithDate
	for _, filePath := range filePaths {
		date, err := ExtractDateTimeFromFilename(filePath)
		if err == nil {
			dateUTC := date.UTC()
			validFiles = append(validFiles, FileWithDate{Name: filePath, StartTime: dateUTC})
		}
	}
	SortFilesByDate(validFiles)
	return validFiles
}

// SortFilesByDate sorts a slice of FileWithDate by their date field.
func SortFilesByDate(files []FileWithDate) {
	sort.Slice(files, func(i, j int) bool {
		return files[i].StartTime.Before(files[j].StartTime)
	})
}

// ExtractDateTimeFromFilename extracts the date and time from the filename.
// Returns time in the inputted timezone.
func ExtractDateTimeFromFilename(filePath string) (time.Time, error) {
	baseName := filepath.Base(filePath)
	nameWithoutExt := strings.TrimSuffix(baseName, filepath.Ext(baseName))

	// Unix timestamp case - keep in UTC
	if timestamp, err := strconv.ParseInt(nameWithoutExt, 10, 64); err == nil {
		return time.Unix(timestamp, 0), nil
	}

	// Datetime format case - keep in local time
	return ParseDateTimeString(nameWithoutExt)
}

// ParseDateTimeString parses a datetime string in our format (2006-01-02_15-04-05)
// and returns it in local time.
func ParseDateTimeString(datetime string) (time.Time, error) {
	if strings.HasSuffix(datetime, "Z") {
		return time.Parse(TimeFormat, strings.TrimSuffix(datetime, "Z"))
	}
	//nolint:gosmopolitan // intentional parsing into local time
	return time.ParseInLocation(TimeFormat, datetime, time.Local)
}

// FormatDateTimeString formats a datetime to a string in UTC with a Z suffix.
func FormatDateTimeString(datetime time.Time) string {
	return datetime.UTC().Format(TimeFormat) + "Z"
}

// MatchStorageToRange identifies video files that overlap with the requested time range (start to end)
// and returns them as FFmpeg concat demuxer entries with appropriate trim points.
//
// The function optimizes processing by:
// 1. Finding the subset of files that could potentially overlap with the time range
// 2. Checking for actual overlap between each file and the requested time range
// 3. Calculating inpoint/outpoint trim values when a file partially overlaps
// 4. Ensuring all matched files have consistent video parameters (width/height/codec)
//
// The input files must be sorted by start time, and the function assumes video segments
// don't overlap in time.
func MatchStorageToRange(files []FileWithDate, start, end time.Time, logger logging.Logger) []ConcatFileEntry {
	var entries []ConcatFileEntry
	// Cache of the first matched video file's width, height, and codec
	// to ensure every video in the matched files set have the same params.
	var firstSeenVideoInfo VideoInfo
	// Find the first and last file that could potentially overlap with the query time range.
	firstFileIndex := -1
	lastFileIndex := len(files)
	for i, file := range files {
		if file.StartTime.After(start) && firstFileIndex == -1 {
			firstFileIndex = i - 1
		}
		if file.StartTime.After(end) {
			lastFileIndex = i
			break
		}
	}
	logger.Debugf("firstFileIndex: %d, lastFileIndex: %d", firstFileIndex, lastFileIndex)
	if firstFileIndex == -1 {
		firstFileIndex = len(files) - 1
	}
	for _, file := range files[firstFileIndex:lastFileIndex] {
		videoFileInfo, err := GetVideoInfo(file.Name)
		if err != nil {
			logger.Debugf("failed to get video duration for file: %s, error: %v", file.Name, err)
			continue
		}
		fileEndTime := file.StartTime.Add(videoFileInfo.Duration)
		// Check if the segment file's time range intersects
		// with the match request time range [start, end)
		if file.StartTime.Before(end) && fileEndTime.After(start) {
			// If the first video file in the matched set, cache the width, height, and codec
			cacheFirstVid(&firstSeenVideoInfo, videoFileInfo)
			if firstSeenVideoInfo.Width != videoFileInfo.Width ||
				firstSeenVideoInfo.Height != videoFileInfo.Height ||
				firstSeenVideoInfo.Codec != videoFileInfo.Codec {
				logger.Warnf(
					"Skipping file %s. Expected (width=%d, height=%d, codec=%s), got (width=%d, height=%d, codec=%s)",
					file.Name,
					firstSeenVideoInfo.Width, firstSeenVideoInfo.Height, firstSeenVideoInfo.Codec,
					videoFileInfo.Width, videoFileInfo.Height, videoFileInfo.Codec,
				)
				continue
			}
			logger.Debugf("Matched file %s", file.Name)
			entry := ConcatFileEntry{FilePath: file.Name}
			// Calculate inpoint if the file starts before the 'start' time and overlaps
			if file.StartTime.Before(start) {
				inpoint := start.Sub(file.StartTime).Seconds()
				entry.Inpoint = &inpoint
			}
			// Calculate outpoint if the file ends after the 'end' time
			if fileEndTime.After(end) {
				outpoint := end.Sub(file.StartTime).Seconds()
				entry.Outpoint = &outpoint
			}
			entries = append(entries, entry)
		}
	}

	return entries
}

// cacheFirstVid caches the first video file's width, height, and codec.
func cacheFirstVid(first *VideoInfo, current VideoInfo) {
	if first.Width == 0 {
		first.Width = current.Width
	}
	if first.Height == 0 {
		first.Height = current.Height
	}
	if first.Codec == "" {
		first.Codec = current.Codec
	}
}

// GenerateOutputFilePath generates the output filename for the video file.
// The filename timestamp is formatted in local time.
func GenerateOutputFilePath(prefix string, timestamp time.Time, metadata, dir string) string {
	// Format timestamp in local time for user-friendly filenames
	//nolint:gosmopolitan // datetime format timestamps must be parsed into local time for outputs.
	// We do not rely on localtime strftime for actual segments. They are stored in Unix UTC time.
	localTime := timestamp.In(time.Local)
	filename := localTime.Format(TimeFormat)

	if prefix != "" {
		filename = fmt.Sprintf("%s_%s", prefix, filename)
	}
	if metadata != "" {
		filename = fmt.Sprintf("%s_%s", filename, metadata)
	}

	return filepath.Join(dir, filename+".mp4")
}

// ValidateTimeRange validates the start and end time range against storage files.
// Extracts the start timestamp of the oldest file and the start of the most recent file.
// Since the most recent segment file is still being written to by the segmenter
// we do not want to include it in the time range.
// func validateTimeRange(files []string, start, end time.Time) error {
func ValidateTimeRange(files []FileWithDate, start, end time.Time) error {
	if len(files) == 0 {
		return errors.New("no storage files found")
	}
	oldestFileStart := files[0].StartTime
	newestFileStart := files[len(files)-1].StartTime
	if start.Before(oldestFileStart) || end.After(newestFileStart) {
		return errors.New("time range is outside of storage range")
	}
	return nil
}

// GetVideoInfo calls the C function get_video_info to retrieve
// duration, width, height, and codec of a video file.
func GetVideoInfo(filePath string) (VideoInfo, error) {
	cFilePath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cFilePath))
	var cinfo C.video_store_video_info
	ret := C.video_store_get_video_info(&cinfo, cFilePath)
	switch ret {
	case C.VIDEO_STORE_VIDEO_INFO_RESP_OK:
		return fromCVideoInfo(cinfo), nil
	case C.VIDEO_STORE_VIDEO_INFO_RESP_ERROR:
		return VideoInfo{}, fmt.Errorf("video_store_get_video_info failed for file: %s", filePath)
	default:
		return VideoInfo{}, fmt.Errorf("video_store_get_video_info failed for file: %s with error: %s",
			filePath, FFmpegError(int(ret)))
	}
}
