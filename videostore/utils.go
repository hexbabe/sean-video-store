package videostore

/*
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
)

// videoInfo in Go, corresponding to the C VideoInfo struct
type videoInfo struct {
	duration time.Duration
	width    int
	height   int
	codec    string
}

type fileWithDate struct {
	name      string
	startTime time.Time
}

// ConcatFileEntry represents an entry in an FFmpeg concat demuxer file
type concatFileEntry struct {
	filePath string
	inpoint  *float64 // Optional start time trim point
	outpoint *float64 // Optional end time trim point
}

// String returns the FFmpeg concat demuxer compatible string representation
func (e concatFileEntry) string() []string {
	var lines []string
	lines = append(lines, fmt.Sprintf("file '%s'", e.filePath))
	if e.inpoint != nil {
		lines = append(lines, fmt.Sprintf("inpoint %.2f", *e.inpoint))
	}
	if e.outpoint != nil {
		lines = append(lines, fmt.Sprintf("outpoint %.2f", *e.outpoint))
	}
	return lines
}

// fromCVideoInfo converts a C.VideoInfo struct to a Go videoInfo struct
func fromCVideoInfo(cinfo C.video_store_video_info) videoInfo {
	return videoInfo{
		// FFmpeg stores AVFormatContext->duration in AV_TIME_BASE units (1,000,000 ticks per second),
		// so it effectively represents microseconds.
		duration: time.Duration(cinfo.duration) * time.Microsecond,
		width:    int(cinfo.width),
		height:   int(cinfo.height),
		codec:    C.GoString(&cinfo.codec[0]),
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

func parseCodecType(codec string) codecType {
	switch codec {
	case "h264":
		return codecH264
	case "h265":
		return codecH265
	default:
		return codecUnknown
	}
}

// ffmpegError returns a string representation of the ffmpeg error code.
func ffmpegError(ret C.int) string {
	const errbufSize = 256
	var errbuf [errbufSize]C.char
	C.av_strerror(ret, &errbuf[0], errbufSize)
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

// createDir creates a directory at the provided path if it does not exist.
func createDir(path string) error {
	const dirPermissions = 0o755
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err := os.MkdirAll(path, dirPermissions)
		if err != nil {
			return err
		}
	}
	return nil
}

// readVideoFile takes in a path to mp4 file and returns bytes of the file.
func readVideoFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// getDirectorySize returns the size of a directory in bytes.
func getDirectorySize(path string) (int64, error) {
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

// getFileSize returns the size of a file in bytes.
func getFileSize(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// getSortedFiles returns a list of files in the provided directory sorted by creation time.
func getSortedFiles(path string) ([]fileWithDate, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var filePaths []string
	for _, file := range files {
		filePaths = append(filePaths, filepath.Join(path, file.Name()))
	}
	return createAndSortFileWithDateList(filePaths), nil
}

// createAndSortFileWithDateList takes a list of file paths, extracts the date from each file name,
// and returns a sorted list of fileWithDate.
func createAndSortFileWithDateList(filePaths []string) []fileWithDate {
	var validFiles []fileWithDate
	for _, filePath := range filePaths {
		date, err := extractDateTimeFromFilename(filePath)
		if err == nil {
			dateUTC := date.UTC()
			validFiles = append(validFiles, fileWithDate{name: filePath, startTime: dateUTC})
		}
	}
	sortFilesByDate(validFiles)
	return validFiles
}

// sortFilesByDate sorts a slice of fileWithDate by their date field.
func sortFilesByDate(files []fileWithDate) {
	sort.Slice(files, func(i, j int) bool {
		return files[i].startTime.Before(files[j].startTime)
	})
}

// extractDateTimeFromFilename extracts the date and time from the filename.
// Returns time in the inputted timezone.
func extractDateTimeFromFilename(filePath string) (time.Time, error) {
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

// matchStorageToRange identifies video files that overlap with the requested time range (start to end)
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
func matchStorageToRange(files []fileWithDate, start, end time.Time, logger logging.Logger) []concatFileEntry {
	var entries []concatFileEntry
	// Cache of the first matched video file's width, height, and codec
	// to ensure every video in the matched files set have the same params.
	var firstSeenVideoInfo videoInfo
	// Find the first and last file that could potentially overlap with the query time range.
	firstFileIndex := -1
	lastFileIndex := len(files)
	for i, file := range files {
		if file.startTime.After(start) && firstFileIndex == -1 {
			firstFileIndex = i - 1
		}
		if file.startTime.After(end) {
			lastFileIndex = i
			break
		}
	}
	logger.Debugf("firstFileIndex: %d, lastFileIndex: %d", firstFileIndex, lastFileIndex)
	if firstFileIndex == -1 {
		firstFileIndex = len(files) - 1
	}
	for _, file := range files[firstFileIndex:lastFileIndex] {
		videoFileInfo, err := getVideoInfo(file.name)
		if err != nil {
			logger.Debugf("failed to get video duration for file: %s, error: %v", file.name, err)
			continue
		}
		fileEndTime := file.startTime.Add(videoFileInfo.duration)
		// Check if the segment file's time range intersects
		// with the match request time range [start, end)
		if file.startTime.Before(end) && fileEndTime.After(start) {
			// If the first video file in the matched set, cache the width, height, and codec
			cacheFirstVid(&firstSeenVideoInfo, videoFileInfo)
			if firstSeenVideoInfo.width != videoFileInfo.width ||
				firstSeenVideoInfo.height != videoFileInfo.height ||
				firstSeenVideoInfo.codec != videoFileInfo.codec {
				logger.Warnf(
					"Skipping file %s. Expected (width=%d, height=%d, codec=%s), got (width=%d, height=%d, codec=%s)",
					file.name,
					firstSeenVideoInfo.width, firstSeenVideoInfo.height, firstSeenVideoInfo.codec,
					videoFileInfo.width, videoFileInfo.height, videoFileInfo.codec,
				)
				continue
			}
			logger.Debugf("Matched file %s", file.name)
			entry := concatFileEntry{filePath: file.name}
			// Calculate inpoint if the file starts before the 'start' time and overlaps
			if file.startTime.Before(start) {
				inpoint := start.Sub(file.startTime).Seconds()
				entry.inpoint = &inpoint
			}
			// Calculate outpoint if the file ends after the 'end' time
			if fileEndTime.After(end) {
				outpoint := end.Sub(file.startTime).Seconds()
				entry.outpoint = &outpoint
			}
			entries = append(entries, entry)
		}
	}

	return entries
}

// cacheFirstVid caches the first video file's width, height, and codec.
func cacheFirstVid(first *videoInfo, current videoInfo) {
	if first.width == 0 {
		first.width = current.width
	}
	if first.height == 0 {
		first.height = current.height
	}
	if first.codec == "" {
		first.codec = current.codec
	}
}

// generateOutputFilePath generates the output filename for the video file.
// The filename timestamp is formatted in local time.
func generateOutputFilePath(prefix string, timestamp time.Time, metadata, dir string) string {
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

// validateTimeRange validates the start and end time range against storage files.
// Extracts the start timestamp of the oldest file and the start of the most recent file.
// Since the most recent segment file is still being written to by the segmenter
// we do not want to include it in the time range.
// func validateTimeRange(files []string, start, end time.Time) error {
func validateTimeRange(files []fileWithDate, start, end time.Time) error {
	if len(files) == 0 {
		return errors.New("no storage files found")
	}
	oldestFileStart := files[0].startTime
	newestFileStart := files[len(files)-1].startTime
	if start.Before(oldestFileStart) || end.After(newestFileStart) {
		return errors.New("time range is outside of storage range")
	}
	return nil
}

// getVideoInfo calls the C function get_video_info to retrieve
// duration, width, height, and codec of a video file.
func getVideoInfo(filePath string) (videoInfo, error) {
	cFilePath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cFilePath))
	var cinfo C.video_store_video_info
	ret := C.video_store_get_video_info(&cinfo, cFilePath)
	switch ret {
	case C.VIDEO_STORE_VIDEO_INFO_RESP_OK:
		return fromCVideoInfo(cinfo), nil
	case C.VIDEO_STORE_VIDEO_INFO_RESP_ERROR:
		return videoInfo{}, fmt.Errorf("video_store_get_video_info failed for file: %s", filePath)
	default:
		return videoInfo{}, fmt.Errorf("video_store_get_video_info failed for file: %s with error: %s",
			filePath, ffmpegError(ret))
	}
}
