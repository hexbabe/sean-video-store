package videostore

/*
#include "encoder.h"
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"sync"
	"unsafe"

	vsutils "github.com/viam-modules/video-store/videostore/utils"
	"go.viam.com/rdk/logging"
)

const (
	subsampleFactor = 2
)

type encoder struct {
	logger         logging.Logger
	framerate      int
	bitrate        int
	preset         string
	storagePath    string
	segmentSeconds int

	cEncoderMu sync.Mutex
	cEncoder   *C.video_store_h264_encoder
}

const (
	outputPattern = "%s.mp4"
	gigabyte      = 1024 * 1024 * 1024
)

func newEncoder(
	encoderConfig EncoderConfig,
	framerate int,
	storagePath string,
	logger logging.Logger,
) (*encoder, error) {
	enc := &encoder{
		logger:         logger,
		bitrate:        encoderConfig.Bitrate,
		framerate:      framerate,
		preset:         encoderConfig.Preset,
		storagePath:    storagePath,
		segmentSeconds: segmentSeconds,
	}

	return enc, nil
}

func (e *encoder) initialize() error {
	var cEncoder *C.video_store_h264_encoder
	e.cEncoderMu.Lock()
	defer e.cEncoderMu.Unlock()
	if e.cEncoder != nil {
		return errors.New("*encoder init called more than once")
	}

	outputPatternCStr := C.CString(e.storagePath + "/" + outputPattern)
	defer C.free(unsafe.Pointer(outputPatternCStr))

	presetCStr := C.CString(e.preset)
	defer C.free(unsafe.Pointer(presetCStr))

	ret := C.video_store_h264_encoder_init(
		&cEncoder,
		C.int(e.segmentSeconds),
		outputPatternCStr,
		C.int64_t(e.bitrate),
		C.int(e.framerate),
		presetCStr,
	)

	if ret != C.VIDEO_STORE_ENCODER_RESP_OK {
		err := errors.New("failed to initialize encoder")
		e.logger.Errorf("%s: %d: %s", err.Error(), ret, vsutils.FFmpegError(int(ret)))
		return err
	}
	e.cEncoder = cEncoder
	return nil
}

// encode encodes the given frame
// handles changing frame sizes
// If the polling loop is not running at the source framerate, the
// PTS will lag behind actual run time.
func (e *encoder) encode(frame []byte) {
	payloadC := C.CBytes(frame)
	defer C.free(payloadC)

	e.cEncoderMu.Lock()
	defer e.cEncoderMu.Unlock()
	if e.cEncoder == nil {
		e.logger.Errorf("encode called before init")
		return
	}
	ret := C.video_store_h264_encoder_write(
		e.cEncoder,
		payloadC,
		C.size_t(len(frame)),
	)
	if ret != C.VIDEO_STORE_ENCODER_RESP_OK {
		err := errors.New("failed to write packet to encoder")
		e.logger.Errorf("%s: %d", err.Error(), ret)
		return
	}
}

func (e *encoder) close() {
	e.cEncoderMu.Lock()
	defer e.cEncoderMu.Unlock()
	if e.cEncoder == nil {
		return
	}
	ret := C.video_store_h264_encoder_close(&e.cEncoder)
	if ret != C.VIDEO_STORE_ENCODER_RESP_OK {
		err := errors.New("failed to close encoder")
		e.logger.Errorf("%s: %d", err.Error(), ret)
		return
	}
	e.cEncoder = nil
}
