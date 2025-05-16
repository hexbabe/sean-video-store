#include "utils.h"
#include <libavutil/log.h>
#include <libavcodec/avcodec.h>
#include <string.h>

int video_store_get_video_info(video_store_video_info *info, // OUT
                               const char *filename          // IN
) {
    AVFormatContext *fmt_ctx = NULL;
    int ret;

    if ((ret = avformat_open_input(&fmt_ctx, filename, NULL, NULL)) < 0) {
        return ret;
    }
    if ((ret = avformat_find_stream_info(fmt_ctx, NULL)) < 0) {
        avformat_close_input(&fmt_ctx);
        return ret;
    }

    if (fmt_ctx->duration == AV_NOPTS_VALUE) {
        av_log(NULL, AV_LOG_DEBUG, "video_store_get_video_info video file has no duration\n");
        avformat_close_input(&fmt_ctx);
        return VIDEO_STORE_VIDEO_INFO_RESP_ERROR;
    }

    // First, check how many video streams are present
    int videoCount = 0;
    for (unsigned i = 0; i < fmt_ctx->nb_streams; i++) {
        AVStream *st = fmt_ctx->streams[i];
        if (st->codecpar->codec_type == AVMEDIA_TYPE_VIDEO) {
            videoCount++;
        }
    }

    // If more than one video stream, return error
    if (videoCount > 1) {
        av_log(NULL, AV_LOG_DEBUG, "video_store_get_video_info video file has more than one video stream\n");
        avformat_close_input(&fmt_ctx);
        return VIDEO_STORE_VIDEO_INFO_RESP_ERROR;
    }

    int tmpWidth = 0;
    int tmpHeight = 0;
    char tmpCodec[VIDEO_STORE_CODEC_NAME_LEN];
    for (unsigned i = 0; i < fmt_ctx->nb_streams; i++) {
        AVStream *st = fmt_ctx->streams[i];
        if (st->codecpar->codec_type == AVMEDIA_TYPE_VIDEO) {
            tmpWidth  = st->codecpar->width;
            tmpHeight = st->codecpar->height;
            const char *codecName = avcodec_get_name(st->codecpar->codec_id);
            if (codecName) {
                strncpy(tmpCodec, codecName, VIDEO_STORE_CODEC_NAME_LEN - 1);
                tmpCodec[VIDEO_STORE_CODEC_NAME_LEN - 1] = '\0';
            }
            break;
        }
    }

    if (tmpWidth == 0) {
        av_log(NULL, AV_LOG_DEBUG, "video_store_get_video_info could not find width of video stream\n");
        avformat_close_input(&fmt_ctx);
        return VIDEO_STORE_VIDEO_INFO_RESP_ERROR;
    }
    if (tmpHeight == 0) {
        av_log(NULL, AV_LOG_DEBUG, "video_store_get_video_info could not find height of video stream\n");
        avformat_close_input(&fmt_ctx);
        return VIDEO_STORE_VIDEO_INFO_RESP_ERROR;
    }
    if (tmpCodec[0] == '\0') {
        av_log(NULL, AV_LOG_DEBUG, "video_store_get_video_info could not find codec of video stream\n");
        avformat_close_input(&fmt_ctx);
        return VIDEO_STORE_VIDEO_INFO_RESP_ERROR;
    }

    info->duration = fmt_ctx->duration;
    info->width    = tmpWidth;
    info->height   = tmpHeight;
    strncpy(info->codec, tmpCodec, VIDEO_STORE_CODEC_NAME_LEN);

    avformat_close_input(&fmt_ctx);
    return VIDEO_STORE_VIDEO_INFO_RESP_OK;
}

void video_store_custom_av_log_callback(void *ptr, int level, const char *fmt, va_list vargs) {
    // Default callback will handle log level filtering
    av_log_default_callback(ptr, level, fmt, vargs);
    // Default callback only prints to stderr
    fflush(stderr);
}

void video_store_set_custom_av_log_callback() {
    av_log_set_callback(video_store_custom_av_log_callback);
}
