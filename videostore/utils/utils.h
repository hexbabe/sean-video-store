#ifndef VIAM_VIDEOSTORE_UTILS_H
#define VIAM_VIDEOSTORE_UTILS_H
#include <libavformat/avformat.h>
#define VIDEO_STORE_VIDEO_INFO_RESP_ERROR 1
#define VIDEO_STORE_VIDEO_INFO_RESP_OK 0
#define VIDEO_STORE_CODEC_NAME_LEN 64
struct video_store_video_info {
    int64_t duration;
    int width;
    int height;
    char codec[VIDEO_STORE_CODEC_NAME_LEN];
};
typedef struct video_store_video_info video_store_video_info;
int video_store_get_video_duration(int64_t *duration, const char *filename);
void video_store_custom_av_log_callback(void *ptr, int level, const char *fmt, va_list vargs);
void video_store_set_custom_av_log_callback();
int video_store_get_video_info(video_store_video_info *info, const char *filename);
#endif /* VIAM_VIDEOSTORE_UTILS_H */
