
namespace caspar {
namespace ffmpeg {

core::audio_channel_layout get_audio_channel_layout(int num_channels, std::uint64_t layout);

core::pixel_format get_pixel_format(AVPixelFormat pix_fmt);

core::pixel_format_desc pixel_format_desc(AVPixelFormat pix_fmt, int width, int height);

}
}