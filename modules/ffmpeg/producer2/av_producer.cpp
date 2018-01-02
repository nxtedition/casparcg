#include "av_producer.h"

#include <boost/exception/exception.hpp>
#include <boost/format.hpp>
#include <boost/range/algorithm/rotate.hpp>

#include <common/scope_exit.h>
#include <core/frame/draw_frame.h>
#include <core/frame/frame.h>
#include <core/frame/frame_factory.h>
#include <core/help/help_repository.h>
#include <core/help/help_sink.h>
#include <core/producer/media_info/media_info.h>

#ifdef _MSC_VER
#pragma warning (push)
#pragma warning (disable : 4244)
#endif
extern "C" {
#include <libavcodec/avcodec.h>
#include <libavfilter/avfilter.h>
#include <libavfilter/buffersink.h>
#include <libavfilter/buffersrc.h>
#include <libavformat/avformat.h>
#include <libswresample/swresample.h>
#include <libavutil/intreadwrite.h>
#include <libavutil/opt.h>
#include <libavutil/pixfmt.h>
#include <libavutil/samplefmt.h>
#include <libavutil/timecode.h>
}
#ifdef _MSC_VER
#pragma warning (pop)
#endif

#include "av_assert.h"
#include "av_util.h"

#include <queue>
#include <exception>
#include <memory>
#include <string>
#include <cinttypes>

namespace caspar {
namespace ffmpeg2 {

const AVRational TIME_BASE_Q = { 1, AV_TIME_BASE };

std::shared_ptr<AVFrame> alloc_frame()
{
    const auto frame = std::shared_ptr<AVFrame>(av_frame_alloc(), [](AVFrame *ptr) { av_frame_free(&ptr); });
    if (!frame)
        FF_RET(AVERROR(ENOMEM), "av_frame_alloc");
    return frame;
}

std::shared_ptr<AVPacket> alloc_packet()
{
    const auto packet = std::shared_ptr<AVPacket>(av_packet_alloc(), [](AVPacket *ptr) { av_packet_free(&ptr); });
    if (!packet)
        FF_RET(AVERROR(ENOMEM), "av_packet_alloc");
    return packet;
}

boost::rational<int> q2r(const AVRational& value)
{
    return boost::rational<int>(value.num, value.den);
}

// TODO variable framerate input?
// TODO amerge multiple audio streams
// TODO secondary video stream is alpha
// TODO timeout with retry?
// TODO handle pts discontinuities
// TODO seek
// TODO loop
// TODO duration

class Decoder
{
    typedef tbb::concurrent_bounded_queue<std::shared_ptr<AVPacket>> packets_t;
    typedef tbb::concurrent_bounded_queue<std::shared_ptr<AVFrame>>  frames_t;

    std::shared_ptr<AVCodecContext> avctx_;
    int                             stream_index_ = -1;

    packets_t                         packets_;
    frames_t                        frames_;
    std::thread                     thread_;

public:
    Decoder()
    {

    }

    explicit Decoder(AVStream* stream)
        : stream_index_(stream->index)
    {
        packets_.set_capacity(128);
        frames_.set_capacity(2);

        const auto codec = avcodec_find_decoder(stream->codecpar->codec_id);
        if (!codec) {
            FF_RET(AVERROR_DECODER_NOT_FOUND, "avcodec_find_decoder");
        }

        avctx_ = std::shared_ptr<AVCodecContext>(avcodec_alloc_context3(codec), [](AVCodecContext* ptr) { avcodec_free_context(&ptr); });
        if (!avctx_) {
            FF_RET(AVERROR(ENOMEM), "avcodec_alloc_context3");
        }

        FF(avcodec_parameters_to_context(avctx_.get(), stream->codecpar));

        FF(av_opt_set_int(avctx_.get(), "refcounted_frames", 1, 0));

        avctx_->pkt_timebase = stream->time_base;

        if (avctx_->codec_type == AVMEDIA_TYPE_VIDEO) {
            avctx_->framerate = av_guess_frame_rate(nullptr, stream, nullptr);
            avctx_->sample_aspect_ratio = av_guess_sample_aspect_ratio(nullptr, stream, nullptr);
        } else if (avctx_->codec_type == AVMEDIA_TYPE_AUDIO) {
            if (!avctx_->channel_layout && avctx_->channels) {
                avctx_->channel_layout = av_get_default_channel_layout(avctx_->channels);
            }
            if (!avctx_->channels && avctx_->channel_layout) {
                avctx_->channels = av_get_channel_layout_nb_channels(avctx_->channel_layout);
            }
        }

        FF(avcodec_open2(avctx_.get(), codec, nullptr));  

        thread_ = std::thread([this]
        {
            int ret;

            try {
                while (true) {
                    const auto frame = alloc_frame();
                    ret = avcodec_receive_frame(avctx_.get(), frame.get());

                    if (ret == AVERROR(EAGAIN)) {
                        std::shared_ptr<AVPacket> packet;
                        packets.pop(packet);
                        FF(avcodec_send_packet(avctx_.get(), packet.get()));
                    } else if (ret == AVERROR_EOF) {
                        avcodec_flush_framess(avctx_.get());
                        break;
                    } else {
                        FF_RET(ret, "avcodec_receive_frame");

                        // TODO
                        frame->pts = frame->best_effort_timestamp;

                        frames_.push(frame);
                    }
                }
            } catch (tbb::user_abort&) {
                return;
            } catch (...) {
                CASPAR_LOG_CURRENT_EXCEPTION();
            }
            frames_.push(nullptr);
        });    
    }

    ~Decoder()
    {
        packets_.abort();
        frames_.abort();
        thread_.join();
    }

    void push(const std::shared_ptr<AVPacket>& packet) 
    {
        if (*this && packet->stream_index == stream_index_) {
            packets_.push(packet);
        }
    }

    std::shared_ptr<AVFrame> pop()
    {
        std::shared_ptr<AVFrame> frame;
        if (*this) {
            frames_.pop(frame);
        }
        return frame;
    }

    AVCodecContext* operator->()
    {
        return avctx_.get();
    }

    explicit operator bool() const 
    { 
        return avctx_; 
    }
};

class Graph
{
    typedef tbb::concurrent_bounded_queue<std::shared_ptr<AVFrame>> frames_t;

    std::shared_ptr<AVFilterGraph>  graph_;

    Decoder                         decoder_;    

    AVFilterContext*                source_ = nullptr;
	AVFilterContext*                sink_ = nullptr;

    frames_t                        frames_;
    std::thread                     thread_;

public:
    Graph()
    {

    }

    explicit Graph(AVStream*                      stream,
                   std::string                    filter_spec,
                   const core::video_format_desc& format_desc)   
        : decoder_(stream)
    {
        frames_.set_capacity(2);

        AVFilterInOut* outputs = avfilter_inout_alloc();
        AVFilterInOut* inputs = avfilter_inout_alloc();

        CASPAR_SCOPE_EXIT 
        {
            avfilter_inout_free(&inputs);
            avfilter_inout_free(&outputs);
        };

        if (!outputs || !inputs) {
            FF_RET(AVERROR(ENOMEM), "avfilter_inout_alloc");
        }

        graph_ = std::shared_ptr<AVFilterGraph>(avfilter_graph_alloc(), [](AVFilterGraph* ptr) { avfilter_graph_free(&ptr); });

        if (!graph_) {
            FF_RET(AVERROR(ENOMEM), "avfilter_graph_alloc");
        }

        if (decoder_->codec_type == AVMEDIA_TYPE_VIDEO) {
            if (filter_spec.empty()) {
                filter_spec = "null";
            }

            // TODO auto letterbox

            const auto frame_rate = q2r(decoder_->framerate);

            if (frame_rate == format_desc.framerate && format_desc.field_count > 1) {
                filter_spec += (boost::format(",scale=%d:%d:interl=-1")
                    % format_desc.width % format_desc.height
                ).str();
            } else {
            filter_spec += (boost::format(",idet,bwdif=mode=%s:parity=auto:deint=interlaced")
                % (frame_rate == format_desc.framerate ? "send_frame" : "send_field")
            ).str();

            filter_spec += (boost::format(",fps=fps=%d/%d")
                % (format_desc.framerate.numerator() * format_desc.field_count) % format_desc.framerate.denominator()
            ).str();

            if (first_pts_ != AV_NOPTS_VALUE) {
                filter_spec += (boost::format(":start_time=%f")
                    % av_q2d(AVRational{ first_pts_, AV_TIME_BASE })
                ).str();
            }

            if (format_desc.field_count == 2) {
                filter_spec += (boost::format(",scale=%d:%d,interlace=scan=")
                    % format_desc.width % format_desc.height
                    % (format_desc.field_mode == core::field_mode::upper ? "tff" : "bff")
                ).str();
            }

            const auto buffersrc = avfilter_get_by_name("buffer");
            const auto buffersink = avfilter_get_by_name("buffersink");

            auto args_str = (boost::format("video_size=%dx%d:pix_fmt=%d:time_base=%d/%d")
                % decoder_->width % decoder_->height
                % decoder_->pix_fmt
                % decoder_->pkt_timebase.num % decoder_->pkt_timebase.den
            ).str();

            if (decoder_->sample_aspect_ratio.num > 0 && decoder_->sample_aspect_ratio.den > 0) {
                args_str += (boost::format(":sar=%d/%d")
                    % decoder_->sample_aspect_ratio.num % decoder_->sample_aspect_ratio.den
                ).str();
            }

            if (decoder_->framerate.num > 0 && decoder_->framerate.den > 0) {
                args_str += (boost::format(":frame_rate=%d/%d")
                    % decoder_->framerate.num % decoder_->framerate.den
                ).str();
            }

            FF(avfilter_graph_create_filter(&source_, buffersrc, "in", args_str.c_str(), nullptr, graph.get()));
            FF(avfilter_graph_create_filter(&sink_, buffersink, "out", nullptr, nullptr, graph.get()));

#ifdef _MSC_VER
#pragma warning (push)
#pragma warning (disable: 4245)
#endif
            const AVPixelFormat pix_fmts[] = {
                AV_PIX_FMT_GRAY8,
                AV_PIX_FMT_RGB24,
                AV_PIX_FMT_BGR24,
                AV_PIX_FMT_BGRA,
                AV_PIX_FMT_ARGB,
                AV_PIX_FMT_RGBA,
                AV_PIX_FMT_ABGR,
                AV_PIX_FMT_YUV444P,
                AV_PIX_FMT_YUV422P,
                AV_PIX_FMT_YUVA444P,
                AV_PIX_FMT_YUVA422P,
                AV_PIX_FMT_NONE
            };
            FF(av_opt_set_int_list(sink, "pix_fmts", pix_fmts, -1, AV_OPT_SEARCH_CHILDREN));
#ifdef _MSC_VER
#pragma warning (pop)
#endif
        } else if (decoder_->codec_type == AVMEDIA_TYPE_AUDIO) {
            if (filter_spec.empty()) {
                filter_spec = "anull";
            }

            const auto buffersrc = avfilter_get_by_name("abuffer");
            const auto buffersink = avfilter_get_by_name("abuffersink");

            const auto args_str = (boost::format("time_base=%d/%d:sample_rate=%d:sample_fmt=%s:channel_layout=%#x")
                % decoder_->pkt_timebase.num % decoder_->pkt_timebase.den
                % decoder_->sample_rate
                % av_get_sample_fmt_name(decoder_->sample_fmt)
                % decoder_->channel_layout
            ).str();

            FF(avfilter_graph_create_filter(&source, buffersrc, "in", args_str.c_str(), nullptr, graph.get()));
            FF(avfilter_graph_create_filter(&sink, buffersink, "out", nullptr, nullptr, graph.get()));

#ifdef _MSC_VER
#pragma warning (push)
#pragma warning (disable: 4245)
#endif
            // NOTE native sample format
            AVSampleFormat sample_fmts[] = { AV_SAMPLE_FMT_S32 , AV_SAMPLE_FMT_NONE };
            FF(av_opt_set_int_list(sink, "sample_fmts", sample_fmts, -1, AV_OPT_SEARCH_CHILDREN));

            // NOTE There is no "native" channel_layout.
            int64_t channel_layouts[] = { static_cast<int64_t>(cc->channel_layout), -1LL };
            FF(av_opt_set_int_list(sink, "channel_layouts", channel_layouts, -1, AV_OPT_SEARCH_CHILDREN));

            // NOTE native sample rate
            int sample_rates[] = { format_desc.audio_sample_rate, -1 };
            FF(av_opt_set_int_list(sink, "sample_rates", sample_rates, -1, AV_OPT_SEARCH_CHILDREN));
#ifdef _MSC_VER
#pragma warning (pop)
#endif
        } else {
            FF_RET(AVERROR(AVERROR_UNKNOWN), "invalid codec type");
        }

        outputs->name = av_strdup("in");
        outputs->filter_ctx = source;
        outputs->pad_idx = 0;
        outputs->next = nullptr;

        inputs->name = av_strdup("out");
        inputs->filter_ctx = sink;
        inputs->pad_idx = 0;
        inputs->next = nullptr;

        if (!inputs->name || !outputs->name) {
            FF_RET(AVERROR(ENOMEM), "av_strdup");
        }

        FF(avfilter_graph_parse_ptr(graph.get(), filter_spec.c_str(), &inputs, &outputs, nullptr));
        FF(avfilter_graph_config(graph.get(), nullptr));

        thread_ = std::thread([this]
        {
            int ret;

            try {
                while (true) {
                    const auto frame = alloc_frame();
                    ret = av_buffersink_get_frame(sink_, frame.get());

                    if (ret == AVERROR(EAGAIN)) {
                        FF(av_buffersrc_write_frame(source_, decoder_.pop().get());
                    } else if (ret == AVERROR_EOF) {
                        break;
                    } else {
                        FF_RET(ret, "av_buffersink_get_frame");
                        frames_.push(std::move(frame));
                    }
                }
            } catch (tbb::user_abort&) {
                return;
            } catch (...) {
                CASPAR_LOG_CURRENT_EXCEPTION();
            }
            frames_.push(nullptr);
        });
    }

    ~Graph()
    {
        frames_.abort();
        thread_.join();
    }

    void push(const std::shared_ptr<AVPacket>& packet) 
    {
        if (*this) {
            decoder_.push(packet);
        }
    }

    std::shared_ptr<AVFrame> pop()
    {
        std::shared_ptr<AVFrame> frame;
        if (*this) {
            frames_.pop(frame);
        }
        return frame;
    }

    AVRational time_base() const
    {
        return av_buffersink_get_time_base(sink_);
    }

    explicit operator bool() const 
    { 
        return graph_; 
    }
};

struct AVProducer::Impl
{
    const std::shared_ptr<core::frame_factory>  frame_factory_;
	const core::video_format_desc               format_desc_;
    const std::string                           filename_;
	
    std::shared_ptr<AVFormatContext>            ic_;

    Graph                                       video_graph_;
	Graph                                       audio_graph_;

	int64_t                                     start_pts_ = AV_NOPTS_VALUE;
	int64_t                                     first_pts_ = AV_NOPTS_VALUE;

	std::vector<int>                            audio_cadence_;
	
	std::shared_ptr<SwrContext>                 swr_;

    std::atomic<bool>                           abort_request_ = false;
    std::thread                                 thread_;

    Impl(
        const std::shared_ptr<core::frame_factory>& frame_factory,
        const core::video_format_desc& format_desc,
        const std::string& filename,
        const std::string& vfilter,
        const std::string& afilter,
        int64_t start)
        : frame_factory_(frame_factory)
        , format_desc_(format_desc)
        , filename_(filename)
        , start_pts_(start_pts)
        , audio_cadence_(format_desc_.audio_cadence)
    {
        {
			AVDictionary* options = nullptr;
			CASPAR_SCOPE_EXIT { av_dict_free(&options); };

            // TODO check if filename is http
			av_dict_set(&options, "reconnect", "1", 0); // HTTP reconnect

			AVFormatContext* ic = nullptr;
			FF(avformat_open_input(&ic, filename.c_str(), nullptr, &options));
			ic_ = std::shared_ptr<AVFormatContext>(ic, [](AVFormatContext* ctx) { avformat_close_input(&ctx); });
            ic_->interrupt_callback.callback = Impl::interrupt_cb;
            ic_->interrupt_callback.opaque = this;
        }

        FF(avformat_find_stream_info(ic_.get(), nullptr));

        for (auto i = 0UL; i < ic_->nb_streams; ++i) {
            ic_->streams[i]->discard = AVDISCARD_ALL;
        }

        const auto video_stream_index = av_find_best_stream(ic_.get(), AVMEDIA_TYPE_VIDEO, -1, -1, nullptr, 0);
        if (video_stream_index >= 0) {
            ic_->streams[video_stream_index]->discard = AVDISCARD_DEFAULT;
            video_graph_ = Graph(ic_->streams[video_stream_index], vfilter, format_desc);
        }

        const auto audio_stream_index = av_find_best_stream(ic_.get(), AVMEDIA_TYPE_AUDIO, -1, -1, nullptr, 0);
        if (audio_stream_index >= 0) {
            ic_->streams[audio_stream_index]->discard = AVDISCARD_DEFAULT;       
            audio_graph_ = Graph(ic_->streams[audio_stream_index], afilter, format_desc);
        }

        first_pts_ = ic_->start_time != AV_NOPTS_VALUE ? ic_->start_time : 0;

        if (start_pts_ != AV_NOPTS_VALUE) {
            first_pts_ += start_pts_;

            auto ts = first_pts_;

            if (!(ic_->iformat->flags & AVFMT_SEEK_TO_PTS)) {
                for (int i = 0; i < ic_->nb_streams; ++i) {
                    if (ic_->streams[i]->codecpar->video_delay) {
                        ts -= 3 * AV_TIME_BASE / 23;
                        break;
                    }
                }
            }

            FF(avformat_seek_file(ic_.get(), -1, INT64_MIN, ts, ts, 0));
        }

        thread_ = std::thread([this]
        {  
            int ret;

            try {
                while (true) {
                    const auto packet = alloc_packet();
                    ret = av_read_frame(ic_.get(), packet.get());

                    if (ret == AVERROR_EOF || avio_feof(ic->pb))
                        break;

                    if (ret == AVERROR(EAGAIN)) {
                        boost::this_thread::sleep(boost::posix_time::milliseconds(100));
                        continue;
                    }
                    
                    FF_RET(ret, "av_read_frame");

                    video_graph_.push(packet);
                    audio_graph_.push(packet);
                }

                video_graph_.push(nullptr);
                audio_graph_.push(nullptr);
            } catch (tbb::user_abort&) {
                return;
            } catch (...) {
                CASPAR_LOG_CURRENT_EXCEPTION();
            }
        });
		
        // av_dump_format(ic_.get(), 0, filename, 0);
    }

    ~Impl()
    {
        abort_request_ = true;
        video_graph_ = boost::none;
        audio_graph_ = boost::none;
        thread_.join();
    }

    static int interrupt_cb(void* ctx)
    {
        const auto impl = reinterpret_cast<Impl*>(ctx);
        return impl->abort_request_->load() ? 1 : 0;
    }

    boost::rational<int64_t> duration() const
    {
        return ic_->duration == AV_NOPTS_VALUE
            ? boost::rational<int64_t>(0, 1);
            : boost::rational<int64_t>(ic_->duration - (start_pts_ != AV_NOPTS_VALUE ? start_pts_ : 0), AV_TIME_BASE);
	}

    boost::optional<core::draw_frame> next() 
	{
        if (!video_graph_ && !audio_graph_) {
            return boost::none;
        }

		std::shared_ptr<AVFrame> video;
		std::shared_ptr<AVFrame> audio;

        if (video_graph_) {
            const auto first_pts = av_rescale_q(first_pts_, TIME_BASE_Q, video_graph_.time_base());

			while (!video || video->pts < first_pts) {
				video = video_graph_.pop();

                if (!video) {
                    break;
                }

                // TODO do we need this?
                // if (video_stream_->pts != AV_NOPTS_VALUE && pts < video_stream_->pts)
                //     break;
			};
        }

        if (audio_graph_) {
            const auto first_pts = video 
                ? av_rescale_q(video->pts, video_graph_.time_base(), AVRational{ 1, frame->sample_rate })
                : av_rescale_q(first_pts_, TIME_BASE_Q, AVRational{ 1, frame->sample_rate });
                
			// Note: Uses 1 step rotated cadence for 1001 modes (1602, 1602, 1601, 1602, 1601)
			// This cadence fills the audio mixer most optimally.
			boost::range::rotate(audio_cadence_, std::end(audio_cadence_) - 1);

            audio = alloc_frame();
			audio->sample_rate = format_desc_.audio_sample_rate;
			audio->channel_layout = av_buffersink_get_channel_layout(audio_stream_->sink);
			audio->channels = av_get_channel_layout_nb_channels(audio->channel_layout);
			audio->format = AV_SAMPLE_FMT_S32;
			audio->nb_samples = audio_cadence_[0];
			FF(av_frame_get_buffer(audio.get(), 0));

			while (!swr_ || swr_get_delay(swr_.get(), audio->sample_rate) < audio->nb_samples) {
                const auto frame = audio_graph_.pop();

                if (!frame) {
					break;
                }

                // // TODO do we need this?
                // if (audio_stream_->pts != AV_NOPTS_VALUE && pts < audio_stream_->pts)
                //     break;

                // TODO compensate to video pts?

				if (!swr_) {
					swr_.reset(swr_alloc(), [](SwrContext* ptr) { swr_free(&ptr); });
					FF(swr_config_frame(swr_.get(), audio.get(), frame.get()));
                    FF(av_opt_set_int(swr_.get(), "first_pts", first_pts, AV_OPT_SEARCH_CHILDREN));
					FF(av_opt_set_int(swr_.get(), "async", 2000, AV_OPT_SEARCH_CHILDREN));
					FF(swr_init(swr_.get()));
				}

				const auto next_pts = av_rescale(
                    frame->pts,
                    static_cast<std::int64_t>(tb.num) * 
                    static_cast<std::int64_t>(frame->sample_rate) * 
                    static_cast<std::int64_t>(audio->sample_rate), 
                    tb.den
                );
				FF(swr_next_pts(swr_.get(), next_pts));
				FF(swr_convert_frame(swr_.get(), nullptr, frame.get()));
			}

			FF(swr_convert_frame(swr_.get(), audio.get(), nullptr));
        }

        if (!video && !audio) {
            return boost::none;
        }

        const auto pix_desc = video
            ? ffmpeg2::pixel_format_desc(static_cast<AVPixelFormat>(video->format), video->width, video->height)
            : core::pixel_format_desc(core::pixel_format::invalid);

        const auto channel_layout = audio
            ? ffmpeg2::get_audio_channel_layout(audio->channels, audio->channel_layout)
            : core::audio_channel_layout::invalid();

		auto frame = frame_factory_->create_frame(this, pix_desc, channel_layout);

		if (video) {
			for (int n = 0; n < static_cast<int>(pix_desc.planes.size()); ++n) {
				for (int y = 0; y < pix_desc.planes[n].height; ++y) {
					std::memcpy(
                        frame.image_data(n).begin() + y * pix_desc.planes[n].linesize, 
                        video->data[n] + y * video->linesize[n], 
                        pix_desc.planes[n].linesize
                    );
                }
			}
		}

		if (audio) {
			const auto beg = reinterpret_cast<uint32_t*>(audio->data[0]);
            const auto end = beg + audio->nb_samples * audio->channels;
            frame.audio_data() = core::mutable_audio_buffer(beg, end);
		}

        return core::draw_frame(std::move(frame));
    }
};

AVProducer::AVProducer(
    const std::shared_ptr<core::frame_factory>& frame_factory,
    const core::video_format_desc& format_desc,
    const std::string& filename,
	const boost::optional<std::string> vfilter,
	const boost::optional<std::string> afilter,
	const boost::optional<int64_t> start
)
    : impl_(new Impl{ 
		frame_factory, 
		format_desc, filename, 
		vfilter.get_value_or(""),
		afilter.get_value_or(""),
		start.get_value_or(AV_NOPTS_VALUE)
	})
{   
}

boost::optional<core::draw_frame> AVProducer::next()
{
    return impl_->next();
}

boost::rational<int64_t> AVProducer::duration() const
{
	return impl_->duration();
}

}  // namespace ffmpeg
}  // namespace caspar