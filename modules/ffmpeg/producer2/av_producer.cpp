#include "av_producer.h"

#include <boost/exception/exception.hpp>
#include <boost/format.hpp>
#include <boost/range/algorithm/rotate.hpp>

#include <common/scope_exit.h>
#include <common/except.h>

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

#include <tbb/concurrent_queue.h>

#include <atomic>
#include <queue>
#include <exception>
#include <memory>
#include <string>
#include <cinttypes>
#include <thread>

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

// TODO variable framerate input?
// TODO amerge multiple audio streams
// TODO secondary video stream is alpha
// TODO timeout with retry?
// TODO AVFMT_TS_DISCONT
// TODO seek
// TODO loop
// TODO ic_->duration accuracy
// TODO ic_->start_time accuracy
// TODO min_pts && max_pts

class Decoder
{
    typedef tbb::concurrent_bounded_queue<std::shared_ptr<AVPacket>> packets_t;
    typedef tbb::concurrent_bounded_queue<std::shared_ptr<AVFrame>>  frames_t;

    std::shared_ptr<AVCodecContext> avctx_;
    int                             stream_index_ = -1;

    packets_t                       packets_;
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
                        packets_.pop(packet);
                        FF(avcodec_send_packet(avctx_.get(), packet.get()));
                    } else if (ret == AVERROR_EOF) {
                        // avcodec_flush_framess(avctx_.get());
                        break;
                    } else {
                        FF_RET(ret, "avcodec_receive_frame");

                        // TODO
                        frame->pts = frame->best_effort_timestamp;

                        CASPAR_VERIFY(frame->pts != AV_NOPTS_VALUE);

                        frames_.push(std::move(frame));
                    }
                }
                frames_.push(nullptr);
            } catch (tbb::user_abort&) {
                return;
            } catch (...) {
                CASPAR_LOG_CURRENT_EXCEPTION();
            }
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
        return avctx_ != nullptr; 
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

            filter_spec += (boost::format(",bwdif=mode=send_field:parity=auto:deint=all")
            ).str();

            filter_spec += (boost::format(",fps=fps=%d/%d")
                % (format_desc.framerate.numerator() * format_desc.field_count) % format_desc.framerate.denominator()
            ).str();

            // TODO Do we need this?
            // if (first_pts_ != AV_NOPTS_VALUE) {
            //     filter_spec += (boost::format(":start_time=%f")
            //         % av_q2d(AVRational{ first_pts_, AV_TIME_BASE })
            //     ).str();
            // }

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

            FF(avfilter_graph_create_filter(&source_, buffersrc, "in", args_str.c_str(), nullptr, graph_.get()));
            FF(avfilter_graph_create_filter(&sink_, buffersink, "out", nullptr, nullptr, graph_.get()));

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
            FF(av_opt_set_int_list(sink_, "pix_fmts", pix_fmts, -1, AV_OPT_SEARCH_CHILDREN));
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

            FF(avfilter_graph_create_filter(&source_, buffersrc, "in", args_str.c_str(), nullptr, graph_.get()));
            FF(avfilter_graph_create_filter(&sink_, buffersink, "out", nullptr, nullptr, graph_.get()));

#ifdef _MSC_VER
#pragma warning (push)
#pragma warning (disable: 4245)
#endif
            // NOTE native sample format
            AVSampleFormat sample_fmts[] = { AV_SAMPLE_FMT_S32 , AV_SAMPLE_FMT_NONE };
            FF(av_opt_set_int_list(sink_, "sample_fmts", sample_fmts, -1, AV_OPT_SEARCH_CHILDREN));

            // NOTE There is no "native" channel_layout.
            int64_t channel_layouts[] = { static_cast<int64_t>(decoder_->channel_layout), -1LL };
            FF(av_opt_set_int_list(sink_, "channel_layouts", channel_layouts, -1, AV_OPT_SEARCH_CHILDREN));

            // NOTE native sample rate
            int sample_rates[] = { format_desc.audio_sample_rate, -1 };
            FF(av_opt_set_int_list(sink_, "sample_rates", sample_rates, -1, AV_OPT_SEARCH_CHILDREN));
#ifdef _MSC_VER
#pragma warning (pop)
#endif
        } else {
            FF_RET(AVERROR(AVERROR_UNKNOWN), "invalid codec type");
        }

        outputs->name = av_strdup("in");
        outputs->filter_ctx = source_;
        outputs->pad_idx = 0;
        outputs->next = nullptr;

        inputs->name = av_strdup("out");
        inputs->filter_ctx = sink_;
        inputs->pad_idx = 0;
        inputs->next = nullptr;

        if (!inputs->name || !outputs->name) {
            FF_RET(AVERROR(ENOMEM), "av_strdup");
        }

        FF(avfilter_graph_parse_ptr(graph_.get(), filter_spec.c_str(), &inputs, &outputs, nullptr));
        FF(avfilter_graph_config(graph_.get(), nullptr));

        thread_ = std::thread([this]
        {
            int ret;

            int64_t pts = AV_NOPTS_VALUE;

            try {
                while (true) {
                    const auto frame = alloc_frame();
                    ret = av_buffersink_get_frame(sink_, frame.get());

                    if (ret == AVERROR(EAGAIN)) {
                        FF(av_buffersrc_write_frame(source_, decoder_.pop().get()));
                    } else if (ret == AVERROR_EOF) {
                        break;
                    } else {
                        FF_RET(ret, "av_buffersink_get_frame");

                        CASPAR_VERIFY(frame->pts != AV_NOPTS_VALUE);

                        if (pts != AV_NOPTS_VALUE && frame->pts < pts) {
                            continue;
                        }

                        pts = frame->pts;

                        frames_.push(std::move(frame));
                    }
                }
                frames_.push(nullptr);
            } catch (tbb::user_abort&) {
                return;
            } catch (...) {
                CASPAR_LOG_CURRENT_EXCEPTION();
            }
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
        return sink_ ? av_buffersink_get_time_base(sink_) : AVRational { 0, 1 };
    }

    int width () const
    {
        return sink_ ? av_buffersink_get_w(sink_) : 0;
    }

    int height () const
    {
        return sink_ ? av_buffersink_get_h(sink_) : 0;
    }

    int64_t channel_layout() const
    {
        return sink_ ? av_buffersink_get_channel_layout(sink_) : 0;
    }

    explicit operator bool() const 
    { 
        return graph_ != nullptr;
    }
};

struct AVProducer::Impl
{
    const std::shared_ptr<core::frame_factory>  frame_factory_;
	const core::video_format_desc               format_desc_;
    const std::string                           filename_;
	
    std::shared_ptr<AVFormatContext>            ic_;

    std::unique_ptr<Graph>                      video_graph_;
    std::unique_ptr<Graph>                      audio_graph_;

    std::atomic<int64_t>                        time_ = AV_NOPTS_VALUE;
	std::atomic<int64_t>                        start_ = AV_NOPTS_VALUE;
    std::atomic<int64_t>                        duration_ = AV_NOPTS_VALUE;
    std::atomic<bool>                           loop_ = false;

	std::vector<int>                            audio_cadence_;
	
	std::shared_ptr<SwrContext>                 swr_;

    std::atomic<bool>                           abort_request_ = false;
    std::thread                                 thread_;

    Impl(
        std::shared_ptr<core::frame_factory> frame_factory,
        core::video_format_desc format_desc,
        std::string filename,
        std::string vfilter,
        std::string afilter,
        int64_t start,
        int64_t duration,
        bool loop)
        : frame_factory_(frame_factory)
        , format_desc_(format_desc)
        , filename_(filename)
        , start_(start)
        , duration_(duration)
        , loop_(loop)
        , audio_cadence_(format_desc_.audio_cadence)
    {
        {
			AVDictionary* options = nullptr;
			CASPAR_SCOPE_EXIT { av_dict_free(&options); };

            // TODO check if filename is http
			av_dict_set(&options, "reconnect", "1", 0); // HTTP reconnect
            av_dict_set(&options, "rw_timeout", "5000000", 0); // 5 second IO timeout

			AVFormatContext* ic = nullptr;
			FF(avformat_open_input(&ic, filename_.c_str(), nullptr, &options));
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
            video_graph_.reset(new Graph(ic_->streams[video_stream_index], vfilter, format_desc));
        }

        const auto audio_stream_index = av_find_best_stream(ic_.get(), AVMEDIA_TYPE_AUDIO, -1, -1, nullptr, 0);
        if (audio_stream_index >= 0) {
            ic_->streams[audio_stream_index]->discard = AVDISCARD_DEFAULT;       
            audio_graph_.reset(new Graph(ic_->streams[audio_stream_index], afilter, format_desc));
        }

        if (start_ != AV_NOPTS_VALUE) {
            auto ts = (ic_->start_time != AV_NOPTS_VALUE ? ic_->start_time : 0) + start_;

            if (!(ic_->iformat->flags & AVFMT_SEEK_TO_PTS)) {
                for (auto i = 0ULL; i < ic_->nb_streams; ++i) {
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

                    if (ret == AVERROR_EOF || avio_feof(ic_->pb)) {
                        video_graph_->push(nullptr);
                        audio_graph_->push(nullptr);
                        // TODO loop
                        break;
                    }

                    if (ret == AVERROR(EAGAIN)) {
                        boost::this_thread::sleep(boost::posix_time::milliseconds(100));
                        continue;
                    }
                    
                    FF_RET(ret, "av_read_frame");

                    video_graph_->push(packet);
                    audio_graph_->push(packet);
                }
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
        abort();
        thread_.join();
    }

    void abort()
    {
        abort_request_ = true;
        video_graph_.reset(new Graph());
        audio_graph_.reset(new Graph());
    }

    static int interrupt_cb(void* ctx)
    {
        const auto impl = reinterpret_cast<Impl*>(ctx);
        return impl->abort_request_.load() ? 1 : 0;
    }

    void seek(int64_t time)
    {
        CASPAR_THROW_EXCEPTION(not_implemented());
        // TODO
    }

    int64_t time() const
    {
        return time_;
    }

    void loop(bool loop)
    {
        loop_ = loop;
    }

    bool loop() const
    {
        return loop_;
    }

    void start(int64_t start)
    {
        start_ = start;
    }

    int64_t start() const
    {
        return start_;
    }

    void duration(int64_t duration)
    {
        duration_ = duration;
    }

    int64_t duration() const
    {
        const auto start = start_.load();
        return duration_ == AV_NOPTS_VALUE && ic_->duration != AV_NOPTS_VALUE
            ? std::max<int64_t>(0, ic_->duration - (start != AV_NOPTS_VALUE ? start : 0))
            : duration_;
    }

    int width() const
    {
        return video_graph_->width();
    }

    int height() const
    {
        return video_graph_->height();
    }

    core::draw_frame next() 
	{
        if (!video_graph_ && !audio_graph_) {
            return core::draw_frame();
        }

        if (time_ != AV_NOPTS_VALUE && duration_ != AV_NOPTS_VALUE && time_ >= duration_) {
            return core::draw_frame();
        }

		std::shared_ptr<AVFrame> video;
		std::shared_ptr<AVFrame> audio;

        const auto start = start_.load();
        const auto start_pts = (ic_->start_time != AV_NOPTS_VALUE ? ic_->start_time : 0) + 
                               (start != AV_NOPTS_VALUE ? start : 0);
    
        if (video_graph_) {
            const auto first_pts = av_rescale_q(start_pts, TIME_BASE_Q, video_graph_->time_base());

			while (!video || video->pts < first_pts) {
				video = video_graph_->pop();

                if (!video) {
                    break;
                }
			};
        }

        if (audio_graph_) {                
			// Note: Uses 1 step rotated cadence for 1001 modes (1602, 1602, 1601, 1602, 1601)
			// This cadence fills the audio mixer most optimally.
			boost::range::rotate(audio_cadence_, std::end(audio_cadence_) - 1);

            audio = alloc_frame();
			audio->sample_rate = format_desc_.audio_sample_rate;
			audio->channel_layout = audio_graph_->channel_layout();
			audio->channels = av_get_channel_layout_nb_channels(audio->channel_layout);
			audio->format = AV_SAMPLE_FMT_S32;
			audio->nb_samples = audio_cadence_[0];
			FF(av_frame_get_buffer(audio.get(), 0));

			while (!swr_ || swr_get_delay(swr_.get(), audio->sample_rate) < audio->nb_samples) {
                const auto frame = audio_graph_->pop();

                if (!frame) {
					break;
                }

                // TODO compensate to video pts?

				if (!swr_) {
                    const auto first_pts = video
                        ? av_rescale_q(video->pts, video_graph_->time_base(), AVRational{ 1, frame->sample_rate })
                        : av_rescale_q(start_pts, TIME_BASE_Q, AVRational{ 1, frame->sample_rate });

					swr_.reset(swr_alloc(), [](SwrContext* ptr) { swr_free(&ptr); });
					FF(swr_config_frame(swr_.get(), audio.get(), frame.get()));
                    FF(av_opt_set_int(swr_.get(), "first_pts", first_pts, AV_OPT_SEARCH_CHILDREN));
					FF(av_opt_set_int(swr_.get(), "async", 2000, AV_OPT_SEARCH_CHILDREN));
					FF(swr_init(swr_.get()));
				}

				const auto next_pts = av_rescale(
                    frame->pts,
                    static_cast<std::int64_t>(audio_graph_->time_base().num) * 
                    static_cast<std::int64_t>(frame->sample_rate) * 
                    static_cast<std::int64_t>(audio->sample_rate), 
                    audio_graph_->time_base().den
                );
				FF(swr_next_pts(swr_.get(), next_pts));
				FF(swr_convert_frame(swr_.get(), nullptr, frame.get()));
			}

			FF(swr_convert_frame(swr_.get(), audio.get(), nullptr));
        }

        if (!video && !audio) {
            return core::draw_frame();
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

        {
            const auto tb = AVRational { format_desc_.framerate.numerator(), format_desc_.framerate.denominator() };
            const auto time = time_.load();
            time_ = (time != AV_NOPTS_VALUE ? time : 0) + av_rescale_q(1, tb, TIME_BASE_Q);
        }

        return duration_ != AV_NOPTS_VALUE && time_ > duration_
            ? core::draw_frame()
            : core::draw_frame(std::move(frame));
    }
};

AVProducer::AVProducer(
    std::shared_ptr<core::frame_factory> frame_factory,
    core::video_format_desc format_desc,
    std::string filename,
	boost::optional<std::string> vfilter,
	boost::optional<std::string> afilter,
	boost::optional<int64_t> start,
    boost::optional<int64_t> duration,
    boost::optional<bool> loop 
)
    : impl_(new Impl{ 
		std::move(frame_factory), 
		std::move(format_desc), 
        std::move(filename), 
		std::move(vfilter.get_value_or("")),
		std::move(afilter.get_value_or("")),
		std::move(start.get_value_or(AV_NOPTS_VALUE)),
        std::move(duration.get_value_or(AV_NOPTS_VALUE)),
        std::move(loop.get_value_or(false))
	})
{   
}

core::draw_frame AVProducer::next()
{
    return impl_->next();
}

AVProducer& AVProducer::seek(int64_t time)
{
    impl_->seek(time);
    return *this;
}

AVProducer& AVProducer::loop(bool loop)
{
    impl_->loop(loop);
    return *this;
}

bool AVProducer::loop() const
{
    return impl_->loop();
}

AVProducer& AVProducer::start(int64_t start)
{
    impl_->start(start);
    return *this;
}

int64_t AVProducer::time() const
{
    const auto time = impl_->time();
    return time != AV_NOPTS_VALUE ? time : 0;
}

int64_t AVProducer::start() const
{
    const auto start = impl_->start();
    return start != AV_NOPTS_VALUE ? start : 0;
}

AVProducer& AVProducer::duration(int64_t duration)
{
    impl_->duration(duration);
    return *this;
}

int64_t AVProducer::duration() const
{
    const auto duration = impl_->duration();
    return duration != AV_NOPTS_VALUE ? duration : std::numeric_limits<int64_t>::max();
}

int AVProducer::width() const
{
    return impl_->width();
}

int AVProducer::height() const
{
    return impl_->height();
}

void AVProducer::abort()
{
    return impl_->abort();
}

}  // namespace ffmpeg
}  // namespace caspar
