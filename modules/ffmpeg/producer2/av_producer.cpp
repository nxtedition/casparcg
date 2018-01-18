#include "av_producer.h"

#include <boost/exception/exception.hpp>
#include <boost/format.hpp>
#include <boost/rational.hpp>
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
#include <condition_variable>
#include <set>

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

// TODO timeout with retry?
// TODO AVFMT_TS_DISCONT
// TODO filter preset
// TODO ic_->duration accuracy
// TODO ic_->start_time accuracy
// TODO min_pts && max_pts
// TODO AVDISCARD

struct AVProducer::Impl
{   
    const std::shared_ptr<core::frame_factory>      frame_factory_;
	const core::video_format_desc                   format_desc_;
    const std::string                               filename_;

    std::shared_ptr<AVFormatContext>                ic_;
    std::atomic<bool>                               abort_request_ = false;

    std::map<int, std::shared_ptr<AVCodecContext>>  decoders_;

    std::map<int, std::vector<AVFilterContext*>>    sources_;

    AVFilterContext*                                video_sink_ = nullptr;
    std::shared_ptr<AVFilterGraph>                  video_graph_;

    AVFilterContext*                                audio_sink_ = nullptr;
    std::shared_ptr<AVFilterGraph>                  audio_graph_;

    int64_t                                         time_ = AV_NOPTS_VALUE;
    int64_t                                         seek_ = AV_NOPTS_VALUE;
	int64_t                                         start_ = AV_NOPTS_VALUE;
    int64_t                                         duration_ = AV_NOPTS_VALUE;
    bool                                            loop_ = false;

    std::string                                     afilter_;
    std::string                                     vfilter_;
 
	std::vector<int>                                audio_cadence_;
 
    core::draw_frame                                frame_ = core::draw_frame::late();

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
        , vfilter_(vfilter)
        , afilter_(afilter)
        , audio_cadence_(format_desc_.audio_cadence)
    {
        {
            AVDictionary* options = nullptr;
            CASPAR_SCOPE_EXIT{ av_dict_free(&options); };

            // TODO check if filename is http
            FF(av_dict_set(&options, "reconnect", "1", 0)); // HTTP reconnect
                                                            // TODO timeout?
            FF(av_dict_set(&options, "rw_timeout", "5000000", 0)); // 5 second IO timeout

            AVFormatContext* ic = nullptr;
            FF(avformat_open_input(&ic, filename_.c_str(), nullptr, &options));
            ic_ = std::shared_ptr<AVFormatContext>(ic, [](AVFormatContext* ctx) { avformat_close_input(&ctx); });

            // TODO
            //ic_->interrupt_callback.callback = interrupt_cb;
            //ic_->interrupt_callback.opaque = this;

            FF(avformat_find_stream_info(ic_.get(), nullptr));
        }

        if (start_ != AV_NOPTS_VALUE) {
            seek(start_);
        }

        open_filter(vfilter, AVMEDIA_TYPE_VIDEO);
        open_filter(afilter, AVMEDIA_TYPE_AUDIO);
    }

    ~Impl()
    {
    }

    void open_filter(std::string filter_spec, AVMediaType media_type)
    {
        if (media_type == AVMEDIA_TYPE_VIDEO) {
            if (filter_spec.empty()) {
                filter_spec = "null";
            }

            filter_spec += (boost::format(",bwdif=mode=send_field:parity=auto:deint=all")
                            ).str();

            filter_spec += (boost::format(",fps=fps=%d/%d")
                            % (format_desc_.framerate.numerator() * format_desc_.field_count) % format_desc_.framerate.denominator()
                            ).str();

            if (format_desc_.field_count == 2) {
                filter_spec += (boost::format(",scale=%d:%d,interlace=scan=")
                                % format_desc_.width % format_desc_.height
                                % (format_desc_.field_mode == core::field_mode::upper ? "tff" : "bff")
                                ).str();
            }
        } else if (media_type == AVMEDIA_TYPE_AUDIO) {
            if (filter_spec.empty()) {
                filter_spec = "anull";
            }

            filter_spec += (boost::format(",aresample=async=2000")
                ).str();
        }

        AVFilterInOut* outputs = nullptr;
        AVFilterInOut* inputs = nullptr;
        AVFilterContext* sink = nullptr;

        CASPAR_SCOPE_EXIT
        {
            avfilter_inout_free(&inputs);
            avfilter_inout_free(&outputs);
        };

        int video_input_count = 0;
        int audio_input_count = 0;
        {
            auto graph = avfilter_graph_alloc();
            if (!graph) {
                FF_RET(AVERROR(ENOMEM), "avfilter_graph_alloc");
            }

            CASPAR_SCOPE_EXIT
            {
                avfilter_graph_free(&graph);
                avfilter_inout_free(&inputs);
                avfilter_inout_free(&outputs);
            };

            FF(avfilter_graph_parse2(graph, filter_spec.c_str(), &inputs, &outputs));

            for (auto cur = inputs; cur; cur = cur->next) {
                const auto type = avfilter_pad_get_type(cur->filter_ctx->input_pads, cur->pad_idx);
                if (type == AVMEDIA_TYPE_VIDEO) {
                    video_input_count += 1;
                } else if (type == AVMEDIA_TYPE_AUDIO) {
                    audio_input_count += 1;
                }
            }
        }

        auto graph = std::shared_ptr<AVFilterGraph>(avfilter_graph_alloc(), [](AVFilterGraph* ptr) { avfilter_graph_free(&ptr); });

        if (!graph) {
            FF_RET(AVERROR(ENOMEM), "avfilter_graph_alloc");
        }

        if (media_type == AVMEDIA_TYPE_VIDEO) {
            FF(avfilter_graph_create_filter(&sink, avfilter_get_by_name("buffersink"), "out", nullptr, nullptr, graph.get()));

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
                // NOTE CasparCG does not properly handle interlaced vertical chrome subsampling.
                // Use only YUV444 and YUV422 formats.
                AV_PIX_FMT_NONE
            };
            FF(av_opt_set_int_list(sink, "pix_fmts", pix_fmts, -1, AV_OPT_SEARCH_CHILDREN));
#ifdef _MSC_VER
#pragma warning (pop)
#endif
        } else if (media_type == AVMEDIA_TYPE_AUDIO) {
            FF(avfilter_graph_create_filter(&sink, avfilter_get_by_name("abuffersink"), "out", nullptr, nullptr, graph.get()));
#ifdef _MSC_VER
#pragma warning (push)
#pragma warning (disable: 4245)
#endif
            const AVSampleFormat sample_fmts[] = {
                AV_SAMPLE_FMT_S32,
                AV_SAMPLE_FMT_NONE
            };
            FF(av_opt_set_int_list(sink, "sample_fmts", sample_fmts, -1, AV_OPT_SEARCH_CHILDREN));

            const int sample_rates[] = {
                48000,
                -1
            };
            FF(av_opt_set_int_list(sink, "sample_rates", sample_rates, -1, AV_OPT_SEARCH_CHILDREN));
#ifdef _MSC_VER
#pragma warning (pop)
#endif
        } else {
            CASPAR_THROW_EXCEPTION(ffmpeg_error_t()
                                   << boost::errinfo_errno(EINVAL)
                                   << msg_info_t("invalid output media type")
            );
        }

        if (audio_input_count == 1) {
            int count = 0;
            for (auto i = 0ULL; i < ic_->nb_streams; ++i) {
                if (ic_->streams[i]->codecpar->codec_type == AVMEDIA_TYPE_AUDIO) {
                    count += 1;
                }
            }
            if (count > 1) {
                filter_spec = (boost::format("amerge=inputs=%d") % count).str() + filter_spec;
            }
        } else if (video_input_count == 1) {
            int count = 0;
            for (auto i = 0ULL; i < ic_->nb_streams; ++i) {
                if (ic_->streams[i]->codecpar->codec_type == AVMEDIA_TYPE_VIDEO) {
                    count += 1;
                }
            }
            if (count > 1) {
                filter_spec = "alphamerge" + filter_spec;
            }
        }

        FF(avfilter_graph_parse2(graph.get(), filter_spec.c_str(), &inputs, &outputs));

        std::set<int> used;

        // inputs
        {
            for (auto cur = inputs; cur; cur = cur->next) {
                const auto type = avfilter_pad_get_type(cur->filter_ctx->input_pads, cur->pad_idx);
                if (type != AVMEDIA_TYPE_VIDEO && type != AVMEDIA_TYPE_AUDIO) {
                    CASPAR_THROW_EXCEPTION(ffmpeg_error_t()
                                           << boost::errinfo_errno(EINVAL)
                                           << msg_info_t("only video and audio filters supported")
                    );
                }

                // TODO find stream based on link name
                // TODO share stream decoders between graphs
                AVStream* st = nullptr;
                for (auto i = 0ULL; i < ic_->nb_streams; ++i) {
                    st = ic_->streams[i];
                    if (st->codecpar->codec_type == type && used.find(static_cast<int>(i)) == used.end()) {
                        break;
                    }
                }

                if (!st) {
                    CASPAR_THROW_EXCEPTION(ffmpeg_error_t()
                                           << boost::errinfo_errno(EINVAL)
                                           << msg_info_t((boost::format("cannot find matching stream for input pad %d on filter %s") % cur->pad_idx % cur->filter_ctx->name).str())
                    );
                }

                used.insert(st->index);

                st->discard = AVDISCARD_DEFAULT;
                
                if (!decoders_[st->index]) {
                    decoders_.emplace(st->index, open_decoder(st));
                }

                auto avctx = decoders_[st->index];

                if (st->codecpar->codec_type == AVMEDIA_TYPE_VIDEO) {
                    auto args_str = (boost::format("video_size=%dx%d:pix_fmt=%d:time_base=%d/%d")
                                     % avctx->width % avctx->height
                                     % avctx->pix_fmt
                                     % avctx->pkt_timebase.num % avctx->pkt_timebase.den
                                     ).str();
                    const auto name_str = (boost::format("in%d") % st->index).str();

                    if (avctx->sample_aspect_ratio.num > 0 && avctx->sample_aspect_ratio.den > 0) {
                        args_str += (boost::format(":sar=%d/%d")
                                     % avctx->sample_aspect_ratio.num % avctx->sample_aspect_ratio.den
                                     ).str();
                    }

                    if (avctx->framerate.num > 0 && avctx->framerate.den > 0) {
                        args_str += (boost::format(":frame_rate=%d/%d")
                                     % avctx->framerate.num % avctx->framerate.den
                                     ).str();
                    }

                    AVFilterContext* source = nullptr;
                    FF(avfilter_graph_create_filter(&source, avfilter_get_by_name("buffer"), name_str.c_str(), args_str.c_str(), nullptr, graph.get()));
                    FF(avfilter_link(source, 0, cur->filter_ctx, cur->pad_idx));
                    sources_[st->index].push_back(source);
                } else if (st->codecpar->codec_type == AVMEDIA_TYPE_AUDIO) {
                    const auto args_str = (boost::format("time_base=%d/%d:sample_rate=%d:sample_fmt=%s:channel_layout=%#x")
                                           % avctx->pkt_timebase.num % avctx->pkt_timebase.den
                                           % avctx->sample_rate
                                           % av_get_sample_fmt_name(avctx->sample_fmt)
                                           % avctx->channel_layout
                                           ).str();
                    const auto name_str = (boost::format("in%d") % st->index).str();

                    AVFilterContext* source = nullptr;
                    FF(avfilter_graph_create_filter(&source, avfilter_get_by_name("abuffer"), name_str.c_str(), args_str.c_str(), nullptr, graph.get()));
                    FF(avfilter_link(source, 0, cur->filter_ctx, cur->pad_idx));
                    sources_[st->index].push_back(source);
                } else {
                    CASPAR_THROW_EXCEPTION(ffmpeg_error_t()
                                           << boost::errinfo_errno(EINVAL)
                                           << msg_info_t("invalid filter input media type")
                    );
                }
            }
        }

        // output
        {
            const auto cur = outputs;

            if (!cur || cur->next) {
                CASPAR_THROW_EXCEPTION(ffmpeg_error_t()
                                       << boost::errinfo_errno(EINVAL)
                                       << msg_info_t("invalid filter graph output count")
                );
            }

            if (avfilter_pad_get_type(cur->filter_ctx->output_pads, cur->pad_idx) != media_type) {
                CASPAR_THROW_EXCEPTION(ffmpeg_error_t()
                                       << boost::errinfo_errno(EINVAL)
                                       << msg_info_t("invalid filter output media type")
                );
            }

            FF(avfilter_link(cur->filter_ctx, cur->pad_idx, sink, 0));
        }

        FF(avfilter_graph_config(graph.get(), nullptr));

        if (media_type == AVMEDIA_TYPE_AUDIO) {
            video_sink_ = sink;
            video_graph_ = graph;
        } else if (media_type == AVMEDIA_TYPE_AUDIO) {
            audio_sink_ = sink;
            audio_graph_ = graph;
        }
    }

    std::shared_ptr<AVCodecContext> open_decoder(AVStream* stream)
    {
        const auto codec = avcodec_find_decoder(stream->codecpar->codec_id);
        if (!codec) {
            FF_RET(AVERROR_DECODER_NOT_FOUND, "avcodec_find_decoder");
        }

        const auto avctx = std::shared_ptr<AVCodecContext>(avcodec_alloc_context3(codec), [](AVCodecContext* ptr) { avcodec_free_context(&ptr); });
        if (!avctx) {
            FF_RET(AVERROR(ENOMEM), "avcodec_alloc_context3");
        }

        FF(avcodec_parameters_to_context(avctx.get(), stream->codecpar));

        FF(av_opt_set_int(avctx.get(), "refcounted_frames", 1, 0));

        avctx->pkt_timebase = stream->time_base;

        if (avctx->codec_type == AVMEDIA_TYPE_VIDEO) {
            avctx->framerate = av_guess_frame_rate(nullptr, stream, nullptr);
            avctx->sample_aspect_ratio = av_guess_sample_aspect_ratio(nullptr, stream, nullptr);
        } else if (avctx->codec_type == AVMEDIA_TYPE_AUDIO) {
            if (!avctx->channel_layout && avctx->channels) {
                avctx->channel_layout = av_get_default_channel_layout(avctx->channels);
            }
            if (!avctx->channels && avctx->channel_layout) {
                avctx->channels = av_get_channel_layout_nb_channels(avctx->channel_layout);
            }
        }

        FF(avcodec_open2(avctx.get(), codec, nullptr));

        return avctx;
    }

    std::shared_ptr<AVFrame> decode_frame(AVCodecContext* avctx)
    {
        const auto frame = alloc_frame();
        const auto ret = avcodec_receive_frame(avctx, frame.get());

        while (true) {
            if (ret == AVERROR(EAGAIN)) {
                auto packet = read_packet();

                for (auto& p : decoders_) {
                    if (p.first == packet->stream_index) {
                        // TODO overflow?
                        FF(avcodec_send_packet(avctx, packet.get()));
                    }
                }
            } else if (ret == AVERROR_EOF) {
                avcodec_flush_buffers(avctx);
                return nullptr;
            } else {
                FF_RET(ret, "avcodec_receive_frame");
                frame->pts = frame->best_effort_timestamp;
                return frame;
            }
        }
    }

    std::shared_ptr<AVFrame> filter_frame(AVFilterContext* sink, int nb_samples = -1)
    {
        std::shared_ptr<AVFrame> frame;
        int ret;

        while (true) {
            frame = alloc_frame();
            if (nb_samples >= 0) {
                ret = av_buffersink_get_samples(sink, frame.get(), nb_samples);
            } else {
                ret = av_buffersink_get_frame(sink, frame.get());
            }

            if (ret == AVERROR(EAGAIN)) {
                for (auto& p : sources_) {
                    auto avctx = decoders_[p.first].get();

                    unsigned int request_count = 0;
                    
                    for (auto& source : p.second) {
                        request_count = std::max(request_count, av_buffersrc_get_nb_failed_requests(source));
                    }

                    while (request_count-- > 0) {
                        frame = decode_frame(avctx);

                        for (auto& source : p.second) {
                            // TODO overflow?
                            FF(av_buffersrc_write_frame(source, frame.get()));
                        }
                    }
                }
            } else if (ret == AVERROR_EOF) {
                open_filter(vfilter_, AVMEDIA_TYPE_VIDEO);
                open_filter(afilter_, AVMEDIA_TYPE_AUDIO);
                return nullptr;
            } else {
                FF_RET(ret, "av_buffersink_get_frame");
                return frame;
            }

        }
    }

    std::shared_ptr<AVPacket> read_packet()
    {
        auto packet = alloc_packet();
        auto ret = av_read_frame(ic_.get(), packet.get());

        while (true) {
            if (ret == AVERROR_EOF) {
                if (!loop_) {
                    return nullptr;
                }

                {
                    auto ts = start_ != AV_NOPTS_VALUE ? start_ : 0;

                    if (ic_->start_time != AV_NOPTS_VALUE) {
                        ts += ic_->start_time, ic_->start_time;
                    }

                    FF(avformat_seek_file(ic_.get(), -1, INT64_MIN, ts, ts, 0));

                    seek_ = ts;
                }

                return packet;
            } else {
                FF_RET(ret, "av_read_frame");

                return packet;
            }
        }
    }
 
    core::mutable_frame make_frame(std::shared_ptr<AVFrame> video, std::shared_ptr<AVFrame> audio)
    {
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
            frame.audio_data().resize(audio_cadence_[0], 0);
        }

        return frame;
    }


public:

    bool next()
    {
        std::shared_ptr<AVFrame> video;
        std::shared_ptr<AVFrame> audio;

        if (time_ != AV_NOPTS_VALUE && duration_ != AV_NOPTS_VALUE && time_ >= duration_) {
            return true;
        }

        if (video_sink_) {
            std::shared_ptr<AVFrame> frame;

            const auto tb = av_buffersink_get_time_base(video_sink_);
            const auto first_pts = seek_ != AV_NOPTS_VALUE ? av_rescale_q(seek_, TIME_BASE_Q, tb) : AV_NOPTS_VALUE;

            while (true) {
                frame = filter_frame(video_sink_);

                if (first_pts == AV_NOPTS_VALUE || first_pts >= frame->pts) {
                    break;
                }
            }

            video = frame;
        }

        if (audio_sink_) {
            std::shared_ptr<AVFrame> frame;

            // Note: Uses 1 step rotated cadence for 1001 modes (1602, 1602, 1601, 1602, 1601)
            // This cadence fills the audio mixer most optimally.
            boost::range::rotate(audio_cadence_, std::end(audio_cadence_) - 1);

            const auto tb = av_buffersink_get_time_base(audio_sink_);
            const auto first_pts = seek_ != AV_NOPTS_VALUE ? av_rescale_q(seek_, TIME_BASE_Q, tb) : AV_NOPTS_VALUE;

            while (true) {
                frame = filter_frame(audio_sink_, audio_cadence_[0]);

                if (first_pts == AV_NOPTS_VALUE || first_pts >= frame->pts) {
                    break;
                }
            }

            audio = frame;
        }

        if (video) {
            time_ = av_rescale_q(video->pts, av_buffersink_get_time_base(video_sink_), TIME_BASE_Q);
        } else if (audio) {
            time_ = av_rescale_q(audio->pts, av_buffersink_get_time_base(audio_sink_), TIME_BASE_Q);
        }

        frame_ = core::draw_frame(make_frame(video, audio));

        return true;
    }

    core::draw_frame get()
    {
        return frame_;
    }
 
    void seek(int64_t ts)
    {
        //if (ts == AV_NOPTS_VALUE) {
        //    ts = 0;
        //}

        //if (ic_->start_time != AV_NOPTS_VALUE) {
        //    ts += ic_->start_time, ic_->start_time;
        //}

        //FF(avformat_seek_file(ic_.get(), -1, INT64_MIN, ts, ts, 0));

        //for (auto& p : decoders_) {
        //    avcodec_flush_buffers(p.second.get());
        //}

        //open_filter(vfilter_, AVMEDIA_TYPE_VIDEO);
        //open_filter(afilter_, AVMEDIA_TYPE_AUDIO);

        //seek_ = ts;
        //frame_ = core::draw_frame::late();
    }

    int64_t time() const
    {
        return time_ != AV_NOPTS_VALUE
            ? time_ - (ic_->start_time != AV_NOPTS_VALUE ? ic_->start_time : 0)
            : AV_NOPTS_VALUE;
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
        // TODO shortest?
        return duration_ == AV_NOPTS_VALUE && ic_->duration != AV_NOPTS_VALUE
            ? std::max<int64_t>(0, ic_->duration - (start_ != AV_NOPTS_VALUE ? start_ : 0))
            : duration_;
    }

    int width() const
    {
        return video_sink_ ? av_buffersink_get_w(video_sink_) : 0;
    }

    int height() const
    {
        return video_sink_ ? av_buffersink_get_h(video_sink_) : 0;
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

bool AVProducer::next()
{
    return impl_->next();
}

core::draw_frame AVProducer::get()
{
    return impl_->get();
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

}  // namespace ffmpeg
}  // namespace caspar
