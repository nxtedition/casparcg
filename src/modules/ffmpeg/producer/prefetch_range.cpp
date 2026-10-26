#include "prefetch_strategy.h"

#include "av_input.h"

#include "../util/av_assert.h"
#include "../util/av_util.h"

#include <common/env.h>

#include <core/video_format.h>

#include <boost/property_tree/ptree.hpp>

#include <algorithm>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <vector>

extern "C" {
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
}

namespace caspar { namespace ffmpeg {

namespace {

const AVRational TIME_BASE_Q = {1, AV_TIME_BASE};

std::string seconds(int64_t time)
{
    std::ostringstream str;
    str << std::fixed << std::setprecision(3) << static_cast<double>(time) / AV_TIME_BASE << "s";
    return str.str();
}

class range_prefetch : public prefetch_strategy
{
    const std::string      url_;
    int64_t                start_ = 0; // relative to the clip's start_time, like av_producer
    int64_t                seek_  = 0;
    std::optional<int64_t> end_;

  public:
    range_prefetch(std::string url, const play_range& range, const core::video_format_desc& format_desc)
        : url_(std::move(url))
    {
        // Frames are converted exactly as av_producer does it.
        const AVRational format_tb{format_desc.duration, format_desc.time_scale * format_desc.field_count};

        start_ = range.start ? av_rescale_q(*range.start, format_tb, TIME_BASE_Q) : 0;
        seek_  = range.seek ? av_rescale_q(*range.seek, format_tb, TIME_BASE_Q) : start_;
        if (range.duration) {
            end_ = start_ + av_rescale_q(*range.duration, format_tb, TIME_BASE_Q);
        }
    }

    std::string describe() const override
    {
        auto str = url_ + " " + seconds(std::min(seek_, start_)) + "-" + (end_ ? seconds(*end_) : "end");
        if (seek_ != start_) {
            str += " seek " + seconds(seek_);
        }
        return str;
    }

    void fill(const AVIOInterruptCB& interrupt, prefetch_progress& progress) override
    {
        // Opened like the producer opens it, so that the header and probe reads are cached as well.
        auto ic = open_input(url_, cache_mode::prefetch, {}, interrupt);

        auto limit     = prefetch_size_limit();
        progress.size  = avio_size(ic->pb);
        progress.bytes = ic->pb->bytes_read;
        check_size_limit(progress.bytes, limit);

        const auto start_time = ic->start_time != AV_NOPTS_VALUE ? ic->start_time : 0;
        auto       seek       = [&](int64_t time) {
            FF(avformat_seek_file(ic.get(), -1, INT64_MIN, time + start_time, time + start_time, 0));
        };

        // PLAY seeks to SEEK, and to IN when it loops. Demuxers without an index (e.g. mpegts) read the
        // file to find a position, so both seeks are repeated.
        const auto first = std::min(seek_, start_);
        const auto last  = std::max(seek_, start_);
        if (last != first) {
            seek(last);
        }
        if (first != 0 || last != first) {
            seek(first);
        }

        // Only audio and video decide when the range has been read; sparse streams may never get there.
        std::vector<bool> done(ic->nb_streams, true);
        for (unsigned i = 0; i < ic->nb_streams; ++i) {
            const auto st = ic->streams[i];
            done[i] =
                !(st->codecpar->codec_type == AVMEDIA_TYPE_AUDIO ||
                  (st->codecpar->codec_type == AVMEDIA_TYPE_VIDEO && !(st->disposition & AV_DISPOSITION_ATTACHED_PIC)));
        }

        // Covers what the decoders still need past OUT, e.g. for B-frame reordering.
        const auto margin = static_cast<int64_t>(
            std::max(0.0, env::properties().get(L"configuration.ffmpeg.producer.cache.prefetch-margin", 1.0)) *
            AV_TIME_BASE);
        const auto end        = end_ ? *end_ + margin + start_time : INT64_MAX;
        auto       read_ahead = input_buffer_capacity; // playback reads this far past the end
        int64_t    packets    = 0;
        auto       packet     = alloc_packet();

        while (read_ahead > 0) {
            auto ret = av_read_frame(ic.get(), packet.get());
            if (ret == AVERROR_EOF) {
                break;
            }
            FF_RET(ret, "av_read_frame");
            ++packets;

            const auto index = static_cast<size_t>(packet->stream_index);
            const auto ts    = packet->dts != AV_NOPTS_VALUE ? packet->dts : packet->pts;
            if (index < done.size() && ts != AV_NOPTS_VALUE &&
                av_rescale_q(ts, ic->streams[index]->time_base, TIME_BASE_Q) >= end) {
                done[index] = true;
            }
            av_packet_unref(packet.get());

            progress.bytes = ic->pb->bytes_read;
            check_size_limit(progress.bytes, limit);

            if (std::all_of(done.begin(), done.end(), [](bool d) { return d; })) {
                --read_ahead;
            }
        }

        if (packets == 0) {
            throw std::runtime_error("nothing to read from " + seconds(first) + ", past the end of the clip?");
        }
    }
};

} // namespace

spl::shared_ptr<prefetch_strategy>
create_range_prefetch(std::string url, const play_range& range, const core::video_format_desc& format_desc)
{
    return spl::make_shared<range_prefetch>(std::move(url), range, format_desc);
}

}} // namespace caspar::ffmpeg
