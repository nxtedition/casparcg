#include "prefetch_strategy.h"

#include "av_input.h"

#include "../util/av_assert.h"
#include "../util/av_util.h"

#include <common/log.h>
#include <common/scope_exit.h>

#include <stdexcept>
#include <vector>

extern "C" {
#include <libavformat/avformat.h>
#include <libavformat/avio.h>
}

namespace caspar { namespace ffmpeg {

namespace {

class whole_file_prefetch : public prefetch_strategy
{
    const std::string url_;

  public:
    explicit whole_file_prefetch(std::string url)
        : url_(std::move(url))
    {
    }

    std::string describe() const override { return url_; }

    void fill(const AVIOInterruptCB& interrupt, prefetch_progress& progress) override
    {
        AVDictionary* options = nullptr;
        CASPAR_SCOPE_EXIT { av_dict_free(&options); };

        const AVInputFormat* input_format = nullptr;
        auto                 url          = build_open_options(url_, cache_mode::prefetch, &options, &input_format);

        AVIOContext* io = nullptr;
        FF(avio_open2(&io, url.c_str(), AVIO_FLAG_READ, &interrupt, &options));
        CASPAR_SCOPE_EXIT { avio_closep(&io); };

        for (auto& p : to_map(&options)) {
            CASPAR_LOG(warning) << "ffmpeg[" << url_ << "] Prefetch unused option " << p.first << "=" << p.second;
        }

        auto limit    = prefetch_size_limit();
        progress.size = avio_size(io);
        check_size_limit(progress.size, limit);

        // The data itself is dropped.
        std::vector<unsigned char> buf(1 << 20);
        while (true) {
            auto ret = avio_read(io, buf.data(), static_cast<int>(buf.size()));
            if (ret == AVERROR_EOF || ret == 0) {
                break;
            }
            FF_RET(ret, "avio_read");
            progress.bytes += ret;
            check_size_limit(progress.bytes, limit);
        }

        if (progress.size > 0 && progress.bytes != progress.size) {
            throw std::runtime_error("read " + std::to_string(progress.bytes) + " of " +
                                     std::to_string(progress.size) + " bytes");
        }
    }
};

} // namespace

spl::shared_ptr<prefetch_strategy> create_whole_file_prefetch(std::string url)
{
    return spl::make_shared<whole_file_prefetch>(std::move(url));
}

}} // namespace caspar::ffmpeg
