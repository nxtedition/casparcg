#include "av_input.h"

#include "../util/av_assert.h"
#include "../util/av_util.h"

#include <common/env.h>
#include <common/except.h>
#include <common/os/thread.h>
#include <common/param.h>
#include <common/scope_exit.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/filesystem.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/regex.hpp>

#include <algorithm>
#include <set>

extern "C" {
#include <libavformat/avformat.h>
#include <libavutil/avutil.h>
#include <libavutil/eval.h>
}

namespace caspar { namespace ffmpeg {

namespace {

// Parses ffmpeg command line style options ("-key value ...") into an AVDictionary. Every option
// needs a value: a value-less "-flag" followed by another option takes that option as its value.
void set_options(AVDictionary** options, const std::string& str)
{
    static const boost::regex opt_exp("-(?<NAME>[^\\s]+)(\\s+(?<VALUE>[^\\s]+))?");

    for (auto it = boost::sregex_iterator(str.begin(), str.end(), opt_exp); it != boost::sregex_iterator(); ++it) {
        const auto name  = (*it)["NAME"].str();
        const auto value = (*it)["VALUE"].matched ? (*it)["VALUE"].str() : std::string();
        FF(av_dict_set(options, name.c_str(), value.c_str(), 0));
    }
}

// Options from <ffmpeg><producer><options><scope>. Unknown options are logged as unused once the
// input has been opened, but an invalid value for a known option makes the open fail.
void set_config_options(AVDictionary** options, const std::wstring& scope)
{
    const auto str = env::properties().get<std::wstring>(L"configuration.ffmpeg.producer.options." + scope, L"");
    if (!str.empty()) {
        set_options(options, u8(str));
    }
}

} // namespace

std::string build_open_options(const std::string&    filename,
                               cache_mode            cache,
                               AVDictionary**        options,
                               const AVInputFormat** input_format)
{
    static const std::set<std::wstring> PROTOCOLS_TREATED_AS_FORMATS = {L"dshow", L"v4l2", L"iec61883"};

    auto url       = filename;
    auto url_parts = caspar::protocol_split(u16(filename));
    if (url_parts.first == L"http" || url_parts.first == L"https") {
        FF(av_dict_set(options, "multiple_requests", "1", 0));
        FF(av_dict_set(options, "reconnect", "1", 0));
        FF(av_dict_set(options, "reconnect_on_http_error", "404,408,416,429,500,502,503,504", 0));
        FF(av_dict_set(options, "reconnect_streamed", "1", 0));
        FF(av_dict_set(options, "reconnect_delay_max", "120", 0));
        FF(av_dict_set(options, "user_agent", "caspar/3", 0));
        FF(av_dict_set(options, "referer", filename.c_str(), 0)); // HTTP referer header
        FF(av_dict_set(options, "prefer_libcurl", "1", 0));
        FF(av_dict_set(options, "initial_request_size", "32k", 0));
        FF(av_dict_set(options, "short_seek_size", "512k", 0));
        FF(av_dict_set(options, "max_retries", "32", 0));

    } else if (url_parts.first == L"rtmp" || url_parts.first == L"rtmps") {
        FF(av_dict_set(options, "rtmp_live", "live", 0));
    } else if (PROTOCOLS_TREATED_AS_FORMATS.find(url_parts.first) != PROTOCOLS_TREATED_AS_FORMATS.end()) {
        *input_format = av_find_input_format(u8(url_parts.first).c_str());
        url           = u8(url_parts.second);
    }

    if (cache != cache_mode::none) {
        FF(av_dict_set(options, "cache_dir", u8(cache_path()).c_str(), 0));
        url = "shared:" + url;
        // Wait this long for a block another reader (e.g. a prefetch) is fetching before fetching it
        // too. shared's 10 ms default makes concurrent readers download most blocks twice.
        FF(av_dict_set(options, "cache_timeout", "100000", 0));
    }

    if (*input_format == nullptr) {
        // TODO (fix) timeout?
        FF(av_dict_set(options, "rw_timeout", "60000000", 0)); // 60 second IO timeout
    }

    // Applied last so that user configuration wins over the defaults above.
    set_config_options(options, L"default");
    if (!url_parts.first.empty()) {
        // The TLS variants share their protocol implementation, so they share its scope as well.
        auto scope = url_parts.first;
        if (scope == L"https" || scope == L"rtmps") {
            scope.pop_back();
        }
        set_config_options(options, scope);
    }

    if (cache == cache_mode::playback) {
        set_config_options(options, L"shared");
    } else if (cache == cache_mode::prefetch) {
        // The "shared" scope tunes uninterrupted playback (e.g. ignore_errors). A prefetch must
        // instead fail loudly unless every block ends up in the cache, so these always win.
        // Only the cache size limit is shared with playback.
        FF(av_dict_set(options, "ignore_errors", "0", 0));
        FF(av_dict_set(options, "cache_read_only", "0", 0));
        FF(av_dict_set(options, "cache_size_max", std::to_string(prefetch_size_limit()).c_str(), 0));
        FF(av_dict_set(options, "retry_errors", "1", 0));
        FF(av_dict_set(options, "retry_corrupt", "1", 0));
        // Nothing waits on a prefetch, so leave pending blocks to whoever is fetching them.
        FF(av_dict_set(options, "cache_timeout", "30000000", 0));
    }

    return url;
}

int64_t prefetch_size_limit()
{
    AVDictionary* shared = nullptr;
    CASPAR_SCOPE_EXIT { av_dict_free(&shared); };
    set_config_options(&shared, L"shared");
    auto entry = av_dict_get(shared, "cache_size_max", nullptr, 0);
    // av_strtod, like ffmpeg's option parser, accepts suffixes such as 10G.
    return entry ? std::max<int64_t>(0, static_cast<int64_t>(av_strtod(entry->value, nullptr))) : 0;
}

std::wstring cache_path()
{
    return env::properties().get<std::wstring>(L"configuration.ffmpeg.producer.cache.path", L"./ffmpeg-cache");
}

Input::Input(const std::string&                  filename,
             std::shared_ptr<diagnostics::graph> graph,
             std::optional<bool>                 seekable,
             bool                                cache)
    : filename_(filename)
    , graph_(graph)
    , seekable_(seekable)
    , cache_(cache)
    , seek_ts_(AV_NOPTS_VALUE)
    , retry_timeout_(env::properties().get(L"configuration.ffmpeg.producer.cache.read-retry-timeout", 60))
{
    graph_->set_color("seek", diagnostics::color(1.0f, 0.5f, 0.0f));
    graph_->set_color("input", diagnostics::color(0.7f, 0.4f, 0.4f));

    buffer_.set_capacity(input_buffer_capacity);
    thread_ = boost::thread([this] {
        try {
            set_thread_name(L"[ffmpeg::av_producer::Input]");

            int consecutive_enomem = 0;

            while (true) {
                auto packet     = alloc_packet();
                int  ret        = 0;
                int  generation = 0;

                {
                    std::unique_lock<std::mutex> lock(ic_mutex_);
                    ic_cond_.wait(lock, [&] { return ic_ || abort_request_; });

                    if (abort_request_) {
                        break;
                    }

                    generation = seek_generation_;
                    // TODO (perf) Non blocking av_read_frame when possible.
                    ret = av_read_frame(reader(), packet.get());
                }

                if (abort_request_) {
                    break;
                }

                if (generation != read_generation_) {
                    // Seeked: forget the recovery state of the old position.
                    read_generation_ = generation;
                    delivered_dts_.clear();
                    resume_dts_.clear();
                    held_.reset();
                    failing_since_.reset();
                    retries_ = 0;
                }

                if (ret == AVERROR_EXIT) {
                    break;
                } else if (ret == AVERROR(EAGAIN)) {
                    boost::this_thread::yield();
                    continue;
                } else if (ret == AVERROR_EOF) {
                    eof_   = true;
                    packet = nullptr;
                } else if (ret == AVERROR(ENOMEM)) {
                    // Transient memory allocation failure inside the demuxer; log and retry rather
                    // than letting the exception escape and kill the read thread permanently.
                    // Sleep briefly to give the system a chance to free memory before retrying.
                    ++consecutive_enomem;
                    if (consecutive_enomem == 1) {
                        CASPAR_LOG(warning) << "av_input[" << filename_ << "] av_read_frame: out of memory, retrying";
                    } else if (consecutive_enomem >= 20) {
                        CASPAR_LOG(error) << "av_input[" << filename_
                                          << "] av_read_frame: too many consecutive out-of-memory errors, aborting";

                        // Pretend we reached EOF, to avoid the producer stalling expecting more packets
                        eof_   = true;
                        packet = nullptr;
                        buffer_.push(std::move(packet));
                        break;
                    }
                    boost::this_thread::sleep_for(boost::chrono::milliseconds(5));
                    continue;
                } else if (ret < 0 && cache_ && seekable_.value_or(true)) {
                    // Missing data in the cache: the packet held back before the error is truncated.
                    held_.reset();
                    if (!recover(ret)) {
                        // Pretend we reached EOF, to avoid the producer stalling expecting more packets
                        eof_ = true;
                        buffer_.push(nullptr);
                        break;
                    }
                    continue;
                } else {
                    consecutive_enomem = 0;
                    FF_RET(ret, "av_read_frame");
                }

                consecutive_enomem = 0;
                deliver(std::move(packet));
            }
        } catch (...) {
            CASPAR_LOG_CURRENT_EXCEPTION();
            boost::this_thread::sleep_for(boost::chrono::milliseconds(5));
        }
    });
}

Input::~Input()
{
    abort_request_ = true;
    ic_cond_.notify_all();

    std::shared_ptr<AVPacket> packet;
    while (buffer_.try_pop(packet))
        ;

    thread_.join();
}

int Input::interrupt_cb(void* ctx)
{
    auto input = reinterpret_cast<Input*>(ctx);
    return input->abort_request_ ? 1 : 0;
}

bool Input::try_pop(std::shared_ptr<AVPacket>& packet)
{
    auto result = buffer_.try_pop(packet);
    graph_->set_value("input", (static_cast<double>(buffer_.size()) / buffer_.capacity()));
    return result;
}

AVFormatContext*       Input::operator->() { return ic_.get(); }
AVFormatContext* const Input::operator->() const { return ic_.get(); }

void Input::abort()
{
    abort_request_ = true;
    ic_cond_.notify_all();

    std::shared_ptr<AVPacket> packet;
    while (buffer_.try_pop(packet))
        ;
}

void Input::reset(std::optional<int64_t> time)
{
    std::unique_lock<std::mutex> lock(ic_mutex_);
    internal_reset();

    if (time) {
        const auto ts = *time + (ic_->start_time != AV_NOPTS_VALUE ? ic_->start_time : 0);
        if (ts != ic_->start_time) {
            FF(avformat_seek_file(ic_.get(), -1, INT64_MIN, ts, ts, 0));
        }
    }
}

std::shared_ptr<AVFormatContext> open_input(const std::string&     filename,
                                            cache_mode             cache,
                                            std::optional<bool>    seekable,
                                            const AVIOInterruptCB& interrupt)
{
    AVDictionary* options = nullptr;
    CASPAR_SCOPE_EXIT { av_dict_free(&options); };

    if (seekable) {
        CASPAR_LOG(debug) << "av_input[" + filename + "] Disabled seeking";
        FF(av_dict_set(&options, "seekable", *seekable ? "1" : "0", 0));
    }

    const AVInputFormat* input_format = nullptr;
    auto                 url          = build_open_options(filename, cache, &options, &input_format);

    AVFormatContext* ic    = avformat_alloc_context();
    ic->interrupt_callback = interrupt;

    FF(avformat_open_input(&ic, url.c_str(), input_format, &options));
    auto ic2 = std::shared_ptr<AVFormatContext>(ic, [](AVFormatContext* ctx) { avformat_close_input(&ctx); });

    for (auto& p : to_map(&options)) {
        CASPAR_LOG(warning) << "av_input[" + filename + "]" << " Unused option " << p.first << "=" << p.second;
    }

    FF(avformat_find_stream_info(ic2.get(), nullptr));
    return ic2;
}

std::shared_ptr<AVFormatContext> Input::open()
{
    return open_input(
        filename_, cache_ ? cache_mode::playback : cache_mode::none, seekable_, AVIOInterruptCB{Input::interrupt_cb, this});
}

void Input::internal_reset()
{
    ic_ = open();
    recovered_.reset();
    ++seek_generation_;
    seek_ts_ = AV_NOPTS_VALUE;
    ic_cond_.notify_all();
}

AVFormatContext* Input::reader() const { return recovered_ ? recovered_.get() : ic_.get(); }

void Input::deliver(std::shared_ptr<AVPacket> packet)
{
    if (cache_ && packet && (packet->flags & AV_PKT_FLAG_CORRUPT)) {
        // Hold it back for one read: if that read fails, it was cut short by missing data.
        std::swap(held_, packet);
        if (!packet) {
            return;
        }
    } else if (held_) {
        push(std::move(held_));
    }
    push(std::move(packet));
}

void Input::push(std::shared_ptr<AVPacket> packet)
{
    if (cache_ && packet && packet->dts != AV_NOPTS_VALUE) {
        const auto index = static_cast<size_t>(packet->stream_index);
        if (delivered_dts_.size() <= index) {
            delivered_dts_.resize(index + 1, AV_NOPTS_VALUE);
        }
        if (index < resume_dts_.size() && resume_dts_[index] != AV_NOPTS_VALUE) {
            if (packet->dts <= resume_dts_[index]) {
                return; // delivered before the reopen
            }
            resume_dts_[index] = AV_NOPTS_VALUE;
        }
        delivered_dts_[index] = packet->dts;

        if (failing_since_) {
            CASPAR_LOG(info) << "av_input[" << filename_ << "] Recovered after "
                             << std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() -
                                                                                 *failing_since_)
                                    .count()
                             << " s";
            failing_since_.reset();
            retries_ = 0;
        }
    }

    buffer_.push(std::move(packet));
    graph_->set_value("input", (static_cast<double>(buffer_.size()) / buffer_.capacity()));
}

// Data missing from the cache: reopen (retrying the source) and seek back, since reading on would
// skip the failed packet. Returns false to give up.
bool Input::recover(int error)
{
    using namespace std::chrono;

    if (!failing_since_) {
        CASPAR_LOG(warning) << "av_input[" << filename_ << "] Read failed: " << error_string(error)
                            << ". Reopening to retry.";
        graph_->set_tag(diagnostics::tag_severity::WARNING, "read-retry");
        failing_since_ = steady_clock::now();
    }

    while (true) {
        if (retry_timeout_.count() > 0 && steady_clock::now() - *failing_since_ >= retry_timeout_) {
            CASPAR_LOG(error) << "av_input[" << filename_ << "] Giving up after " << retry_timeout_.count()
                              << " s of read errors: " << error_string(error);
            return false;
        }

        int     generation = 0;
        int64_t target     = AV_NOPTS_VALUE;
        {
            std::unique_lock<std::mutex> lock(ic_mutex_);
            generation = seek_generation_;
            auto delay = steady_clock::duration(milliseconds(1000 << std::min(retries_++, 3)));
            if (retry_timeout_.count() > 0) {
                // The last retry happens right at the deadline.
                delay = std::min(delay, *failing_since_ + retry_timeout_ - steady_clock::now());
            }
            if (ic_cond_.wait_for(lock, delay, [&] { return abort_request_ || seek_generation_ != generation; })) {
                return true; // the read loop handles abort and seek
            }

            // Back to the keyframe before the earliest packet any stream still needs.
            for (size_t n = 0; n < delivered_dts_.size() && n < ic_->nb_streams; ++n) {
                if (delivered_dts_[n] != AV_NOPTS_VALUE) {
                    const auto ts = av_rescale_q(delivered_dts_[n], ic_->streams[n]->time_base, AV_TIME_BASE_Q);
                    target        = target == AV_NOPTS_VALUE ? ts : std::min(target, ts);
                }
            }
            if (target == AV_NOPTS_VALUE) {
                target = seek_ts_;
            }
        }

        try {
            // Opened without the lock: with the source unreachable this can take a while.
            auto ctx = open();
            if (target != AV_NOPTS_VALUE) {
                FF(avformat_seek_file(ctx.get(), -1, INT64_MIN, target, target, 0));
            }

            std::unique_lock<std::mutex> lock(ic_mutex_);
            if (seek_generation_ != generation) {
                return true;
            }
            if (ctx->nb_streams != ic_->nb_streams) {
                CASPAR_LOG(error) << "av_input[" << filename_ << "] Reopened with different streams";
                return false;
            }
            for (unsigned n = 0; n < ctx->nb_streams; ++n) {
                if (ctx->streams[n]->codecpar->codec_id != ic_->streams[n]->codecpar->codec_id) {
                    CASPAR_LOG(error) << "av_input[" << filename_ << "] Reopened with different streams";
                    return false;
                }
            }
            recovered_  = std::move(ctx);
            resume_dts_ = delivered_dts_;
            CASPAR_LOG(debug) << "av_input[" << filename_ << "] Reopened, retry " << retries_;
            return true;
        } catch (...) {
            if (abort_request_) {
                return true;
            }
            CASPAR_LOG(debug) << "av_input[" << filename_ << "] Reopen failed, retry " << retries_;
        }
    }
}

bool Input::eof() const { return eof_; }

void Input::seek(int64_t ts, bool flush)
{
    std::unique_lock<std::mutex> lock(ic_mutex_);

    if (ic_ && ts != ic_->start_time && ts != AV_NOPTS_VALUE) {
        FF(avformat_seek_file(reader(), -1, INT64_MIN, ts, ts, 0));
        ++seek_generation_;
        seek_ts_ = ts;
        ic_cond_.notify_all();
    } else {
        internal_reset();
    }

    if (flush) {
        std::shared_ptr<AVPacket> packet;
        while (buffer_.try_pop(packet))
            ;
    }
    eof_ = false;

    graph_->set_tag(diagnostics::tag_severity::INFO, "seek");
}

}} // namespace caspar::ffmpeg
