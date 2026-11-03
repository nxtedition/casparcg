#include "prefetch.h"

#include "av_input.h"
#include "ffmpeg_producer.h"
#include "prefetch_strategy.h"

#include "../util/av_assert.h"
#include "../util/av_util.h"

#include <common/env.h>
#include <common/log.h>
#include <common/os/thread.h>
#include <common/param.h>
#include <common/timer.h>
#include <common/utf.h>

#include <core/producer/stage.h>
#include <core/video_channel.h>
#include <core/video_format.h>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/filesystem.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/regex.hpp>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

extern "C" {
#include <libavformat/avformat.h>
#include <libavformat/avio.h>
}

namespace caspar { namespace ffmpeg {

namespace {

class prefetcher
{
    struct job
    {
        job(spl::shared_ptr<prefetch_strategy> strategy, int priority, uint64_t seq, prefetcher* owner)
            : strategy(std::move(strategy))
            , key(this->strategy->describe())
            , priority(priority)
            , seq(seq)
            , owner(owner)
        {
        }

        spl::shared_ptr<prefetch_strategy> strategy;
        std::string                        key;
        int                                priority;
        uint64_t                           seq;
        std::atomic<bool>                  cancelled{false};
        prefetcher*                        owner;
    };

    std::mutex                                  mutex_;
    std::condition_variable                     cond_;
    std::vector<std::shared_ptr<job>>           queue_;
    std::map<std::string, std::shared_ptr<job>> pending_; // queued or running, by key
    std::vector<std::thread>                    workers_;
    std::atomic<bool>                           abort_{false};
    uint64_t                                    next_seq_ = 0;

    // Higher priority first, then in the order they were queued.
    static bool runs_before(const std::shared_ptr<job>& a, const std::shared_ptr<job>& b)
    {
        return a->priority != b->priority ? a->priority > b->priority : a->seq < b->seq;
    }

  public:
    enum class enqueue_result
    {
        queued,
        reprioritised,
        already_pending,
    };

    ~prefetcher()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            abort_ = true;
        }
        cond_.notify_all();
        for (auto& worker : workers_) {
            worker.join();
        }
    }

    enqueue_result enqueue(spl::shared_ptr<prefetch_strategy> strategy, int priority)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto key = strategy->describe();
        auto it  = pending_.find(key);
        if (it != pending_.end()) {
            auto& existing = it->second;
            if (existing->priority == priority || std::find(queue_.begin(), queue_.end(), existing) == queue_.end()) {
                return enqueue_result::already_pending;
            }
            existing->priority = priority;
            return enqueue_result::reprioritised;
        }

        auto j = std::make_shared<job>(std::move(strategy), priority, next_seq_++, this);
        pending_.emplace(std::move(key), j);
        queue_.push_back(std::move(j));

        // Workers are started lazily so that servers which never prefetch don't pay for them.
        auto concurrency =
            std::max(1, env::properties().get<int>(L"configuration.ffmpeg.producer.cache.prefetch-concurrency", 2));
        if (workers_.size() < static_cast<size_t>(concurrency) && workers_.size() < pending_.size()) {
            workers_.emplace_back([this] { run(); });
        }

        cond_.notify_one();
        return enqueue_result::queued;
    }

    // Cancels the prefetch with key, or without one all prefetches of url. Returns how many it cancelled.
    int cancel(const std::string& url, const std::optional<std::string>& key)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        int count = 0;
        for (auto it = pending_.begin(); it != pending_.end();) {
            auto j = it->second;
            if (key ? j->key != *key : j->strategy->url() != url) {
                ++it;
                continue;
            }
            // A running job stops within the shared protocol's interrupt poll and logs that itself. It leaves
            // pending_ now, so that the same PREFETCH can be queued again right away.
            j->cancelled = true;
            auto queued  = std::find(queue_.begin(), queue_.end(), j);
            if (queued != queue_.end()) {
                queue_.erase(queued);
                CASPAR_LOG(info) << "ffmpeg[" << j->key << "] Prefetch cancelled";
            }
            it = pending_.erase(it);
            ++count;
        }
        return count;
    }

  private:
    static int interrupt_cb(void* ctx)
    {
        auto j = static_cast<job*>(ctx);
        return j->owner->abort_ || j->cancelled ? 1 : 0;
    }

    void run()
    {
        set_thread_name(L"[ffmpeg::prefetch]");

        while (true) {
            std::shared_ptr<job> j;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cond_.wait(lock, [&] { return abort_ || !queue_.empty(); });
                if (abort_) {
                    return;
                }
                auto next = std::min_element(queue_.begin(), queue_.end(), runs_before);
                j         = std::move(*next);
                queue_.erase(next);
            }

            prefetch(*j);

            // A cancelled job's key may already belong to a new job.
            std::lock_guard<std::mutex> lock(mutex_);
            auto                        it = pending_.find(j->key);
            if (it != pending_.end() && it->second == j) {
                pending_.erase(it);
            }
        }
    }

    void prefetch(job& j)
    {
        caspar::timer     timer;
        prefetch_progress progress;
        const auto&       name = j.key;

        try {
            CASPAR_LOG(info) << "ffmpeg[" << name << "] Prefetch started";

            boost::filesystem::create_directories(cache_path());

            j.strategy->fill(AVIOInterruptCB{interrupt_cb, &j}, progress);

            if (progress.size > progress.bytes) {
                CASPAR_LOG(info) << "ffmpeg[" << name << "] Prefetch done (" << progress.bytes << " of "
                                 << progress.size << " bytes in " << timer.elapsed() << "s)";
            } else {
                CASPAR_LOG(info) << "ffmpeg[" << name << "] Prefetch done (" << progress.bytes << " bytes in "
                                 << timer.elapsed() << "s)";
            }
        } catch (...) {
            if (j.cancelled) {
                CASPAR_LOG(info) << "ffmpeg[" << name << "] Prefetch cancelled after " << progress.bytes << " bytes";
            } else if (abort_) {
                CASPAR_LOG(info) << "ffmpeg[" << name << "] Prefetch aborted";
            } else {
                log_failure(name, progress);
            }
        }
    }

    // Called from a catch block.
    static void log_failure(const std::string& name, const prefetch_progress& progress)
    {
        try {
            throw;
        } catch (ffmpeg_error_t& e) {
            auto err = boost::get_error_info<ffmpeg_errn_info>(e);
            CASPAR_LOG(error) << "ffmpeg[" << name << "] Prefetch failed after " << progress.bytes
                              << " bytes: " << (err ? error_string(*err) : std::string("unknown error"));
        } catch (std::runtime_error& e) {
            CASPAR_LOG(error) << "ffmpeg[" << name << "] Prefetch failed: " << e.what();
        } catch (...) {
            CASPAR_LOG_CURRENT_EXCEPTION();
            CASPAR_LOG(error) << "ffmpeg[" << name << "] Prefetch failed";
        }
    }
};

std::mutex                  instance_mutex;
std::unique_ptr<prefetcher> instance;

struct prefetch_request
{
    spl::shared_ptr<prefetch_strategy> strategy;
    bool                               has_range;
    int                                priority;
};

// [channel[-layer]] url [SEEK n] [IN n] [LENGTH n] [OUT n] [PRIORITY n], shared by the PREFETCH commands. Logs
// and returns nothing if it's invalid.
std::optional<prefetch_request> parse_request(const std::wstring& command, protocol::amcp::command_context& ctx)
{
    auto params = ctx.parameters;

    // The channel is only needed for values in frames. Like LOCK, PREFETCH is a global command that reads the
    // channel itself, so that a locked channel doesn't block it.
    static const boost::wregex             channel_exp(L"(\\d{1,9})(-\\d+)?");
    boost::wsmatch                         match;
    std::optional<core::video_format_desc> format_desc;
    if (params.size() > 1 && boost::regex_match(params.at(0), match, channel_exp)) {
        auto channel = std::stoi(match[1].str());
        if (channel < 1 || channel > static_cast<int>(ctx.channels->size())) {
            CASPAR_LOG(error) << command << L": no channel " << params.at(0);
            return {};
        }
        format_desc = ctx.channels->at(channel - 1).raw_channel->stage()->video_format_desc();
        params.erase(params.begin());
    }

    auto resolved = resolve_path(params.at(0));

    // Local files have nothing to cache, and only real protocols go through the shared protocol.
    if (!resolved || !boost::contains(resolved->path, L"://") ||
        avio_find_protocol_name(u8(resolved->path).c_str()) == nullptr) {
        CASPAR_LOG(error) << L"ffmpeg[" << params.at(0) << L"] " << command << L": not a supported url";
        return {};
    }

    auto has_range = contains_param(L"SEEK", params) || contains_param(L"IN", params) ||
                     contains_param(L"LENGTH", params) || contains_param(L"OUT", params);
    std::optional<play_range> range;
    int                       priority = 0;
    try {
        if (has_range) {
            range = parse_play_range(params, format_desc ? &*format_desc : nullptr);
        }
        priority = get_param(L"PRIORITY", params, 0);
    } catch (user_error& e) {
        auto msg = boost::get_error_info<msg_info_t>(e);
        CASPAR_LOG(error) << L"ffmpeg[" << resolved->path << L"] " << command << L": "
                          << (msg ? u16(*msg) : L"invalid SEEK, IN, LENGTH, OUT or PRIORITY");
        return {};
    }

    auto url = u8(resolved->path);
    return prefetch_request{
        range ? create_range_prefetch(url, *range) : create_whole_file_prefetch(url), has_range, priority};
}

} // namespace

std::wstring prefetch_command(protocol::amcp::command_context& ctx)
{
    auto request = parse_request(L"PREFETCH", ctx);
    if (!request) {
        return L"403 PREFETCH FAILED\r\n";
    }

    std::lock_guard<std::mutex> lock(instance_mutex);
    if (!instance) {
        instance = std::make_unique<prefetcher>();
    }
    const auto name = u16(request->strategy->describe());
    switch (instance->enqueue(request->strategy, request->priority)) {
        case prefetcher::enqueue_result::queued:
            break;
        case prefetcher::enqueue_result::reprioritised:
            CASPAR_LOG(info) << L"ffmpeg[" << name << L"] Prefetch moved to priority " << request->priority;
            break;
        case prefetcher::enqueue_result::already_pending:
            CASPAR_LOG(info) << L"ffmpeg[" << name << L"] Prefetch already in progress";
            break;
    }

    return L"202 PREFETCH OK\r\n";
}

std::wstring prefetch_cancel_command(protocol::amcp::command_context& ctx)
{
    auto request = parse_request(L"PREFETCH CANCEL", ctx);
    if (!request) {
        return L"403 PREFETCH CANCEL FAILED\r\n";
    }

    const auto& strategy = *request->strategy;
    const auto  key      = request->has_range ? std::optional<std::string>(strategy.describe()) : std::nullopt;

    std::lock_guard<std::mutex> lock(instance_mutex);
    if (!instance || instance->cancel(strategy.url(), key) == 0) {
        CASPAR_LOG(info) << L"ffmpeg[" << u16(key ? *key : strategy.url()) << L"] No prefetch to cancel";
        return L"404 PREFETCH CANCEL FAILED\r\n";
    }

    return L"202 PREFETCH CANCEL OK\r\n";
}

void check_size_limit(int64_t bytes, int64_t limit)
{
    if (limit > 0 && bytes > limit) {
        throw std::runtime_error("larger than cache_size_max (" + std::to_string(limit) + " bytes)");
    }
}

void prefetch_uninit()
{
    std::lock_guard<std::mutex> lock(instance_mutex);
    instance.reset();
}

}} // namespace caspar::ffmpeg
