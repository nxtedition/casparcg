#include "prefetch.h"

#include "av_input.h"
#include "ffmpeg_producer.h"
#include "prefetch_state.h"
#include "prefetch_strategy.h"

#include "../util/av_assert.h"
#include "../util/av_util.h"

#include <common/env.h>
#include <common/log.h>
#include <common/os/thread.h>
#include <common/param.h>
#include <common/scope_exit.h>
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
#include <sstream>
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
    enum class stop_reason
    {
        none,
        cancel,
        preempt,
    };

    struct job
    {
        job(spl::shared_ptr<prefetch_strategy> strategy,
            std::string                        request,
            int                                priority,
            uint64_t                           seq,
            prefetcher*                        owner)
            : strategy(std::move(strategy))
            , key(this->strategy->describe())
            , requests{std::move(request)}
            , priority(priority)
            , seq(seq)
            , owner(owner)
        {
        }

        spl::shared_ptr<prefetch_strategy> strategy;
        std::string                        key;
        std::vector<std::string>           requests; // every spelling of it; guarded by mutex_
        int                                priority;
        uint64_t                           seq;
        std::atomic<stop_reason>           stop{stop_reason::none};
        bool                               started = false; // ran before, i.e. was preempted if queued
        prefetch_progress                  progress;
        prefetcher*                        owner;
    };

    std::mutex                                  mutex_;
    std::condition_variable                     cond_;
    std::vector<std::shared_ptr<job>>           queue_;
    std::vector<std::shared_ptr<job>>           running_; // in the order they started
    std::map<std::string, std::shared_ptr<job>> pending_; // queued or running, by key
    std::vector<std::thread>                    workers_;
    std::atomic<bool>                           abort_{false};
    uint64_t                                    next_seq_ = 0;

    // Higher priority first, then in the order they were queued.
    static bool runs_before(const std::shared_ptr<job>& a, const std::shared_ptr<job>& b)
    {
        return a->priority != b->priority ? a->priority > b->priority : a->seq < b->seq;
    }

    // Stops running jobs for queued ones that outrank them and have no worker to go to. Jobs that are already
    // stopping count as free workers, so each queued job stops at most one running job.
    void preempt()
    {
        auto queued = queue_;
        std::sort(queued.begin(), queued.end(), runs_before);

        auto busy =
            std::count_if(running_.begin(), running_.end(), [](const auto& j) { return j->stop == stop_reason::none; });
        for (auto i = workers_.size() - static_cast<size_t>(busy); i < queued.size(); ++i) {
            // The lowest priority, and the one started last of those, since it has done the least.
            std::shared_ptr<job> victim;
            for (const auto& j : running_) {
                if (j->stop == stop_reason::none && (!victim || j->priority <= victim->priority)) {
                    victim = j;
                }
            }
            if (!victim || victim->priority >= queued[i]->priority) {
                return;
            }
            victim->stop = stop_reason::preempt;
            CASPAR_LOG(info) << "ffmpeg[" << victim->key << "] Prefetch preempted by " << queued[i]->key;
        }
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

    enqueue_result enqueue(spl::shared_ptr<prefetch_strategy> strategy, std::string request, int priority)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto key = strategy->describe();
        auto it  = pending_.find(key);
        if (it != pending_.end()) {
            auto& existing = it->second;
            // Another spelling of the same range is the same job, but STATUS lists it too.
            auto& requests = existing->requests;
            if (std::find(requests.begin(), requests.end(), request) == requests.end()) {
                requests.push_back(std::move(request));
            }
            if (existing->priority == priority || std::find(queue_.begin(), queue_.end(), existing) == queue_.end()) {
                return enqueue_result::already_pending;
            }
            existing->priority = priority;
            preempt();
            return enqueue_result::reprioritised;
        }

        auto j = std::make_shared<job>(std::move(strategy), std::move(request), priority, next_seq_++, this);
        pending_.emplace(std::move(key), j);
        queue_.push_back(std::move(j));

        // Workers are started lazily so that servers which never prefetch don't pay for them.
        auto concurrency =
            std::max(1, env::properties().get<int>(L"configuration.ffmpeg.producer.cache.prefetch-concurrency", 2));
        if (workers_.size() < static_cast<size_t>(concurrency) && workers_.size() < pending_.size()) {
            workers_.emplace_back([this] { run(); });
        }
        preempt();

        cond_.notify_one();
        return enqueue_result::queued;
    }

    // Cancels the prefetch with key. Returns whether there was one.
    bool cancel(const std::string& key)
    {
        std::vector<std::shared_ptr<job>> paused;
        CASPAR_SCOPE_EXIT
        {
            // A running job records its end itself; a paused one isn't running, so its "paused" is replaced here.
            for (const auto& j : paused) {
                record(*j, prefetch_state::cancelled);
            }
        };
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = pending_.find(key);
        if (it == pending_.end()) {
            return false;
        }
        // A running job stops within the shared protocol's interrupt poll and logs that itself. It leaves
        // pending_ now, so that the same PREFETCH can be queued again right away.
        auto j      = it->second;
        j->stop     = stop_reason::cancel;
        auto queued = std::find(queue_.begin(), queue_.end(), j);
        if (queued != queue_.end()) {
            queue_.erase(queued);
            CASPAR_LOG(info) << "ffmpeg[" << j->key << "] Prefetch cancelled";
            if (j->started) {
                paused.push_back(j);
            }
        }
        pending_.erase(it);
        return true;
    }

    struct list_entry
    {
        const wchar_t*           state;
        int                      priority;
        int64_t                  bytes;
        int64_t                  size;
        std::string              key;
        std::vector<std::string> requests;
    };

    // The jobs (only url's, if given), running first, then the queue in the order it will run.
    std::vector<list_entry> list(const std::optional<std::string>& url)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // A job that is being preempted is already back in line, as far as anyone asking is concerned.
        std::vector<std::shared_ptr<job>> running;
        auto                              queued = queue_;
        for (const auto& j : running_) {
            if (j->stop == stop_reason::none) {
                running.push_back(j);
            } else if (j->stop == stop_reason::preempt) {
                queued.push_back(j);
            }
        }
        std::sort(queued.begin(), queued.end(), runs_before);

        std::vector<list_entry> entries;
        auto                    add = [&](const wchar_t* state, const job& j) {
            if (!url || j.strategy->url() == *url) {
                entries.push_back(
                    {state, j.priority, j.progress.bytes.load(), j.progress.size.load(), j.key, j.requests});
            }
        };
        for (const auto& j : running) {
            add(L"RUNNING", *j);
        }
        for (const auto& j : queued) {
            add(L"QUEUED", *j);
        }
        return entries;
    }

  private:
    static int interrupt_cb(void* ctx)
    {
        auto j = static_cast<job*>(ctx);
        return j->owner->abort_ || j->stop != stop_reason::none ? 1 : 0;
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
                running_.push_back(j);
                j->started = true;
            }

            auto preempted = !prefetch(*j);

            std::lock_guard<std::mutex> lock(mutex_);
            running_.erase(std::find(running_.begin(), running_.end(), j));

            // Back in line with its old seq, first within its priority. A cancel may have come since.
            if (preempted && j->stop == stop_reason::preempt) {
                j->stop = stop_reason::none;
                queue_.push_back(j);
                continue;
            }

            // A cancelled job's key may already belong to a new job.
            auto it = pending_.find(j->key);
            if (it != pending_.end() && it->second == j) {
                pending_.erase(it);
            }
        }
    }

    // Returns false if it was preempted.
    bool prefetch(job& j)
    {
        caspar::timer timer;
        auto&         progress = j.progress;
        const auto&   name     = j.key;

        // A preempted job starts over, reading what it already fetched from the cache.
        progress.bytes = 0;
        progress.size  = -1;

        try {
            CASPAR_LOG(info) << "ffmpeg[" << name << "] Prefetch started";

            create_cache_directories();
            record(j, prefetch_state::running);
            j.strategy->fill(AVIOInterruptCB{interrupt_cb, &j}, progress);

            if (progress.size > progress.bytes) {
                CASPAR_LOG(info) << "ffmpeg[" << name << "] Prefetch done (" << progress.bytes << " of "
                                 << progress.size << " bytes in " << timer.elapsed() << "s)";
            } else {
                CASPAR_LOG(info) << "ffmpeg[" << name << "] Prefetch done (" << progress.bytes << " bytes in "
                                 << timer.elapsed() << "s)";
            }
            record(j, prefetch_state::done);
        } catch (...) {
            if (abort_) {
                CASPAR_LOG(info) << "ffmpeg[" << name << "] Prefetch aborted";
                record(j, prefetch_state::aborted);
            } else if (j.stop == stop_reason::cancel) {
                CASPAR_LOG(info) << "ffmpeg[" << name << "] Prefetch cancelled after " << progress.bytes << " bytes";
                record(j, prefetch_state::cancelled);
            } else if (j.stop == stop_reason::preempt) {
                CASPAR_LOG(info) << "ffmpeg[" << name << "] Prefetch paused after " << progress.bytes
                                 << " bytes, queued again";
                record(j, prefetch_state::paused);
                return false;
            } else {
                log_failure(name, progress);
                record(j, prefetch_state::failed);
            }
        }
        return true;
    }

    // Called without mutex_ held.
    void record(const job& j, prefetch_state state)
    {
        prefetch_record r{
            state, j.strategy->range(), j.progress.bytes.load(), j.progress.size.load(), 0, {}, j.strategy->url()};
        {
            std::lock_guard<std::mutex> lock(mutex_);
            r.requests = j.requests;
        }
        write_prefetch_record(std::move(r));
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
    int                                priority;
    std::string                        request; // see request_text
};

// The parameters as the client sent them, without the channel and PRIORITY, for STATUS to hand back to match on.
std::string request_text(const std::vector<std::wstring>& params)
{
    std::wstring text;
    for (size_t i = 0; i < params.size(); ++i) {
        if (i > 0 && boost::iequals(params[i], L"PRIORITY")) {
            ++i; // and its value
            continue;
        }
        text += (text.empty() ? L"" : L" ") + params[i];
    }
    return u8(text);
}

// The url a PREFETCH command's url parameter names ("shared:" stripped), or nothing if it can't be prefetched: local
// files have nothing to cache, and only real protocols go through the shared protocol. Logs why.
std::optional<std::wstring> prefetch_url(const std::wstring& command, const std::wstring& param)
{
    auto resolved = resolve_path(param);
    if (!resolved || !boost::contains(resolved->path, L"://") ||
        avio_find_protocol_name(u8(resolved->path).c_str()) == nullptr) {
        CASPAR_LOG(error) << L"ffmpeg[" << param << L"] " << command << L": not a supported url";
        return {};
    }
    return resolved->path;
}

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

    auto path = prefetch_url(command, params.at(0));
    if (!path) {
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
        CASPAR_LOG(error) << L"ffmpeg[" << *path << L"] " << command << L": "
                          << (msg ? u16(*msg) : L"invalid SEEK, IN, LENGTH, OUT or PRIORITY");
        return {};
    }

    auto url = u8(*path);
    return prefetch_request{
        range ? create_range_prefetch(url, *range) : create_whole_file_prefetch(url), priority, request_text(params)};
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
    switch (instance->enqueue(request->strategy, request->request, request->priority)) {
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

std::wstring prefetch_status_command(protocol::amcp::command_context& ctx)
{
    auto params = ctx.parameters;
    auto cached = !params.empty() && boost::iequals(params.at(0), L"CACHED");
    if (cached) {
        params.erase(params.begin());
    }

    std::optional<std::string> url;
    if (!params.empty()) {
        // As PREFETCH takes it, so that "shared:" doesn't matter.
        auto path = prefetch_url(L"PREFETCH STATUS", params.at(0));
        if (!path) {
            return L"403 PREFETCH STATUS FAILED\r\n";
        }
        url = u8(*path);
    }

    std::vector<prefetcher::list_entry> live;
    {
        std::lock_guard<std::mutex> lock(instance_mutex);
        if (instance) {
            live = instance->list(url);
        }
    }

    std::wstringstream reply;
    reply << L"200 PREFETCH STATUS OK\r\n";
    // A line per spelling of the request, so that each client finds the one it sent.
    auto line = [&](const wchar_t*                  state,
                    const std::wstring&             priority,
                    int64_t                         bytes,
                    int64_t                         size,
                    const std::vector<std::string>& requests) {
        for (const auto& request : requests) {
            reply << state << L" " << priority << L" " << bytes << L" " << size << L" " << u16(request) << L"\r\n";
        }
    };
    for (const auto& e : live) {
        line(e.state, std::to_wstring(e.priority), e.bytes, e.size, e.requests);
    }

    if (cached) {
        auto records = read_cached_prefetches(url);
        std::sort(records.begin(), records.end(), [](const auto& a, const auto& b) {
            return a.url != b.url ? a.url < b.url
                                  : describe_prefetch(a.url, a.range) < describe_prefetch(b.url, b.range);
        });

        std::map<std::string, const prefetch_record*> whole_done;
        for (const auto& r : records) {
            if (!r.range && r.state == prefetch_state::done) {
                whole_done[r.url] = &r;
            }
        }

        for (const auto& r : records) {
            const auto key = describe_prefetch(r.url, r.range);
            // DONE means available: any range of a url whose whole file is cached is too, whatever its own
            // prefetch did. It then shows the whole file's bytes.
            auto whole = r.range && r.state != prefetch_state::done ? whole_done.find(r.url) : whole_done.end();
            if (r.state == prefetch_state::done || whole != whole_done.end()) {
                const auto& done = whole != whole_done.end() ? *whole->second : r;
                line(L"DONE", L"-", done.bytes, done.size, r.requests);
                continue;
            }
            // Otherwise a live job is the current state of its request.
            if (std::any_of(live.begin(), live.end(), [&](const prefetcher::list_entry& e) { return e.key == key; })) {
                continue;
            }

            const wchar_t* state = L"";
            switch (r.state) {
                case prefetch_state::done:
                    break;
                case prefetch_state::running:
                    state = L"INTERRUPTED"; // no live job: Caspar stopped without recording its end
                    break;
                case prefetch_state::failed:
                    state = L"FAILED";
                    break;
                case prefetch_state::cancelled:
                    state = L"CANCELLED";
                    break;
                case prefetch_state::paused:
                    state = L"PAUSED";
                    break;
                case prefetch_state::aborted:
                    state = L"ABORTED";
                    break;
            }
            line(state, L"-", r.bytes, r.size, r.requests);
        }
    }

    reply << L"\r\n";
    return reply.str();
}

std::wstring prefetch_cancel_command(protocol::amcp::command_context& ctx)
{
    auto request = parse_request(L"PREFETCH CANCEL", ctx);
    if (!request) {
        return L"403 PREFETCH CANCEL FAILED\r\n";
    }

    const auto key = request->strategy->describe();

    std::lock_guard<std::mutex> lock(instance_mutex);
    if (!instance || !instance->cancel(key)) {
        CASPAR_LOG(info) << L"ffmpeg[" << u16(key) << L"] No prefetch to cancel";
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
