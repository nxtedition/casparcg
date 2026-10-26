#include "prefetch.h"

#include "av_input.h"
#include "ffmpeg_producer.h"
#include "prefetch_strategy.h"

#include "../util/av_assert.h"
#include "../util/av_util.h"

#include <common/env.h>
#include <common/log.h>
#include <common/os/thread.h>
#include <common/timer.h>
#include <common/utf.h>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/filesystem.hpp>
#include <boost/property_tree/ptree.hpp>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
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
    std::mutex               mutex_;
    std::condition_variable  cond_;
    std::deque<spl::shared_ptr<prefetch_strategy>> queue_;
    std::set<std::string>                          pending_; // queued or running
    std::vector<std::thread> workers_;
    std::atomic<bool>        abort_{false};

  public:
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

    // Returns false if the same prefetch is already queued or running.
    bool enqueue(spl::shared_ptr<prefetch_strategy> strategy)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!pending_.insert(strategy->describe()).second) {
            return false;
        }
        queue_.push_back(std::move(strategy));

        // Workers are started lazily so that servers which never prefetch don't pay for them.
        auto concurrency =
            std::max(1, env::properties().get<int>(L"configuration.ffmpeg.producer.cache.prefetch-concurrency", 2));
        if (workers_.size() < static_cast<size_t>(concurrency) && workers_.size() < pending_.size()) {
            workers_.emplace_back([this] { run(); });
        }

        cond_.notify_one();
        return true;
    }

  private:
    static int interrupt_cb(void* ctx) { return static_cast<prefetcher*>(ctx)->abort_ ? 1 : 0; }

    void run()
    {
        set_thread_name(L"[ffmpeg::prefetch]");

        while (true) {
            std::shared_ptr<prefetch_strategy> strategy;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cond_.wait(lock, [&] { return abort_ || !queue_.empty(); });
                if (abort_) {
                    return;
                }
                strategy = std::move(queue_.front());
                queue_.pop_front();
            }

            prefetch(*strategy);

            std::lock_guard<std::mutex> lock(mutex_);
            pending_.erase(strategy->describe());
        }
    }

    void prefetch(prefetch_strategy& strategy)
    {
        caspar::timer     timer;
        prefetch_progress progress;
        const auto        name = strategy.describe();

        try {
            CASPAR_LOG(info) << "ffmpeg[" << name << "] Prefetch started";

            boost::filesystem::create_directories(cache_path());

            strategy.fill(AVIOInterruptCB{interrupt_cb, this}, progress);

            if (progress.size > progress.bytes) {
                CASPAR_LOG(info) << "ffmpeg[" << name << "] Prefetch done (" << progress.bytes << " of "
                                 << progress.size << " bytes in " << timer.elapsed() << "s)";
            } else {
                CASPAR_LOG(info) << "ffmpeg[" << name << "] Prefetch done (" << progress.bytes << " bytes in "
                                 << timer.elapsed() << "s)";
            }
        } catch (ffmpeg_error_t& e) {
            auto err = boost::get_error_info<ffmpeg_errn_info>(e);
            if (err && *err == AVERROR_EXIT) {
                CASPAR_LOG(info) << "ffmpeg[" << name << "] Prefetch aborted";
            } else {
                CASPAR_LOG(error) << "ffmpeg[" << name << "] Prefetch failed after " << progress.bytes
                                  << " bytes: " << (err ? error_string(*err) : std::string("unknown error"));
            }
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

} // namespace

std::wstring prefetch_command(protocol::amcp::command_context& ctx)
{
    auto resolved = resolve_path(ctx.parameters.at(0));

    // Local files have nothing to cache, and only real protocols go through the shared protocol.
    if (!resolved || !boost::contains(resolved->path, L"://") ||
        avio_find_protocol_name(u8(resolved->path).c_str()) == nullptr) {
        CASPAR_LOG(error) << L"ffmpeg[" << ctx.parameters.at(0) << L"] Cannot prefetch, not a supported url";
        return L"403 PREFETCH FAILED\r\n";
    }

    std::lock_guard<std::mutex> lock(instance_mutex);
    if (!instance) {
        instance = std::make_unique<prefetcher>();
    }
    if (!instance->enqueue(create_whole_file_prefetch(u8(resolved->path)))) {
        CASPAR_LOG(info) << L"ffmpeg[" << resolved->path << L"] Prefetch already in progress";
    }

    return L"202 PREFETCH OK\r\n";
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
