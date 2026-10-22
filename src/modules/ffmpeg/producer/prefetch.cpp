#include "prefetch.h"

#include "av_input.h"
#include "ffmpeg_producer.h"

#include "../util/av_assert.h"
#include "../util/av_util.h"

#include <common/env.h>
#include <common/log.h>
#include <common/os/thread.h>
#include <common/scope_exit.h>
#include <common/timer.h>
#include <common/utf.h>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/filesystem.hpp>
#include <boost/property_tree/ptree.hpp>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
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
    std::deque<std::string>  queue_;
    std::set<std::string>    pending_; // queued or running
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

    // Returns false if the url is already queued or running.
    bool enqueue(const std::string& filename)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!pending_.insert(filename).second) {
            return false;
        }
        queue_.push_back(filename);

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
            std::string filename;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cond_.wait(lock, [&] { return abort_ || !queue_.empty(); });
                if (abort_) {
                    return;
                }
                filename = std::move(queue_.front());
                queue_.pop_front();
            }

            prefetch(filename);

            std::lock_guard<std::mutex> lock(mutex_);
            pending_.erase(filename);
        }
    }

    void prefetch(const std::string& filename)
    {
        caspar::timer timer;
        int64_t       total = 0;

        try {
            CASPAR_LOG(info) << "ffmpeg[" << filename << "] Prefetch started";

            boost::filesystem::create_directories(cache_path());

            AVDictionary* options = nullptr;
            CASPAR_SCOPE_EXIT { av_dict_free(&options); };

            const AVInputFormat* input_format = nullptr;
            auto                 url = build_open_options(filename, cache_mode::prefetch, &options, &input_format);

            AVIOInterruptCB cb{interrupt_cb, this};
            AVIOContext*    io = nullptr;
            FF(avio_open2(&io, url.c_str(), AVIO_FLAG_READ, &cb, &options));
            CASPAR_SCOPE_EXIT { avio_closep(&io); };

            for (auto& p : to_map(&options)) {
                CASPAR_LOG(warning) << "ffmpeg[" << filename << "] Prefetch unused option " << p.first << "="
                                    << p.second;
            }

            // Past cache_size_max shared stops caching and would only download the rest for nothing.
            auto limit     = prefetch_size_limit();
            auto too_large = [&](int64_t bytes) {
                if (limit > 0 && bytes > limit) {
                    CASPAR_LOG(error) << "ffmpeg[" << filename << "] Prefetch failed: larger than cache_size_max ("
                                      << limit << " bytes)";
                    return true;
                }
                return false;
            };

            auto size = avio_size(io);
            if (too_large(size)) {
                return;
            }

            // Reading through the shared protocol is what fills the cache, so the data itself is dropped.
            std::vector<unsigned char> buf(1 << 20);
            while (true) {
                auto ret = avio_read(io, buf.data(), static_cast<int>(buf.size()));
                if (ret == AVERROR_EOF || ret == 0) {
                    break;
                }
                FF_RET(ret, "avio_read");
                total += ret;
                if (too_large(total)) {
                    return;
                }
            }

            if (size > 0 && total != size) {
                CASPAR_LOG(error) << "ffmpeg[" << filename << "] Prefetch failed: read " << total << " of " << size
                                  << " bytes";
                return;
            }

            CASPAR_LOG(info) << "ffmpeg[" << filename << "] Prefetch done (" << total << " bytes in " << timer.elapsed()
                             << "s)";
        } catch (ffmpeg_error_t& e) {
            auto err = boost::get_error_info<ffmpeg_errn_info>(e);
            if (err && *err == AVERROR_EXIT) {
                CASPAR_LOG(info) << "ffmpeg[" << filename << "] Prefetch aborted";
            } else {
                CASPAR_LOG(error) << "ffmpeg[" << filename << "] Prefetch failed after " << total
                                  << " bytes: " << (err ? error_string(*err) : std::string("unknown error"));
            }
        } catch (...) {
            CASPAR_LOG_CURRENT_EXCEPTION();
            CASPAR_LOG(error) << "ffmpeg[" << filename << "] Prefetch failed";
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
    if (!instance->enqueue(u8(resolved->path))) {
        CASPAR_LOG(info) << L"ffmpeg[" << resolved->path << L"] Prefetch already in progress";
    }

    return L"202 PREFETCH OK\r\n";
}

void prefetch_uninit()
{
    std::lock_guard<std::mutex> lock(instance_mutex);
    instance.reset();
}

}} // namespace caspar::ffmpeg
