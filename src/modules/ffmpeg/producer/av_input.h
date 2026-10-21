#pragma once

#include <common/diagnostics/graph.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>

#include <tbb/concurrent_queue.h>

#include <boost/optional.hpp>
#include <boost/thread.hpp>

struct AVPacket;
struct AVFormatContext;
struct AVDictionary;
struct AVInputFormat;

namespace caspar { namespace ffmpeg {

enum class cache_mode
{
    none,
    playback,
    prefetch,
};

// Fills options with the defaults and configured options for opening filename, and returns the url
// to open. With caching, the url is wrapped in the shared protocol.
std::string build_open_options(const std::string&    filename,
                               cache_mode            cache,
                               AVDictionary**        options,
                               const AVInputFormat** input_format);

// The shared protocol's cache_size_max from options.shared, which PREFETCH respects as well; 0 = none.
int64_t prefetch_size_limit();

std::wstring cache_path();

class Input
{
  public:
    Input(const std::string&                  filename,
          std::shared_ptr<diagnostics::graph> graph,
          std::optional<bool>                 seekable,
          bool                                cache);
    ~Input();

    static int interrupt_cb(void* ctx);

    bool try_pop(std::shared_ptr<AVPacket>& packet);

    AVFormatContext* operator->();

    AVFormatContext* const operator->() const;

    void reset();
    void abort();
    bool eof() const;
    void seek(int64_t ts, bool flush = true);

  private:
    void internal_reset();

    std::optional<bool> seekable_;
    bool                cache_;

    const std::string                   filename_;
    std::shared_ptr<diagnostics::graph> graph_;

    mutable std::mutex               ic_mutex_;
    std::shared_ptr<AVFormatContext> ic_;
    std::condition_variable          ic_cond_;

    tbb::concurrent_bounded_queue<std::shared_ptr<AVPacket>> buffer_;

    std::atomic<bool> eof_{false};

    std::atomic<bool> abort_request_{false};
    boost::thread     thread_;
};

}} // namespace caspar::ffmpeg
