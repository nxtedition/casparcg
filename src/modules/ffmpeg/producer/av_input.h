#pragma once

#include <common/diagnostics/graph.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <vector>

#include <tbb/concurrent_queue.h>

#include <boost/optional.hpp>
#include <boost/thread.hpp>

struct AVPacket;
struct AVFormatContext;
struct AVDictionary;
struct AVInputFormat;
struct AVIOInterruptCB;

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

// Opens filename the way the producer does, including avformat_find_stream_info.
std::shared_ptr<AVFormatContext> open_input(const std::string&     filename,
                                            cache_mode             cache,
                                            std::optional<bool>    seekable,
                                            const AVIOInterruptCB& interrupt);

// How many packets Input reads ahead of the decoders.
constexpr int input_buffer_capacity = 256;

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

    // With a time (relative to the start of the input), it is seeked there before the read thread
    // reads anything.
    void reset(std::optional<int64_t> time = {});
    void abort();
    bool eof() const;
    void seek(int64_t ts, bool flush = true);

  private:
    std::shared_ptr<AVFormatContext> open();
    void                             internal_reset();
    AVFormatContext*                 reader() const;
    void                             deliver(std::shared_ptr<AVPacket> packet);
    void                             push(std::shared_ptr<AVPacket> packet);
    bool                             recover(int error);

    std::optional<bool> seekable_;
    bool                cache_;

    const std::string                   filename_;
    std::shared_ptr<diagnostics::graph> graph_;

    mutable std::mutex               ic_mutex_;
    std::shared_ptr<AVFormatContext> ic_;
    std::condition_variable          ic_cond_;

    // Read-error recovery for cached inputs, see recover(). The reopened context is only read from;
    // ic_ stays the one decoders and filters were built from, since they keep pointers into it.
    std::shared_ptr<AVFormatContext> recovered_;
    int                              seek_generation_ = 0;
    int64_t                          seek_ts_;
    std::chrono::seconds             retry_timeout_;

    // Read thread only.
    int                                                  read_generation_ = 0;
    std::vector<int64_t>                                 delivered_dts_;
    std::vector<int64_t>                                 resume_dts_;
    std::shared_ptr<AVPacket>                            held_;
    std::optional<std::chrono::steady_clock::time_point> failing_since_;
    int                                                  retries_ = 0;

    tbb::concurrent_bounded_queue<std::shared_ptr<AVPacket>> buffer_;

    std::atomic<bool> eof_{false};

    std::atomic<bool> abort_request_{false};
    boost::thread     thread_;
};

}} // namespace caspar::ffmpeg
