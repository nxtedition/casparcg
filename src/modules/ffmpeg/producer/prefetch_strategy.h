#pragma once

#include "ffmpeg_producer.h"

#include <common/memory.h>

#include <atomic>
#include <cstdint>
#include <optional>
#include <string>

struct AVIOInterruptCB;

namespace caspar { namespace ffmpeg {

// Atomic, since PREFETCH STATUS reads it while a worker writes it.
struct prefetch_progress
{
    std::atomic<int64_t> bytes{0};
    std::atomic<int64_t> size{-1}; // of the whole resource, if known
};

// The part of a url a range prefetch reads, in AV_TIME_BASE units relative to the clip's start_time.
struct prefetch_range
{
    int64_t                start = 0;
    int64_t                seek  = 0;
    std::optional<int64_t> end; // none: to the end
};

// The url and the part of it to prefetch (none: the whole file), e.g. "url 20.000s-30.000s". Identical
// requests are only run once.
std::string describe_prefetch(const std::string& url, const std::optional<prefetch_range>& range);

class prefetch_strategy
{
  protected:
    prefetch_strategy() = default;

  public:
    prefetch_strategy& operator=(const prefetch_strategy&) = delete;
    virtual ~prefetch_strategy()                           = default;

    prefetch_strategy(const prefetch_strategy&) = delete;

    virtual const std::string& url() const = 0;

    virtual std::optional<prefetch_range> range() const = 0;

    std::string describe() const { return describe_prefetch(url(), range()); }

    // Reads through the shared protocol, which is what fills the cache. Throws on failure.
    virtual void fill(const AVIOInterruptCB& interrupt, prefetch_progress& progress) = 0;
};

// Throws once bytes passes the cache size limit (0 = none), since shared stops caching there and the
// rest would be downloaded for nothing.
void check_size_limit(int64_t bytes, int64_t limit);

spl::shared_ptr<prefetch_strategy> create_whole_file_prefetch(std::string url);

// The part of url that PLAY with the same range reads.
spl::shared_ptr<prefetch_strategy> create_range_prefetch(std::string url, const play_range& range);

}} // namespace caspar::ffmpeg
