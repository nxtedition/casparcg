#pragma once

#include <common/memory.h>

#include <cstdint>
#include <string>

struct AVIOInterruptCB;

namespace caspar { namespace ffmpeg {

struct prefetch_progress
{
    int64_t bytes = 0;
    int64_t size  = -1; // of the whole resource, if known
};

class prefetch_strategy
{
  protected:
    prefetch_strategy() = default;

  public:
    prefetch_strategy& operator=(const prefetch_strategy&) = delete;
    virtual ~prefetch_strategy()                           = default;

    prefetch_strategy(const prefetch_strategy&) = delete;

    // The url and the part of it to prefetch. Identical requests are only run once.
    virtual std::string describe() const = 0;

    // Reads through the shared protocol, which is what fills the cache. Throws on failure.
    virtual void fill(const AVIOInterruptCB& interrupt, prefetch_progress& progress) = 0;
};

// Throws once bytes passes the cache size limit (0 = none), since shared stops caching there and the
// rest would be downloaded for nothing.
void check_size_limit(int64_t bytes, int64_t limit);

spl::shared_ptr<prefetch_strategy> create_whole_file_prefetch(std::string url);

}} // namespace caspar::ffmpeg
