#pragma once

#include "prefetch_strategy.h"

#include <boost/filesystem/path.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace caspar { namespace ffmpeg {

// What became of a prefetch request, kept per url in "<metadata_dir>/<cache_key>.prefetch" so that it
// outlives the queue: a line per request (the whole file, or a range), with the last outcome of each.
enum class prefetch_state
{
    running,
    done,
    failed,
    cancelled,
    paused, // preempted, queued again
    aborted,
};

struct prefetch_record
{
    prefetch_state                state;
    std::optional<prefetch_range> range; // none: the whole file
    int64_t                       bytes   = 0;
    int64_t                       size    = -1;
    int64_t                       updated = 0; // unix seconds
    std::string                   url;
};

// Replaces the line for record's request in its url's file. A done line is only replaced by another done:
// the data stays cached whatever a later attempt does. Logs instead of throwing.
void write_prefetch_record(prefetch_record record);

// The lines of a .prefetch file; none if it's missing or not one.
std::vector<prefetch_record> read_prefetch_records(const boost::filesystem::path& path);

// The lines of url's file, or without a url of every .prefetch file in metadata_dir. Leaves out done lines
// whose .cache is gone.
std::vector<prefetch_record> read_cached_prefetches(const std::optional<std::string>& url);

}} // namespace caspar::ffmpeg
