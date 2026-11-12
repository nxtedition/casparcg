#include "prefetch_state.h"

#include "av_input.h"

#include <common/log.h>
#include <common/scope_exit.h>
#include <common/utf.h>

#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/interprocess/sync/file_lock.hpp>

#include <algorithm>
#include <cstdio>
#include <ctime>
#include <exception>
#include <mutex>
#include <sstream>
#include <stdexcept>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace caspar { namespace ffmpeg {

namespace {

// First line of a .prefetch file. Each further line is "state start seek end bytes size updated url", times in
// AV_TIME_BASE units or "-" (start, seek and end for the whole file, end for "to the end").
const std::string HEADER = "caspar-prefetch 1";

const char* const STATE_NAMES[] = {"running", "done", "failed", "cancelled", "paused", "aborted"};

std::mutex state_mutex;

// Held while reading or writing the .prefetch files of a metadata_dir. The mutex covers threads, the lock file
// processes sharing the cache; fcntl locks belong to the process, so a thread unlocking it would release another
// thread's lock without the mutex. The lock is on a file of its own, since .prefetch files are replaced by a
// rename. Readers take it too: Windows can't replace a file that another handle has open.
class state_lock
{
    std::lock_guard<std::mutex>    thread_lock_{state_mutex};
    boost::interprocess::file_lock file_lock_;
    bool                           exclusive_;

    static boost::interprocess::file_lock open(const boost::filesystem::path& dir)
    {
        const auto path = dir / ".prefetch.lock";
        boost::filesystem::ofstream(path, std::ios::app).close(); // file_lock needs the file to exist
        return boost::interprocess::file_lock(path.string().c_str());
    }

  public:
    state_lock(const boost::filesystem::path& dir, bool exclusive)
        : file_lock_(open(dir))
        , exclusive_(exclusive)
    {
        exclusive_ ? file_lock_.lock() : file_lock_.lock_sharable();
    }

    ~state_lock() { exclusive_ ? file_lock_.unlock() : file_lock_.unlock_sharable(); }

    state_lock(const state_lock&)            = delete;
    state_lock& operator=(const state_lock&) = delete;
};

// On disk before it's renamed into place, so that a power cut can't leave an empty .prefetch file behind.
void write_synced(const boost::filesystem::path& path, const std::string& data)
{
#ifdef _WIN32
    FILE* file = _wfopen(path.c_str(), L"wb");
#else
    FILE* file = std::fopen(path.c_str(), "wb");
#endif
    if (!file) {
        throw std::runtime_error("can't open " + path.string());
    }
    CASPAR_SCOPE_EXIT
    {
        if (file) {
            std::fclose(file);
        }
    };
#ifdef _WIN32
    auto sync = [&] { return _commit(_fileno(file)) == 0; };
#else
    auto sync = [&] { return fsync(fileno(file)) == 0; };
#endif
    auto ok = std::fwrite(data.data(), 1, data.size(), file) == data.size() && std::fflush(file) == 0 && sync();
    ok      = std::fclose(file) == 0 && ok;
    file    = nullptr;
    if (!ok) {
        throw std::runtime_error("can't write " + path.string());
    }
}

std::string field(const std::optional<int64_t>& value) { return value ? std::to_string(*value) : "-"; }

std::optional<int64_t> parse_field(const std::string& str)
{
    if (str == "-") {
        return {};
    }
    return std::stoll(str);
}

bool same_request(const std::optional<prefetch_range>& a, const std::optional<prefetch_range>& b)
{
    if (!a || !b) {
        return !a && !b;
    }
    return a->start == b->start && a->seek == b->seek && a->end == b->end;
}

std::optional<prefetch_record> parse_line(const std::string& line)
{
    std::istringstream str(line);
    std::string        state, start, seek, end;
    prefetch_record    record{};
    if (!(str >> state >> start >> seek >> end >> record.bytes >> record.size >> record.updated)) {
        return {};
    }
    std::getline(str >> std::ws, record.url);

    auto name = std::find(std::begin(STATE_NAMES), std::end(STATE_NAMES), state);
    if (name == std::end(STATE_NAMES) || record.url.empty()) {
        return {};
    }
    record.state = static_cast<prefetch_state>(name - std::begin(STATE_NAMES));
    if (start != "-") {
        record.range = prefetch_range{std::stoll(start), std::stoll(seek), parse_field(end)};
    }
    return record;
}

boost::filesystem::path state_path(const cache_locations& locations, const std::string& url)
{
    return locations.metadata_dir / (cache_key(url) + ".prefetch");
}

} // namespace

std::vector<prefetch_record> read_prefetch_records(const boost::filesystem::path& path)
{
    std::vector<prefetch_record> records;
    boost::filesystem::ifstream  file(path);
    std::string                  line;
    if (!std::getline(file, line) || line != HEADER) {
        return records;
    }
    while (std::getline(file, line)) {
        try {
            if (auto record = parse_line(line)) {
                records.push_back(std::move(*record));
            }
        } catch (std::exception&) {
            // A damaged line (stoll) is dropped, and the next write leaves it out.
        }
    }
    return records;
}

void write_prefetch_record(prefetch_record record)
{
    try {
        record.updated       = static_cast<int64_t>(std::time(nullptr));
        const auto locations = get_cache_locations();
        const auto path      = state_path(locations, record.url);

        state_lock lock(locations.metadata_dir, true);

        auto records = read_prefetch_records(path);
        auto it      = std::find_if(records.begin(), records.end(), [&](const prefetch_record& r) {
            return same_request(r.range, record.range);
        });
        if (it == records.end()) {
            records.push_back(record);
        } else if (it->state != prefetch_state::done || record.state == prefetch_state::done) {
            *it = record;
        } else {
            return;
        }

        std::ostringstream str;
        str << HEADER << "\n";
        for (const auto& r : records) {
            const auto range = r.range.value_or(prefetch_range{});
            str << STATE_NAMES[static_cast<int>(r.state)] << " " << (r.range ? std::to_string(range.start) : "-") << " "
                << (r.range ? std::to_string(range.seek) : "-") << " " << field(range.end) << " " << r.bytes << " "
                << r.size << " " << r.updated << " " << r.url << "\n";
        }

        auto tmp = path;
        tmp += ".tmp";
        write_synced(tmp, str.str());
        boost::filesystem::rename(tmp, path);
    } catch (std::exception& e) {
        CASPAR_LOG(warning) << "ffmpeg[" << describe_prefetch(record.url, record.range)
                            << "] Prefetch state not saved: " << e.what();
    }
}

}} // namespace caspar::ffmpeg
