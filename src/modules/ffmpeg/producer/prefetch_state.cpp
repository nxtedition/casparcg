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

extern "C" {
#include <libavutil/base64.h>
}

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace caspar { namespace ffmpeg {

namespace {

// First line of a .prefetch file. Each further line is "state start seek end bytes size updated requests url",
// times in AV_TIME_BASE units or "-" (start, seek and end for the whole file, end for "to the end"), requests
// the spellings of the request, each base64 encoded since they contain spaces, comma separated.
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

std::string requests(const prefetch_record& record)
{
    std::string str;
    for (const auto& request : record.requests) {
        std::vector<char> out(AV_BASE64_SIZE(request.size()));
        av_base64_encode(out.data(),
                         static_cast<int>(out.size()),
                         reinterpret_cast<const uint8_t*>(request.data()),
                         static_cast<int>(request.size()));
        str += (str.empty() ? "" : ",") + std::string(out.data());
    }
    return str.empty() ? "-" : str;
}

std::optional<std::string> decode(const std::string& base64)
{
    std::vector<uint8_t> out(base64.size() * 3 / 4 + 3);
    auto                 size = av_base64_decode(out.data(), base64.c_str(), static_cast<int>(out.size()));
    if (size < 0) {
        return {};
    }
    return std::string(out.begin(), out.begin() + size);
}

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
    std::string        state, start, seek, end, requests;
    prefetch_record    record{};
    if (!(str >> state >> start >> seek >> end >> record.bytes >> record.size >> record.updated >> requests)) {
        return {};
    }
    std::istringstream list(requests);
    for (std::string request; std::getline(list, request, ',');) {
        if (request.empty() || request == "-") {
            continue;
        }
        if (auto text = decode(request)) {
            record.requests.push_back(std::move(*text));
        }
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

std::vector<prefetch_record> read_cached_prefetches(const std::optional<std::string>& url)
{
    const auto                   locations = get_cache_locations();
    std::vector<prefetch_record> result;

    boost::system::error_code ec;
    if (!boost::filesystem::is_directory(locations.metadata_dir, ec)) {
        return result; // nothing prefetched yet
    }
    std::optional<state_lock> lock;
    try {
        lock.emplace(locations.metadata_dir, false);
    } catch (std::exception& e) {
        CASPAR_LOG(warning) << L"PREFETCH STATUS: can't lock " << locations.metadata_dir.wstring() << L": "
                            << u16(e.what());
        return result;
    }

    auto add = [&](const boost::filesystem::path& path) {
        auto records = read_prefetch_records(path);
        auto data    = locations.cache_dir / path.stem();
        data += ".cache";
        // Done isn't true anymore once the cache was deleted by hand.
        boost::system::error_code exists_ec;
        auto                      deleted = !boost::filesystem::exists(data, exists_ec);
        for (auto& r : records) {
            if (!deleted || r.state != prefetch_state::done) {
                result.push_back(std::move(r));
            }
        }
    };

    if (url) {
        add(state_path(locations, *url));
    } else {
        for (boost::filesystem::directory_iterator it(locations.metadata_dir, ec), end; !ec && it != end;
             it.increment(ec)) {
            if (it->path().extension() == ".prefetch") {
                add(it->path());
            }
        }
    }
    return result;
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
        } else {
            // A new spelling is kept even when the state isn't replaced: a done range is done for it too.
            auto requests = it->requests;
            for (const auto& request : record.requests) {
                if (std::find(requests.begin(), requests.end(), request) == requests.end()) {
                    requests.push_back(request);
                }
            }
            if (it->state != prefetch_state::done || record.state == prefetch_state::done) {
                *it = record;
            } else if (requests == it->requests) {
                return;
            }
            it->requests = std::move(requests);
        }

        std::ostringstream str;
        str << HEADER << "\n";
        for (const auto& r : records) {
            const auto range = r.range.value_or(prefetch_range{});
            str << STATE_NAMES[static_cast<int>(r.state)] << " " << (r.range ? std::to_string(range.start) : "-") << " "
                << (r.range ? std::to_string(range.seek) : "-") << " " << field(range.end) << " " << r.bytes << " "
                << r.size << " " << r.updated << " " << requests(r) << " " << r.url << "\n";
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
