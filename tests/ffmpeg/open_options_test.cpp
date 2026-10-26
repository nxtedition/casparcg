// Run with tests/ffmpeg/run-open-options-test.sh on Linux.
#include <modules/ffmpeg/producer/av_input.h>

#include <boost/property_tree/ptree.hpp>

#include <iostream>
#include <stdexcept>
#include <string>

extern "C" {
#include <libavutil/dict.h>
}

namespace {

boost::property_tree::wptree config;

void expect(const AVDictionary* options, const char* name, const char* expected)
{
    auto entry = av_dict_get(options, name, nullptr, 0);
    if (expected ? !entry || std::string(entry->value) != expected : entry != nullptr) {
        throw std::runtime_error(std::string(name) + ": expected " + (expected ? expected : "<unset>") + ", got " +
                                 (entry ? entry->value : "<unset>"));
    }
}

void check(caspar::ffmpeg::cache_mode mode)
{
    AVDictionary*        options = nullptr;
    const AVInputFormat* format  = nullptr;
    try {
        auto url = caspar::ffmpeg::build_open_options("https://example.com/clip.mov", mode, &options, &format);
        if (url != (mode == caspar::ffmpeg::cache_mode::none ? "https://example.com/clip.mov"
                                                             : "shared:https://example.com/clip.mov")) {
            throw std::runtime_error("unexpected input URL");
        }
        expect(options, "user_agent", "configured-agent");
        expect(options, "rw_timeout", "123456");
        if (mode == caspar::ffmpeg::cache_mode::none) {
            expect(options, "metadata_dir", "default-metadata");
            expect(options, "cache_dir", nullptr);
        } else {
            expect(options, "metadata_dir", "shared-metadata");
            expect(options, "cache_dir", "shared-cache");
            expect(options, "cache_size_max", "1073741824");
            auto prefetch = mode == caspar::ffmpeg::cache_mode::prefetch;
            expect(options, "ignore_errors", prefetch ? "0" : "1");
            expect(options, "cache_read_only", prefetch ? "0" : "1");
            expect(options, "retry_errors", prefetch ? "1" : "0");
            expect(options, "retry_corrupt", prefetch ? "1" : "0");
            expect(options, "cache_timeout", prefetch ? "30000000" : "7");
        }
    } catch (...) {
        av_dict_free(&options);
        throw;
    }
    av_dict_free(&options);
}

} // namespace

namespace caspar::env {

// Supply configuration without starting the server or opening media.
const boost::property_tree::wptree& properties() { return config; }

} // namespace caspar::env

int main()
{
    config.put(L"configuration.ffmpeg.producer.options.default", L"-metadata_dir default-metadata -rw_timeout 123456");
    config.put(L"configuration.ffmpeg.producer.options.http", L"-user_agent configured-agent");
    config.put(L"configuration.ffmpeg.producer.options.shared",
               L"-metadata_dir shared-metadata -cache_dir shared-cache -cache_size_max 1073741824 "
               L"-ignore_errors 1 -cache_read_only 1 -retry_errors 0 -retry_corrupt 0 -cache_timeout 7");
    try {
        check(caspar::ffmpeg::cache_mode::none);
        check(caspar::ffmpeg::cache_mode::playback);
        check(caspar::ffmpeg::cache_mode::prefetch);
        std::cout << "open options: uncached, playback and prefetch passed\n";
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
