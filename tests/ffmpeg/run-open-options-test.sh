#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
build=$(mktemp -d)
trap 'rm -rf "$build"' EXIT HUP INT TERM

# Discard unused Input methods so this options test needs no server, graphics or media fixtures.
"${CXX:-c++}" -std=c++20 -O1 -DBOOST_ALL_DYN_LINK -Wno-terminate \
    -ffunction-sections -fdata-sections -I"$repo/src" \
    $(pkg-config --cflags libavformat libavutil tbb) \
    "$repo/tests/ffmpeg/open_options_test.cpp" \
    "$repo/src/modules/ffmpeg/producer/av_input.cpp" "$repo/src/common/utf.cpp" "$repo/src/common/log.cpp" \
    -Wl,--gc-sections $(pkg-config --libs libavformat libavutil tbb) \
    -lboost_regex -lboost_log -lboost_log_setup -lboost_thread -lboost_filesystem -lboost_locale \
    -pthread -o "$build/open-options-test"
"$build/open-options-test"
