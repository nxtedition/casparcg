#pragma once

#include <protocol/amcp/amcp_command_context.h>

#include <string>

namespace caspar { namespace ffmpeg {

// PREFETCH [channel[-layer]] url [SEEK n] [IN n] [LENGTH n] [OUT n]: reads the resource through the
// shared protocol in the background, filling the cache so that it can be played even if the source
// becomes unreachable. With a range, only what PLAY with the same range reads. Values in frames need the
// channel, times (hh:mm:ss.frac) don't.
std::wstring prefetch_command(protocol::amcp::command_context& ctx);

void prefetch_uninit();

}} // namespace caspar::ffmpeg
