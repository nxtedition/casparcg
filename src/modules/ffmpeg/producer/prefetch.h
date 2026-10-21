#pragma once

#include <protocol/amcp/amcp_command_context.h>

#include <string>

namespace caspar { namespace ffmpeg {

// PREFETCH [url]: reads the whole resource through the shared protocol in the background, filling
// the cache so that it can be played even if the source becomes unreachable.
std::wstring prefetch_command(protocol::amcp::command_context& ctx);

void prefetch_uninit();

}} // namespace caspar::ffmpeg
