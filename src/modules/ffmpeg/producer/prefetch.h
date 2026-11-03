#pragma once

#include <protocol/amcp/amcp_command_context.h>

#include <string>

namespace caspar { namespace ffmpeg {

// PREFETCH [channel[-layer]] url [SEEK n] [IN n] [LENGTH n] [OUT n] [PRIORITY n]: reads the resource through
// the shared protocol in the background, filling the cache so that it can be played even if the source
// becomes unreachable. With a range, only what PLAY with the same range reads. Values in frames need the
// channel, times (hh:mm:ss.frac) don't. Higher PRIORITY runs first (default 0); repeating a queued PREFETCH
// with another priority moves it. With every worker busy, a higher priority preempts a running prefetch,
// which resumes later and reads what it already fetched from the cache.
std::wstring prefetch_command(protocol::amcp::command_context& ctx);

// PREFETCH CANCEL [channel[-layer]] url [SEEK n] [IN n] [LENGTH n] [OUT n]: cancels the queued or running
// prefetch with that range, or without a range all prefetches of url. What's cached stays cached.
std::wstring prefetch_cancel_command(protocol::amcp::command_context& ctx);

void prefetch_uninit();

}} // namespace caspar::ffmpeg
