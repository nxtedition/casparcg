#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace caspar { namespace ffmpeg {

// A SEEK, IN, OUT or LENGTH value given as a time, [[hours:]minutes:]seconds[.frac] with an optional leading
// '-', in AV_TIME_BASE units. Empty if the value has neither ':' nor '.', i.e. is in frames. Throws
// std::invalid_argument if it is not a valid time.
std::optional<int64_t> parse_time(const std::wstring& value);

}} // namespace caspar::ffmpeg
