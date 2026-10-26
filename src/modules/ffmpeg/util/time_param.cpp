#include "time_param.h"

#include <limits>
#include <stdexcept>
#include <string_view>

extern "C" {
#include <libavutil/avutil.h>
}

namespace caspar { namespace ffmpeg {

namespace {

int64_t mul_add(int64_t value, int64_t mul, int64_t add)
{
    if (value > (std::numeric_limits<int64_t>::max() - add) / mul) {
        throw std::invalid_argument("time out of range");
    }
    return value * mul + add;
}

bool is_digits(std::wstring_view str) { return str.find_first_not_of(L"0123456789") == std::wstring_view::npos; }

int64_t number(std::wstring_view digits)
{
    int64_t value = 0;
    for (auto c : digits) {
        value = mul_add(value, 10, c - L'0');
    }
    return value;
}

} // namespace

std::optional<int64_t> parse_time(const std::wstring& value)
{
    if (value.find_first_of(L":.") == std::wstring::npos) {
        return {};
    }

    std::wstring_view str = value;

    const auto negative = !str.empty() && str.front() == L'-';
    if (negative) {
        str.remove_prefix(1);
    }

    std::wstring_view frac;
    if (auto dot = str.find(L'.'); dot != std::wstring_view::npos) {
        frac = str.substr(dot + 1);
        str  = str.substr(0, dot);
    }

    if (!is_digits(frac) || (frac.empty() && str.find_first_of(L"0123456789") == std::wstring_view::npos)) {
        throw std::invalid_argument("not a time");
    }

    int64_t seconds = 0;
    int     parts   = 0;
    while (true) {
        const auto colon = str.find(L':');
        const auto part  = str.substr(0, colon);
        if (!is_digits(part) || ++parts > 3) {
            throw std::invalid_argument("not a time");
        }
        seconds = mul_add(seconds, 60, number(part));
        if (colon == std::wstring_view::npos) {
            break;
        }
        str.remove_prefix(colon + 1);
    }

    // Rounded to AV_TIME_BASE from one more digit, the rest is ignored.
    auto digits = std::wstring(frac.substr(0, 7));
    digits.resize(7, L'0');
    const auto time = mul_add(seconds, AV_TIME_BASE, (number(digits) + 5) / 10);

    return negative ? -time : time;
}

}} // namespace caspar::ffmpeg
