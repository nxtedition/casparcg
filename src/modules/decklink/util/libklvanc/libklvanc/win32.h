#ifndef _WIN32_H
#define _WIN32_H

#define WIN32_LEAN_AND_MEAN
#include <Windows.h>
//#include <winsock.h>
#include <stdint.h> // portable: uint64_t   MSVC: __int64

#pragma warning(disable: 4244)
#pragma warning(disable: 4018)
#pragma warning(disable: 4389)

//int gettimeofday(struct timeval * tp, struct timezone * tzp)
//{
//    // Note: some broken versions only have 8 trailing zero's, the correct epoch has 9 trailing zero's
//    // This magic number is the number of 100 nanosecond intervals since January 1, 1601 (UTC)
//    // until 00:00:00 January 1, 1970 
//    static const uint64_t EPOCH = ((uint64_t)116444736000000000ULL);
//
//    SYSTEMTIME  system_time;
//    FILETIME    file_time;
//    uint64_t    time;
//
//    GetSystemTime(&system_time);
//    SystemTimeToFileTime(&system_time, &file_time);
//    time = ((uint64_t)file_time.dwLowDateTime);
//    time += ((uint64_t)file_time.dwHighDateTime) << 32;
//
//    tp->tv_sec = (long)((time - EPOCH) / 10000000L);
//    tp->tv_usec = (long)(system_time.wMilliseconds * 1000);
//    return 0;
//}

#define __inline__

template <typename T>
int has_even_parity(T x)
{
    unsigned char shift = 1;
    while (shift < (sizeof(T) * 8)) {
        x ^= (x >> shift);
        shift <<= 1;
    }
    return !(x & 0x1);
}

#define __builtin_parity(x) !has_even_parity(x)

#endif // _WIN32_H
