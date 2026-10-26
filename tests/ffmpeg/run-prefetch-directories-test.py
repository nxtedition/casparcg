#!/usr/bin/env python3
"""Linux integration test: run with the path to a built casparcg executable.

Intercept shared-protocol opens to check directories before FFmpeg uses them.
No network source or custom FFmpeg shared-protocol build is required.
"""
import os
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import time

SHIM = r'''
#include <libavformat/avio.h>
#include <libavutil/dict.h>
#include <libavutil/error.h>
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static int directory(AVDictionary *options, const char *name)
{
    const AVDictionaryEntry *entry = av_dict_get(options, name, NULL, 0);
    struct stat info;
    return entry && !stat(entry->value, &info) && S_ISDIR(info.st_mode);
}

int avio_open2(AVIOContext **io, const char *url, int flags,
               const AVIOInterruptCB *callback, AVDictionary **options)
{
    if (!strncmp(url, "shared:", 7)) {
        FILE *marker = fopen(getenv("PREFETCH_TEST_MARKER"), "w");
        if (!marker)
            return AVERROR(EIO);
        fprintf(marker, "%d %d\n", directory(*options, "cache_dir"),
                directory(*options, "metadata_dir"));
        fclose(marker);
        return AVERROR_EXIT;
    }
    int (*original)(AVIOContext **, const char *, int, const AVIOInterruptCB *, AVDictionary **)
        = dlsym(RTLD_NEXT, "avio_open2");
    return original(io, url, flags, callback, options);
}
'''


def run_case(binary, shim, root, override, metadata, preexisting):
    root.mkdir()
    cache = root / ("custom/cache" if override else "default/cache")
    meta = root / "custom/metadata" if metadata else None
    if preexisting:
        cache.mkdir(parents=True)
        if meta:
            meta.mkdir(parents=True)
    options = (f"-cache_dir {cache}" if override else "")
    if meta:
        options += f" -metadata_dir {meta}"
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    config = root / "casparcg.config"
    config.write_text(f"""<configuration>
<paths><media-path>{root}/media</media-path><data-path>{root}/data</data-path>
<template-path>{root}/template</template-path><log-path>{root}/log</log-path></paths>
<channels/><controllers><tcp><port>{port}</port><protocol>AMCP</protocol></tcp></controllers>
<ffmpeg><producer><cache><path>{root}/default/cache</path></cache>
<options><shared>{options}</shared></options></producer></ffmpeg>
</configuration>""")
    marker = root / "opened"
    env = dict(os.environ, LD_PRELOAD=str(shim), PREFETCH_TEST_MARKER=str(marker), EGL_PLATFORM="surfaceless")
    with (root / "server.log").open("w") as log:
        process = subprocess.Popen([str(binary), str(config)], cwd=root, env=env,
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 20
            while True:
                if process.poll() is not None:
                    raise RuntimeError((root / "server.log").read_text())
                try:
                    client = socket.create_connection(("127.0.0.1", port), timeout=0.2)
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError("AMCP startup timed out")
                    time.sleep(0.05)
            with client:
                client.sendall(b'PREFETCH "http://example.com/clip.mov"\r\n')
                while not marker.exists() or marker.stat().st_size == 0:
                    if time.monotonic() >= deadline:
                        raise RuntimeError("PREFETCH did not open shared URL")
                    time.sleep(0.05)
            expected = f"1 {int(metadata)}"
            actual = marker.read_text().strip()
            if actual != expected:
                raise RuntimeError(f"{root.name}: expected cache/metadata directories {expected}, got {actual}")
            print(f"{root.name}: passed")
        finally:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def main():
    binary = Path(sys.argv[1]).resolve()
    with tempfile.TemporaryDirectory(prefix="casparcg-prefetch-") as tmp:
        root = Path(tmp)
        source, shim = root / "open.c", root / "open.so"
        source.write_text(SHIM)
        flags = shlex.split(subprocess.check_output(
            ["pkg-config", "--cflags", "--libs", "libavformat", "libavutil"], text=True))
        subprocess.run(["cc", "-shared", "-fPIC", str(source), "-o", str(shim), *flags, "-ldl"], check=True)
        for name, override, metadata, preexisting in [
            ("default", False, False, False),
            ("override", True, False, False),
            ("separate-metadata", True, True, False),
            ("existing", True, True, True),
        ]:
            run_case(binary, shim, root / name, override, metadata, preexisting)


if __name__ == "__main__":
    main()
