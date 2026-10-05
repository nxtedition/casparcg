Deterministic rendering
=======================

A deterministic channel renders decoupled from the wall clock. It produces frames as fast as
its producers and consumers allow, and only while a consumer is attached, so the same
commands produce the same frames every run — faster or slower than realtime, whichever the
machine manages. It is meant for rendering to a file, not for playout.

A render is described with the four `SCHEDULE` commands and then committed as a unit. Nothing
touches the channel until `SCHEDULE COMMIT`.


Configuring the channel
-----------------------

```xml
<channels>
  <channel>
    <video-mode>1080p5000</video-mode>
    <deterministic>true</deterministic>
  </channel>
</channels>
```

A deterministic channel may not declare `<consumers>` or `<producers>`; the render attaches its
own, and the server refuses to start if it finds either.

The channel's `<video-mode>` is only its starting format. `SCHEDULE BEGIN` sets the format per
render, and the channel switches to it before the first frame.

Useful alongside it:

| Key | What it does |
|---|---|
| `configuration.shutdown-on-eof` | Once stdin reaches EOF, shut the server down as soon as no channel has a consumer. A script piped in to render to files then ends the process when the last recording is done. Linux only. |
| `configuration.shutdown-on-eof-grace-ms` | How long no channel may have a consumer before that shutdown, covering consumers that commands sent just before EOF have yet to attach. |
| `configuration.deterministic.producer-wait-timeout-s` | How long one producer may keep a render waiting before the render is given up on. `0` waits forever. |
| `configuration.deterministic.writer-stall-timeout-s` | How long a recording consumer's writer may accept no frame before it is treated as gone and the render is given up on. `0` waits forever. |
| `configuration.html.wait-for-fp` | Hold a HTML producer's first frame until the page has painted once. Always on for a producer on a deterministic channel. |


The commands
------------

All four reply `202 SCHEDULE <VERB> OK` on success and `403 SCHEDULE <VERB> FAILED` on refusal.
The client only gets the status code, so the reason is in the server log.

### `SCHEDULE <channel> BEGIN <video-mode> ADD <consumer> [params]`

Starts describing a render on `<channel>`, replacing any description not yet committed.

`<video-mode>` is the format the render produces, and must be progressive: the stage waits per
field, but no producer's wait reads the field it is given, so an interlaced render would sample
where it means to wait. Interlaced formats are refused rather than rendered wrong.

`ADD` takes a consumer exactly as `ADD` would, and it must support back-pressure — it has to be
able to make the channel wait rather than drop a frame. The `FILE`/`STREAM` (ffmpeg) consumer
does; `SCREEN`, `DECKLINK` and `NDI` do not, and are refused here. The consumer is built once at
`BEGIN` purely to reject a bad one early, and built again at `COMMIT` to do the recording.

```
SCHEDULE 1 BEGIN 1080p5000 ADD FILE out.mp4 -codec:v libx264 -preset ultrafast -filter:v format=yuv420p
```

### `SCHEDULE FRAME <frame> <command>`

Runs an ordinary AMCP command just before frame `<frame>` is produced — frame `0` before the
first. Several commands may share a frame, and they run in the order they were added.

The render is picked from the command's own channel, not from a channel on `SCHEDULE FRAME`
itself, so `SCHEDULE FRAME 0 PLAY 1-10 AMB` schedules on channel 1.

The command is parsed when it is scheduled, so a malformed one is refused immediately rather
than mid-render. `SCHEDULE` commands cannot themselves be scheduled.

```
SCHEDULE FRAME 0   PLAY 1-10 AMB
SCHEDULE FRAME 0   PLAY 1-20 [HTML] file:///srv/caspar/templates/lower-third.html
SCHEDULE FRAME 125 CALL 1-20 NEXT
SCHEDULE FRAME 250 STOP 1-20
```

### `SCHEDULE <channel> END <frames>`

The render stops once `<frames>` frames have been produced, so the recording holds exactly that
many. Required: `COMMIT` refuses a render without it.

Refused if a command is already scheduled at a frame the render never reaches.

### `SCHEDULE <channel> COMMIT`

Starts the render: switches the channel to the render's format, attaches the consumer, and runs
the schedule. Attaching the consumer is what sets the channel going.

Refused, leaving the description in place to retry, while the previous render on the channel is
still running.

When the render ends — on reaching `END`, or by being given up on — the channel detaches its
consumers, which closes off the recording, and resets itself ready for the next render.


A complete render
-----------------

```
SCHEDULE 1 BEGIN 1080p5000 ADD FILE /tmp/out.mp4 -codec:v libx264 -preset ultrafast -filter:v format=yuv420p -codec:a aac -b:a 128k
SCHEDULE FRAME 0 PLAY 1-10 AMB
SCHEDULE 1 END 50
SCHEDULE 1 COMMIT
```

Fifty frames at 50 fps: one second of video, byte-identical from run to run.

Piped in with `shutdown-on-eof` set, that script is a complete render job — the server exits
once the file is closed.


What makes it reproducible
--------------------------

The channel pulls its producers rather than sampling them. Before each frame it waits for every
producer on a layer it will draw, so no frame depends on whether a decoder happened to be ready
in time.

That only works for producers that can be paced by being pulled. A producer on an external
clock — decklink, NDI, a route from another channel — cannot be waited on, and the stage warns
once per producer that the render now depends on its timing.

HTML producers on a deterministic channel render on a virtual clock instead of the wall clock:
each frame advances the page's time by exactly one frame and is composited with an external
BeginFrame, so animations land on the same frames every run. A frame whose paint cannot be
matched to it is taken anyway and logged as not reproducible, both per frame and as a count when
the producer is torn down — if that appears in the log, the recording is not byte-identical.


When a render will not finish
-----------------------------

A producer that never delivers would hold the channel for good, so each wait has a ceiling
(`producer-wait-timeout-s`, 30 s by default). Reaching it ends the render with the producer
named in the log, detaches the consumers and resets the channel, rather than recording black at
full speed.

The same applies to the recording consumer: a writer that neither fails nor drains for
`writer-stall-timeout-s` is treated as gone and the render is given up on. A writer that fails
outright — a full disk, say — ends the render at once.

Both defaults were chosen as comfortably beyond any legitimate wait rather than measured against
a real workload, which is why they are configurable.
