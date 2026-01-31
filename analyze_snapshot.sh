#!/bin/bash
# Analyze the color of the test snapshot using ffmpeg

SNAP="/Users/olzzon/coding/nxt/casparcg/build/shell/media/test_snapshot.png"

echo "=== Snapshot Analysis ==="
if [ -f "$SNAP" ]; then
    ls -la "$SNAP"
    echo ""

    # Get image info
    echo "Image info:"
    ffprobe -v error -select_streams v:0 -show_entries stream=width,height,pix_fmt -of csv=p=0 "$SNAP"

    echo ""
    echo "Sampling center pixel using ffmpeg:"
    # Extract a 1x1 crop from the center and output the raw RGB values
    ffmpeg -i "$SNAP" -vf "crop=1:1:640:360,format=rgb24" -f rawvideo -frames:v 1 - 2>/dev/null | xxd -p | head -c 6
    echo " (hex RGB)"

    # Convert to decimal
    HEX=$(ffmpeg -i "$SNAP" -vf "crop=1:1:640:360,format=rgb24" -f rawvideo -frames:v 1 - 2>/dev/null | xxd -p | head -c 6)
    R=$((16#${HEX:0:2}))
    G=$((16#${HEX:2:2}))
    B=$((16#${HEX:4:2}))
    echo "Center pixel RGB: ($R, $G, $B)"

    echo ""
    echo "Expected for YELLOW: (255, 255, 0)"
    echo "Expected for RED: (255, 0, 0)"
else
    echo "Snapshot not found: $SNAP"
fi
