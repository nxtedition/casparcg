#!/bin/bash
# Test script that takes a snapshot and analyzes the color

pkill -9 -f casparcg 2>/dev/null || true
sleep 2

cd /Users/olzzon/coding/nxt/casparcg/build/shell
./casparcg > /tmp/ccg_snap.log 2>&1 &
CASPARCG_PID=$!
sleep 8

SNAP_PATH="media/red_test_snapshot.png"

python3 << 'PYEOF'
import socket
import time

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost', 5250))
s.settimeout(5)
try: s.recv(1024)
except: pass

def cmd(c):
    s.send(f'{c}\r\n'.encode())
    time.sleep(0.3)
    try: return s.recv(4096).decode().strip()
    except: return ''

print('Playing RED...')
print(cmd('PLAY 1-1 COLOR RED'))
time.sleep(2)

print('Taking snapshot...')
print(cmd('ADD 1 IMAGE media/red_test_snapshot.png'))
time.sleep(1)
print(cmd('REMOVE 1 IMAGE'))
s.close()
PYEOF

sleep 2
kill $CASPARCG_PID 2>/dev/null
sleep 1

echo ""
echo "=== SNAPSHOT ANALYSIS ==="
if [ -f "$SNAP_PATH" ]; then
    ls -la "$SNAP_PATH"
    echo ""
    echo "Center pixel color (should be red = srgb(255,0,0)):"
    convert "$SNAP_PATH" -crop 10x10+640+360 -resize 1x1 txt:- 2>/dev/null | tail -1
    echo ""
    echo "Full image average color:"
    convert "$SNAP_PATH" -resize 1x1 txt:- 2>/dev/null | tail -1
else
    echo "Snapshot file not found: $SNAP_PATH"
fi
