#!/bin/bash
# Test script to debug screen consumer

pkill -9 -f casparcg 2>/dev/null || true
sleep 2

cd /Users/olzzon/coding/nxt/casparcg/build/shell
./casparcg > /tmp/ccg_screen.log 2>&1 &
CASPARCG_PID=$!
echo "Started CasparCG (PID: $CASPARCG_PID)"
sleep 8

echo "Sending PLAY command at $(date +%H:%M:%S)"
python3 << 'PYEOF'
import socket
import time
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost', 5250))
s.settimeout(5)
try: s.recv(1024)
except: pass

print("Playing RED...")
s.send(b'PLAY 1-1 COLOR RED\r\n')
time.sleep(0.5)
resp = s.recv(1024)
print(f"Response: {resp.decode().strip()}")
time.sleep(5)  # Wait longer for frames to propagate
s.close()
PYEOF

echo "Finished at $(date +%H:%M:%S)"
sleep 2
kill $CASPARCG_PID 2>/dev/null
sleep 1

echo ""
echo "=== Screen consumer frame data (last 20) ==="
grep -E "Frame data" /tmp/ccg_screen.log | tail -20

echo ""
echo "=== copy_to results (first 10) ==="
grep -E "copy_to result" /tmp/ccg_screen.log | head -10

echo ""
echo "=== Blend pipeline render calls ==="
grep -E "\[vk::blend_pipeline\] Render:" /tmp/ccg_screen.log | head -5

echo ""
echo "=== Mixer frame created ==="
grep -E "\[mixer\] Frame created" /tmp/ccg_screen.log | head -10
