#!/bin/bash
# Test script for CasparCG color rendering

# Kill any existing CasparCG processes
pkill -9 -f casparcg 2>/dev/null || true
sleep 2

# Start CasparCG
cd /Users/olzzon/coding/nxt/casparcg/build/shell
./casparcg &
CASPARCG_PID=$!
sleep 6

# Send PLAY command
python3 << 'EOF'
import socket
import time
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost', 5250))
s.settimeout(2)
try: s.recv(1024)
except: pass
s.send(b'PLAY 1-1 COLOR RED\r\n')
time.sleep(0.5)
print("PLAY command sent")
s.close()
EOF

# Wait for frames to render
sleep 3

# Kill CasparCG
kill $CASPARCG_PID 2>/dev/null
sleep 1

# Show results
echo ""
echo "=== copy_to results ==="
grep -E "copy_to result:" log/casparcg.log 2>/dev/null | head -20

echo ""
echo "=== blend_pipeline logs ==="
grep -E "\[vk::blend_pipeline\]" log/casparcg.log 2>/dev/null | head -10

echo ""
echo "=== image_kernel logs ==="
grep -E "\[vk::image_kernel\]" log/casparcg.log 2>/dev/null | head -10
