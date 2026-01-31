#!/bin/bash
# Automated test script for Claude

pkill -9 -f casparcg 2>/dev/null || true
sleep 2

cd /Users/olzzon/coding/nxt/casparcg/build/shell
./casparcg > /tmp/ccg_output.log 2>&1 &
CASPARCG_PID=$!
sleep 6

python3 -c "
import socket
import time
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost', 5250))
s.settimeout(2)
try: s.recv(1024)
except: pass
s.send(b'PLAY 1-1 COLOR RED\r\n')
time.sleep(0.5)
s.close()
"

sleep 3
kill $CASPARCG_PID 2>/dev/null
sleep 1

echo "=== RESULTS ===" > /tmp/ccg_results.txt
grep -E "copy_to result:" /tmp/ccg_output.log 2>/dev/null | head -15 >> /tmp/ccg_results.txt
echo "" >> /tmp/ccg_results.txt
echo "=== BLEND PIPELINE ===" >> /tmp/ccg_results.txt
grep -E "\[vk::blend_pipeline\]" /tmp/ccg_output.log 2>/dev/null | head -5 >> /tmp/ccg_results.txt
echo "" >> /tmp/ccg_results.txt
echo "=== IMAGE KERNEL ===" >> /tmp/ccg_results.txt
grep -E "\[vk::image_kernel\]" /tmp/ccg_output.log 2>/dev/null | head -5 >> /tmp/ccg_results.txt

cat /tmp/ccg_results.txt
