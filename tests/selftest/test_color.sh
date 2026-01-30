#!/bin/bash
cd /Users/olzzon/coding/nxt/casparcg/build/shell

# Kill any existing instance
pkill -f "casparcg" 2>/dev/null
sleep 1

# Start CasparCG in background
./casparcg > /tmp/casparcg_output.log 2>&1 &
PID=$!

sleep 5

# Test using printf with carriage return (AMCP requires \r\n)
echo "=== Trying VERSION command ==="
{ printf "VERSION\r\n"; sleep 1; } | nc localhost 5260

echo ""
echo "=== Trying PLAY command ==="
{ printf "PLAY 1-1 COLOR RED\r\n"; sleep 2; } | nc localhost 5260

echo ""
echo "=== CasparCG log (last 50 lines) ==="
tail -50 /tmp/casparcg_output.log

# Clean up
kill $PID 2>/dev/null
