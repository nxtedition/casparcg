#!/usr/bin/env python3
"""
AMCP Client for CasparCG self-tests.
Provides a simple interface to send commands and receive responses.
"""

import socket
import time
from typing import Optional, Tuple, List


class AMCPClient:
    """Simple AMCP protocol client for CasparCG."""

    def __init__(self, host: str = "localhost", port: int = 5250, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.socket: Optional[socket.socket] = None
        self.buffer = ""

    def connect(self) -> bool:
        """Connect to CasparCG server."""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(self.timeout)
            self.socket.connect((self.host, self.port))
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def disconnect(self):
        """Disconnect from server."""
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
            self.socket = None

    def send(self, command: str) -> Tuple[int, str]:
        """
        Send AMCP command and return response.

        Returns:
            Tuple of (response_code, response_data)
            Response codes:
                200-202: Success
                400-404: Client error
                500+: Server error
                -1: Connection error
        """
        if not self.socket:
            return (-1, "Not connected")

        try:
            # Send command with CRLF terminator
            full_command = command.strip() + "\r\n"
            self.socket.sendall(full_command.encode('utf-8'))

            # Read response
            response = self._read_response()
            return self._parse_response(response)

        except Exception as e:
            return (-1, f"Error: {e}")

    def _read_response(self) -> str:
        """Read complete response from server."""
        response_lines = []

        while True:
            # Read data
            try:
                data = self.socket.recv(4096).decode('utf-8')
                if not data:
                    break
                self.buffer += data
            except socket.timeout:
                break

            # Process complete lines
            while "\r\n" in self.buffer:
                line, self.buffer = self.buffer.split("\r\n", 1)
                response_lines.append(line)

                # Check if this is a single-line response (2xx codes)
                if response_lines and len(response_lines) == 1:
                    first_line = response_lines[0]
                    if first_line.startswith(("202", "400", "401", "402", "403", "404", "500", "501", "502")):
                        return "\r\n".join(response_lines)

                # Multi-line responses end with empty line
                if line == "" and len(response_lines) > 1:
                    return "\r\n".join(response_lines)

        return "\r\n".join(response_lines)

    def _parse_response(self, response: str) -> Tuple[int, str]:
        """Parse AMCP response into code and data."""
        if not response:
            return (-1, "Empty response")

        lines = response.split("\r\n")
        first_line = lines[0]

        # Extract response code
        parts = first_line.split(" ", 1)
        try:
            code = int(parts[0])
            data = "\r\n".join(lines[1:]) if len(lines) > 1 else (parts[1] if len(parts) > 1 else "")
            return (code, data.strip())
        except ValueError:
            return (-1, response)

    def is_connected(self) -> bool:
        """Check if connected to server."""
        return self.socket is not None

    # Convenience methods for common commands

    def version(self) -> Tuple[int, str]:
        """Get server version."""
        return self.send("VERSION")

    def info(self, channel: Optional[int] = None) -> Tuple[int, str]:
        """Get channel or server info."""
        if channel:
            return self.send(f"INFO {channel}")
        return self.send("INFO")

    def play_color(self, channel: int, layer: int, color: str) -> Tuple[int, str]:
        """Play a solid color on specified channel/layer."""
        return self.send(f"PLAY {channel}-{layer} COLOR {color}")

    def loadbg_color(self, channel: int, layer: int, color: str) -> Tuple[int, str]:
        """Load a color to background."""
        return self.send(f"LOADBG {channel}-{layer} COLOR {color}")

    def play(self, channel: int, layer: int, producer: str = "") -> Tuple[int, str]:
        """Play content on channel/layer."""
        if producer:
            return self.send(f"PLAY {channel}-{layer} {producer}")
        return self.send(f"PLAY {channel}-{layer}")

    def stop(self, channel: int, layer: int) -> Tuple[int, str]:
        """Stop playback on channel/layer."""
        return self.send(f"STOP {channel}-{layer}")

    def clear(self, channel: int, layer: Optional[int] = None) -> Tuple[int, str]:
        """Clear channel or specific layer."""
        if layer is not None:
            return self.send(f"CLEAR {channel}-{layer}")
        return self.send(f"CLEAR {channel}")

    def add_consumer(self, channel: int, consumer: str, args: str = "") -> Tuple[int, str]:
        """Add a consumer to channel."""
        cmd = f"ADD {channel} {consumer}"
        if args:
            cmd += f" {args}"
        return self.send(cmd)

    def remove_consumer(self, channel: int, consumer: str) -> Tuple[int, str]:
        """Remove a consumer from channel."""
        return self.send(f"REMOVE {channel} {consumer}")

    def mixer(self, channel: int, layer: int, command: str, *args) -> Tuple[int, str]:
        """Send MIXER command."""
        args_str = " ".join(str(a) for a in args)
        return self.send(f"MIXER {channel}-{layer} {command} {args_str}".strip())

    def mixer_opacity(self, channel: int, layer: int, opacity: float) -> Tuple[int, str]:
        """Set layer opacity."""
        return self.mixer(channel, layer, "OPACITY", opacity)

    def mixer_blend(self, channel: int, layer: int, mode: str) -> Tuple[int, str]:
        """Set layer blend mode."""
        return self.mixer(channel, layer, "BLEND", mode)

    def mixer_fill(self, channel: int, layer: int, x: float, y: float,
                   width: float, height: float) -> Tuple[int, str]:
        """Set layer fill (position and scale)."""
        return self.mixer(channel, layer, "FILL", x, y, width, height)

    def mixer_brightness(self, channel: int, layer: int, brightness: float) -> Tuple[int, str]:
        """Set layer brightness."""
        return self.mixer(channel, layer, "BRIGHTNESS", brightness)

    def mixer_contrast(self, channel: int, layer: int, contrast: float) -> Tuple[int, str]:
        """Set layer contrast."""
        return self.mixer(channel, layer, "CONTRAST", contrast)

    def mixer_saturation(self, channel: int, layer: int, saturation: float) -> Tuple[int, str]:
        """Set layer saturation."""
        return self.mixer(channel, layer, "SATURATION", saturation)


class AMCPTestHelper:
    """Helper class for running AMCP-based tests."""

    def __init__(self, client: AMCPClient):
        self.client = client
        self.errors: List[str] = []

    def assert_success(self, result: Tuple[int, str], message: str = "") -> bool:
        """Assert that a command succeeded (2xx response)."""
        code, data = result
        if code < 200 or code >= 300:
            error = f"Command failed: {message} - Code {code}: {data}"
            self.errors.append(error)
            print(f"  FAIL: {error}")
            return False
        print(f"  OK: {message}")
        return True

    def wait(self, seconds: float):
        """Wait for specified duration."""
        time.sleep(seconds)

    def get_errors(self) -> List[str]:
        """Get list of errors encountered."""
        return self.errors

    def has_errors(self) -> bool:
        """Check if any errors occurred."""
        return len(self.errors) > 0

    def reset(self):
        """Reset error state."""
        self.errors = []


if __name__ == "__main__":
    # Quick connection test
    client = AMCPClient()
    if client.connect():
        print("Connected to CasparCG")
        code, version = client.version()
        print(f"Version: {version}")
        client.disconnect()
    else:
        print("Failed to connect to CasparCG")
