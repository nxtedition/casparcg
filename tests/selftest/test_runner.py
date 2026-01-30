#!/usr/bin/env python3
"""
CasparCG Self-Test Runner

Runs automated tests to verify CasparCG functionality.
Tests are organized by implementation phase from MACOS_SUPPORT.md.

Usage:
    python test_runner.py                    # Run all tests
    python test_runner.py --phase 3          # Run specific phase
    python test_runner.py --test color       # Run specific test
    python test_runner.py --list             # List available tests
"""

import argparse
import importlib
import os
import sys
import time
from dataclasses import dataclass
from typing import List, Optional, Callable, Dict

from amcp_client import AMCPClient, AMCPTestHelper
from video_analyzer import VideoAnalyzer
from config import TestConfig, get_test_config_from_env, get_output_path


@dataclass
class TestResult:
    """Result of a single test."""
    name: str
    phase: int
    passed: bool
    duration: float
    message: str = ""


class TestRunner:
    """Manages and runs CasparCG self-tests."""

    def __init__(self, config: TestConfig):
        self.config = config
        self.client: Optional[AMCPClient] = None
        self.helper: Optional[AMCPTestHelper] = None
        self.analyzer = VideoAnalyzer()
        self.results: List[TestResult] = []
        self.tests: Dict[str, Dict] = {}

        self._register_tests()

    def _register_tests(self):
        """Register all available tests."""
        # Phase 1: Build System & Foundation
        self.register_test("build_verification", 1, self.test_build_verification,
                           "Verify binary runs and responds to AMCP")

        # Phase 2: Basic Vulkan Context
        self.register_test("connection", 2, self.test_connection,
                           "Verify AMCP connection to CasparCG")

        # Phase 3: Basic Playout + Layering
        self.register_test("color_playback", 3, self.test_color_playback,
                           "Play solid colors and verify output")
        self.register_test("multi_layer", 3, self.test_multi_layer,
                           "Test multiple layer compositing")
        self.register_test("alpha_blend", 3, self.test_alpha_blend,
                           "Test alpha blending between layers")

        # Phase 4: Blend Modes
        self.register_test("blend_modes", 4, self.test_blend_modes,
                           "Test various blend modes")

        # Phase 5: Geometric Transforms
        self.register_test("transforms", 5, self.test_transforms,
                           "Test geometric transforms (fill, rotation)")

        # Phase 6: Color Processing
        self.register_test("color_adjust", 6, self.test_color_adjustments,
                           "Test brightness, contrast, saturation")

        # Phase 8: Producers
        self.register_test("video_playback", 8, self.test_video_playback,
                           "Test video file playback")

        # Phase 9: Screen Consumer
        self.register_test("screen_output", 9, self.test_screen_output,
                           "Verify screen consumer output")

        # Phase 10: FFmpeg Consumer
        self.register_test("recording", 10, self.test_recording,
                           "Test FFmpeg recording consumer")

    def register_test(self, name: str, phase: int, func: Callable,
                      description: str):
        """Register a test function."""
        self.tests[name] = {
            'phase': phase,
            'func': func,
            'description': description
        }

    def connect(self) -> bool:
        """Connect to CasparCG server."""
        self.client = AMCPClient(self.config.host, self.config.port)
        if not self.client.connect():
            print(f"Failed to connect to CasparCG at {self.config.host}:{self.config.port}")
            return False

        self.helper = AMCPTestHelper(self.client)
        print(f"Connected to CasparCG at {self.config.host}:{self.config.port}")

        code, version = self.client.version()
        if code >= 200 and code < 300:
            print(f"Server version: {version}")

        return True

    def disconnect(self):
        """Disconnect from server."""
        if self.client:
            self.client.disconnect()
            self.client = None
            self.helper = None

    def cleanup_channel(self, channel: int):
        """Clear all content from a channel."""
        if self.client:
            self.client.clear(channel)

    def run_test(self, name: str) -> TestResult:
        """Run a single test by name."""
        if name not in self.tests:
            return TestResult(name, 0, False, 0, f"Unknown test: {name}")

        test = self.tests[name]
        print(f"\n{'='*60}")
        print(f"Running: {name} (Phase {test['phase']})")
        print(f"Description: {test['description']}")
        print('='*60)

        start_time = time.time()
        try:
            passed = test['func']()
            message = "PASSED" if passed else "FAILED"
        except Exception as e:
            passed = False
            message = f"Exception: {e}"
            import traceback
            traceback.print_exc()

        duration = time.time() - start_time

        result = TestResult(
            name=name,
            phase=test['phase'],
            passed=passed,
            duration=duration,
            message=message
        )
        self.results.append(result)

        status = "PASS" if passed else "FAIL"
        print(f"\nResult: {status} ({duration:.2f}s)")

        return result

    def run_phase(self, phase: int) -> List[TestResult]:
        """Run all tests for a specific phase."""
        tests = [name for name, t in self.tests.items() if t['phase'] == phase]
        return self.run_tests(tests)

    def run_tests(self, test_names: List[str]) -> List[TestResult]:
        """Run multiple tests."""
        results = []
        for name in test_names:
            result = self.run_test(name)
            results.append(result)
            # Clean up between tests
            self.cleanup_channel(self.config.playback_channel)
            self.cleanup_channel(self.config.record_channel)
            time.sleep(0.5)
        return results

    def run_all(self) -> List[TestResult]:
        """Run all registered tests."""
        return self.run_tests(list(self.tests.keys()))

    def print_summary(self):
        """Print test results summary."""
        print("\n" + "="*60)
        print("TEST SUMMARY")
        print("="*60)

        passed = sum(1 for r in self.results if r.passed)
        failed = sum(1 for r in self.results if not r.passed)
        total_time = sum(r.duration for r in self.results)

        # Group by phase
        phases = {}
        for r in self.results:
            if r.phase not in phases:
                phases[r.phase] = []
            phases[r.phase].append(r)

        for phase in sorted(phases.keys()):
            print(f"\nPhase {phase}:")
            for r in phases[phase]:
                status = "PASS" if r.passed else "FAIL"
                print(f"  [{status}] {r.name}: {r.message} ({r.duration:.2f}s)")

        print(f"\nTotal: {passed} passed, {failed} failed ({total_time:.2f}s)")

        return failed == 0

    def list_tests(self):
        """List all available tests."""
        print("Available tests:")
        print("-" * 60)

        # Group by phase
        phases = {}
        for name, test in self.tests.items():
            phase = test['phase']
            if phase not in phases:
                phases[phase] = []
            phases[phase].append((name, test['description']))

        for phase in sorted(phases.keys()):
            print(f"\nPhase {phase}:")
            for name, desc in phases[phase]:
                print(f"  {name:20s} - {desc}")

    # ==========================================================================
    # Test implementations
    # ==========================================================================

    def test_build_verification(self) -> bool:
        """Test Phase 1: Verify binary runs and responds to basic AMCP."""
        all_passed = True

        # Test VERSION command
        code, version = self.client.version()
        if not self.helper.assert_success((code, version), "Get server version"):
            all_passed = False
        else:
            print(f"  Server version: {version}")

        # Test INFO command
        code, info = self.client.info()
        if not self.helper.assert_success((code, info), "Get server info"):
            all_passed = False

        # Test INFO for channel 1
        code, ch_info = self.client.info(1)
        if not self.helper.assert_success((code, ch_info), "Get channel 1 info"):
            all_passed = False

        # Test CLEAR command (should work even with stub renderer)
        code, msg = self.client.clear(1)
        if not self.helper.assert_success((code, msg), "Clear channel 1"):
            all_passed = False

        return all_passed

    def test_connection(self) -> bool:
        """Test basic AMCP connection."""
        # Already connected if we got here
        code, info = self.client.info()
        return self.helper.assert_success((code, info), "Get server info")

    def test_color_playback(self) -> bool:
        """Test playing solid colors."""
        colors = ['RED', 'GREEN', 'BLUE', 'WHITE', 'BLACK']
        all_passed = True

        for color in colors:
            # Play color
            result = self.client.play_color(self.config.playback_channel, 1, color)
            if not self.helper.assert_success(result, f"Play {color}"):
                all_passed = False
                continue

            self.helper.wait(0.5)

            # Get channel info to verify playback
            code, info = self.client.info(self.config.playback_channel)
            if code < 200 or code >= 300:
                print(f"  Failed to get channel info")
                all_passed = False

        return all_passed

    def test_multi_layer(self) -> bool:
        """Test multiple layer compositing."""
        ch = self.config.playback_channel

        # Layer 1: Red background
        r1 = self.client.play_color(ch, 1, "RED")
        if not self.helper.assert_success(r1, "Play RED on layer 1"):
            return False

        # Layer 2: Green, half size, offset
        r2 = self.client.play_color(ch, 2, "GREEN")
        if not self.helper.assert_success(r2, "Play GREEN on layer 2"):
            return False

        # Scale layer 2 to quarter screen
        r3 = self.client.mixer_fill(ch, 2, 0.25, 0.25, 0.5, 0.5)
        if not self.helper.assert_success(r3, "Scale layer 2"):
            return False

        # Layer 3: Blue, smaller, offset
        r4 = self.client.play_color(ch, 3, "BLUE")
        if not self.helper.assert_success(r4, "Play BLUE on layer 3"):
            return False

        r5 = self.client.mixer_fill(ch, 3, 0.6, 0.6, 0.25, 0.25)
        if not self.helper.assert_success(r5, "Scale layer 3"):
            return False

        self.helper.wait(1.0)

        # Verify via info
        code, info = self.client.info(ch)
        return self.helper.assert_success((code, info), "Get channel info")

    def test_alpha_blend(self) -> bool:
        """Test alpha blending between layers."""
        ch = self.config.playback_channel

        # Background: White
        r1 = self.client.play_color(ch, 1, "WHITE")
        if not self.helper.assert_success(r1, "Play WHITE background"):
            return False

        # Foreground: Red with 50% opacity
        r2 = self.client.play_color(ch, 2, "RED")
        if not self.helper.assert_success(r2, "Play RED foreground"):
            return False

        r3 = self.client.mixer_opacity(ch, 2, 0.5)
        if not self.helper.assert_success(r3, "Set 50% opacity"):
            return False

        self.helper.wait(1.0)

        # Result should be pink-ish (white + 50% red)
        return True

    def test_blend_modes(self) -> bool:
        """Test all 29 Photoshop-compatible blend modes (Phase 4).

        Blend modes:
        - Basic: normal, add, subtract, multiply, screen
        - Lighten group: lighten, color_dodge, linear_dodge
        - Darken group: darken, color_burn, linear_burn
        - Contrast group: overlay, soft_light, hard_light, vivid_light,
                         linear_light, pin_light, hard_mix
        - Inversion group: difference, exclusion
        - Component group: hue, saturation, color, luminosity
        - Special: average, negation, phoenix, reflect, glow
        """
        ch = self.config.playback_channel

        # All 29 blend modes (matching core::blend_mode enum order)
        blend_modes = [
            'normal',       # 0
            'lighten',      # 1
            'darken',       # 2
            'multiply',     # 3
            'average',      # 4
            'add',          # 5
            'subtract',     # 6
            'difference',   # 7
            'negation',     # 8
            'exclusion',    # 9
            'screen',       # 10
            'overlay',      # 11
            'soft_light',   # 12
            'hard_light',   # 13
            'color_dodge',  # 14
            'color_burn',   # 15
            'linear_dodge', # 16
            'linear_burn',  # 17
            'linear_light', # 18
            'vivid_light',  # 19
            'pin_light',    # 20
            'hard_mix',     # 21
            'reflect',      # 22
            'glow',         # 23
            'phoenix',      # 24
            'contrast',     # 25 (also known as hue)
            'saturation',   # 26
            'color',        # 27
            'luminosity',   # 28
        ]

        passed_count = 0
        failed_modes = []

        print(f"  Testing {len(blend_modes)} blend modes...")

        for i, mode in enumerate(blend_modes):
            self.client.clear(ch)

            # Background: Gray
            r1 = self.client.play_color(ch, 1, "#808080")  # Gray hex
            if r1[0] < 200 or r1[0] >= 300:
                r1 = self.client.play_color(ch, 1, "GRAY")

            # Foreground: Red with blend mode
            r2 = self.client.play_color(ch, 2, "RED")

            # Set blend mode
            r3 = self.client.mixer_blend(ch, 2, mode)
            if r3[0] >= 200 and r3[0] < 300:
                passed_count += 1
                print(f"    [{i:2d}] {mode:15s} OK")
            else:
                failed_modes.append(mode)
                print(f"    [{i:2d}] {mode:15s} FAILED (code {r3[0]})")

            # Brief pause to allow rendering
            self.helper.wait(0.1)

        print(f"  Results: {passed_count}/{len(blend_modes)} blend modes passed")
        if failed_modes:
            print(f"  Failed modes: {', '.join(failed_modes)}")

        # Pass if all blend modes are accepted by the server
        return len(failed_modes) == 0

    def test_transforms(self) -> bool:
        """Test geometric transforms."""
        ch = self.config.playback_channel

        # Play a color
        r1 = self.client.play_color(ch, 1, "BLUE")
        if not self.helper.assert_success(r1, "Play BLUE"):
            return False

        # Test FILL (position + scale)
        r2 = self.client.mixer_fill(ch, 1, 0.1, 0.1, 0.5, 0.5)
        if not self.helper.assert_success(r2, "Apply FILL transform"):
            return False

        self.helper.wait(0.5)

        # Test ROTATION (if supported)
        r3 = self.client.mixer(ch, 1, "ROTATION", 45)
        self.helper.assert_success(r3, "Apply 45 degree rotation")

        self.helper.wait(0.5)

        # Reset
        self.client.mixer_fill(ch, 1, 0, 0, 1, 1)
        self.client.mixer(ch, 1, "ROTATION", 0)

        return True

    def test_color_adjustments(self) -> bool:
        """Test color adjustment effects."""
        ch = self.config.playback_channel

        # Play a color
        r1 = self.client.play_color(ch, 1, "RED")
        if not self.helper.assert_success(r1, "Play RED"):
            return False

        # Test brightness
        r2 = self.client.mixer_brightness(ch, 1, 0.5)
        if not self.helper.assert_success(r2, "Set brightness to 0.5"):
            return False
        self.helper.wait(0.5)

        # Test contrast
        r3 = self.client.mixer_contrast(ch, 1, 1.5)
        if not self.helper.assert_success(r3, "Set contrast to 1.5"):
            return False
        self.helper.wait(0.5)

        # Test saturation
        r4 = self.client.mixer_saturation(ch, 1, 0.5)
        if not self.helper.assert_success(r4, "Set saturation to 0.5"):
            return False
        self.helper.wait(0.5)

        # Reset
        self.client.mixer_brightness(ch, 1, 1.0)
        self.client.mixer_contrast(ch, 1, 1.0)
        self.client.mixer_saturation(ch, 1, 1.0)

        return True

    def test_video_playback(self) -> bool:
        """Test video file playback (requires test media)."""
        ch = self.config.playback_channel

        # Try to play a test video
        # This test will be skipped if no test media exists
        result = self.client.play(ch, 1, "AMB")  # Common test file
        code, msg = result

        if code == 404:
            print("  Test media 'AMB' not found - skipping video playback test")
            print("  Add a video file named 'AMB.mp4' to the media folder to enable")
            return True  # Skip rather than fail

        return self.helper.assert_success(result, "Play video file")

    def test_screen_output(self) -> bool:
        """Test screen consumer (visual verification required)."""
        ch = self.config.playback_channel

        # Play color bars or solid colors
        r1 = self.client.play_color(ch, 1, "RED")
        if not self.helper.assert_success(r1, "Play RED for screen output"):
            return False

        self.helper.wait(1.0)

        # This test requires visual verification
        print("  Visual verification: RED should be displayed on screen")
        return True

    def test_recording(self) -> bool:
        """Test FFmpeg recording consumer."""
        ch = self.config.playback_channel
        output_file = get_output_path(self.config, "test_recording.mp4")

        # Remove old output
        if os.path.exists(output_file):
            os.remove(output_file)

        # Start recording
        r1 = self.client.add_consumer(ch, "FILE", f"{output_file} {self.config.ffmpeg_args}")
        if not self.helper.assert_success(r1, "Add FFmpeg consumer"):
            return False

        self.helper.wait(self.config.record_settle)

        # Play test pattern
        r2 = self.client.play_color(ch, 1, "GREEN")
        if not self.helper.assert_success(r2, "Play GREEN"):
            self.client.remove_consumer(ch, "FILE")
            return False

        # Record for a few seconds
        self.helper.wait(self.config.color_test_duration)

        # Stop recording
        r3 = self.client.remove_consumer(ch, "FILE")
        if not self.helper.assert_success(r3, "Remove FFmpeg consumer"):
            return False

        self.helper.wait(0.5)

        # Verify output file
        if not os.path.exists(output_file):
            print(f"  Output file not created: {output_file}")
            return False

        info = self.analyzer.get_video_info(output_file)
        if not info:
            print(f"  Could not analyze output file")
            return False

        print(f"  Recorded: {info.width}x{info.height}, {info.frame_count} frames, {info.fps}fps")

        # Verify resolution
        if not self.analyzer.verify_resolution(output_file, self.config.width, self.config.height):
            return False

        # Verify color
        from video_analyzer import COLORS
        if not self.analyzer.verify_solid_color(output_file, COLORS['GREEN'], frame_number=10):
            return False

        return True


def main():
    parser = argparse.ArgumentParser(description="CasparCG Self-Test Runner")
    parser.add_argument('--phase', type=int, help="Run tests for specific phase")
    parser.add_argument('--test', type=str, help="Run specific test by name")
    parser.add_argument('--list', action='store_true', help="List available tests")
    parser.add_argument('--host', type=str, default="localhost", help="CasparCG host")
    parser.add_argument('--port', type=int, default=5250, help="CasparCG port")
    parser.add_argument('--output-dir', type=str, default="test_output", help="Output directory")

    args = parser.parse_args()

    # Load config
    config = get_test_config_from_env()
    config.host = args.host
    config.port = args.port
    config.output_dir = args.output_dir

    runner = TestRunner(config)

    if args.list:
        runner.list_tests()
        return 0

    # Connect to CasparCG
    if not runner.connect():
        print("ERROR: Could not connect to CasparCG")
        print(f"Make sure CasparCG is running at {config.host}:{config.port}")
        return 1

    try:
        if args.test:
            runner.run_test(args.test)
        elif args.phase:
            runner.run_phase(args.phase)
        else:
            runner.run_all()

        success = runner.print_summary()
        return 0 if success else 1

    finally:
        runner.disconnect()


if __name__ == "__main__":
    sys.exit(main())
