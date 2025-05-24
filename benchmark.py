import subprocess
import itertools
import re
import os
import sys
import csv
import signal
import time

# --- Configuration ---
MAIN_EXECUTABLE = "./main"
DEFAULT_SOSD_FILE = "../data/osm_cellids_200M_uint64"
BENCHMARK_TIMEOUT = 600  # Timeout in seconds (e.g., 10 minutes) per run

# --- Benchmark Parameters ---
# Define the lists of parameters you want to test.
# The order here doesn't strictly matter anymore, as we'll define it explicitly.
benchmark_params = {
    "workers": [4, 8, 12, 16, 20, 24, 28, 32],
    "index_type": ["bplustree", "alex"],
    "joiner_type": ["handshake", "broadcast"],
    "stream_type": ["sosd"],
    "tuples_r": [800000],
    "tuples_s": [800000],
    "window_size": [500000],
    "diff": [(2**63 - 1) * 0.0001],  # int64_max * 0.0001
    "channel_buffer_size": [128],
    "sosd_file": [DEFAULT_SOSD_FILE],
    "sosd_shuffle": [1],
    "log_level": ["off"],
    "preload": [0],
    "watcher_enabled": [0],
}

# --- Global Variable for Subprocess ---
current_process = None


# --- Signal Handler ---
def signal_handler(sig, frame):
    """Handles SIGINT (Ctrl+C) to terminate the subprocess."""
    global current_process
    print("\nCtrl+C detected! Terminating subprocess...", file=sys.stderr)

    if current_process and current_process.poll() is None:
        print(f"Attempting to terminate PID: {current_process.pid}", file=sys.stderr)
        try:
            current_process.terminate()
            current_process.wait(timeout=5)
            print("Subprocess terminated gracefully.", file=sys.stderr)
        except subprocess.TimeoutExpired:
            print("Subprocess did not terminate, sending SIGKILL...", file=sys.stderr)
            current_process.kill()
            current_process.wait()
            print("Subprocess killed.", file=sys.stderr)
        except Exception as e:
            print(f"Error during subprocess termination: {e}", file=sys.stderr)
            if current_process.poll() is None:
                current_process.kill()
                current_process.wait()
    else:
        print("No active subprocess found or already terminated.", file=sys.stderr)

    sys.exit(130)


# --- Helper Functions ---


def check_executable(path):
    """Checks if the main executable exists and is executable."""
    if not os.path.exists(path):
        print(f"Error: Executable '{path}' not found.", file=sys.stderr)
        sys.exit(1)
    if not os.access(path, os.X_OK):
        print(f"Error: Executable '{path}' is not executable.", file=sys.stderr)
        sys.exit(1)


def generate_configs(params, desired_order):
    """
    Generates all combinations of benchmark parameters, respecting the
    desired_order for iteration. Keys in desired_order will be iterated
    like nested loops (first key is outermost loop).
    """

    # Start with the desired order
    ordered_keys = desired_order[:]

    # Add any remaining keys from params that are not in desired_order
    for key in params.keys():
        if key not in ordered_keys:
            ordered_keys.append(key)

    # Get the lists of values in the now-defined order
    ordered_values = [params[key] for key in ordered_keys]

    # Use itertools.product. The order of input lists determines the
    # iteration order. The first list changes slowest, the last fastest.
    # This matches the nested loop analogy.
    for combination in itertools.product(*ordered_values):
        yield dict(zip(ordered_keys, combination))


def parse_output(stdout):
    """Parses the standard output of the C++ program."""
    pattern = re.compile(
        r"^(.*?):\s*([\d.]+)\s*ms\s*\|\s*([\d.]+)\s*tuples/s", re.MULTILINE
    )
    match = pattern.search(stdout)
    if match:
        label = match.group(1).strip()
        duration_ms = float(match.group(2))
        throughput = float(match.group(3))
        return label, duration_ms, throughput
    else:
        print("Warning: Could not parse output line.", file=sys.stderr)
        print("--- Stdout ---", file=sys.stderr)
        print(stdout, file=sys.stderr)
        print("--------------", file=sys.stderr)
        return None, None, None


def run_single_benchmark(config):
    """Runs a single benchmark test using Popen and returns results."""
    global current_process
    cmd = [MAIN_EXECUTABLE]
    for key, value in config.items():
        cmd.append(f"--{key}")
        cmd.append(str(value))

    print(f"Running: {' '.join(cmd)}", file=sys.stderr)

    try:
        current_process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        stdout, stderr = current_process.communicate(timeout=BENCHMARK_TIMEOUT)
        returncode = current_process.returncode
        pid = current_process.pid
        current_process = None

        if returncode != 0:
            print(
                f"Error during execution (PID: {pid}, Return Code: {returncode}): {' '.join(cmd)}",
                file=sys.stderr,
            )
            print(f"Stderr:\n{stderr}", file=sys.stderr)
            return (
                config["joiner_type"],
                config["index_type"],
                config["workers"],
                "RUN_ERROR",
                "RUN_ERROR",
            )

        label, duration, throughput = parse_output(stdout)
        if label:
            return (
                config["joiner_type"],
                config["index_type"],
                config["workers"],
                duration,
                throughput,
            )
        else:
            return (
                config["joiner_type"],
                config["index_type"],
                config["workers"],
                "PARSE_ERROR",
                "PARSE_ERROR",
            )

    except FileNotFoundError:
        print(f"Error: Command '{MAIN_EXECUTABLE}' not found.", file=sys.stderr)
        current_process = None
        return (
            config["joiner_type"],
            config["index_type"],
            config["workers"],
            "EXEC_ERROR",
            "EXEC_ERROR",
        )
    except subprocess.TimeoutExpired:
        print(f"Error: Benchmark timed out: {' '.join(cmd)}", file=sys.stderr)
        if current_process:
            current_process.kill()
            current_process.communicate()
        current_process = None
        return (
            config["joiner_type"],
            config["index_type"],
            config["workers"],
            "TIMEOUT",
            "TIMEOUT",
        )
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        if current_process and current_process.poll() is None:
            current_process.kill()
            current_process.wait()
        current_process = None
        return (
            config["joiner_type"],
            config["index_type"],
            config["workers"],
            "UNKNOWN_ERROR",
            "UNKNOWN_ERROR",
        )
    finally:
        current_process = None


# --- Main Script ---


def main():
    """Runs the full benchmark suite using generate_configs and prints results."""

    signal.signal(signal.SIGINT, signal_handler)
    check_executable(MAIN_EXECUTABLE)

    csv_writer = csv.writer(sys.stdout)
    header = [
        "JoinerType",
        "IndexType",
        "Workers",
        "Duration_ms",
        "Throughput_tuples_s",
    ]
    csv_writer.writerow(header)
    sys.stdout.flush()

    # Define the desired order for the main loops
    # The first item changes slowest, the last changes fastest.
    main_loop_order = ["joiner_type", "index_type", "workers"]

    # Generate and run all configurations in the specified order
    for config in generate_configs(benchmark_params, main_loop_order):
        # --- Sanity Checks/Skips ---
        if config["stream_type"] == "sosd" and not os.path.exists(config["sosd_file"]):
            print(
                f"Error: SOSD file '{config['sosd_file']}' not found. Skipping test.",
                file=sys.stderr,
            )
            csv_writer.writerow(
                [
                    config["joiner_type"],
                    config["index_type"],
                    config["workers"],
                    "SOSD_FILE_MISSING",
                    "SOSD_FILE_MISSING",
                ]
            )
            continue

        # Run the test
        result_row = run_single_benchmark(config)

        # Write result
        csv_writer.writerow(result_row)
        sys.stdout.flush()

    print("\nBenchmark finished.", file=sys.stderr)


if __name__ == "__main__":
    main()
