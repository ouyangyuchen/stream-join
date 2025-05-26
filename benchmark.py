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
BENCHMARK_TIMEOUT = 3600  # Timeout for each benchmark run in seconds
NUM_ITERATIONS = 1  # Number of *non-timeout* runs required per config
MAX_ATTEMPTS_FACTOR = (
    2  # Max attempts = NUM_ITERATIONS * this factor (prevents infinite loops)
)

# --- Benchmark Parameters ---
benchmark_params = {
    "workers": [4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48],
    "index_type": ["bplustree", "list"],
    "joiner_type": ["handshake", "broadcast"],
    "stream_type": ["random"],
    "tuples_r": [2000000],
    "tuples_s": [2000000],
    "window_size": [1000000],
    "diff": [3000],
    "channel_buffer_size": [128],
    "sosd_file": [DEFAULT_SOSD_FILE],
    "sosd_shuffle": [1],
    "log_level": ["off"],
    "preload": [1],
    "watcher_enabled": [0],
}

# --- Global Variable for Subprocess ---
current_process = None


# --- Signal Handler (No changes needed) ---
def signal_handler(sig, frame):
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


# --- Helper Functions (check_executable, generate_configs, parse_output - no changes) ---
def check_executable(path):
    if not os.path.exists(path):
        print(f"Error: Executable '{path}' not found.", file=sys.stderr)
        sys.exit(1)
    if not os.access(path, os.X_OK):
        print(f"Error: Executable '{path}' is not executable.", file=sys.stderr)
        sys.exit(1)


def generate_configs(params, desired_order):
    ordered_keys = desired_order[:]
    for key in params.keys():
        if key not in ordered_keys:
            ordered_keys.append(key)
    ordered_values = [params[key] for key in ordered_keys]
    for combination in itertools.product(*ordered_values):
        yield dict(zip(ordered_keys, combination))


def parse_output(stdout):
    pattern = re.compile(
        r"^(.*?):\s*([\d.]+)\s*ms\s*\|\s*([\d.]+)\s*tuples/s", re.MULTILINE
    )
    match = pattern.search(stdout)
    if match:
        return float(match.group(2)), float(match.group(3))  # duration_ms, throughput
    else:
        print("Warning: Could not parse output line.", file=sys.stderr)
        print("--- Stdout ---", file=sys.stderr)
        print(stdout, file=sys.stderr)
        print("--------------", file=sys.stderr)
        return None, None


# --- run_single_benchmark (No changes needed) ---
def run_single_benchmark(config):
    global current_process
    cmd = [MAIN_EXECUTABLE]
    for key, value in config.items():
        cmd.append(f"--{key}")
        cmd.append(str(value))
    try:
        current_process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        stdout, stderr = current_process.communicate(timeout=BENCHMARK_TIMEOUT)
        returncode = current_process.returncode
        current_process = None
        if returncode != 0:
            print(
                f"    Single run error (Return Code: {returncode}): {' '.join(cmd)}",
                file=sys.stderr,
            )
            print(f"    Stderr:\n{stderr}", file=sys.stderr)
            return "RUN_ERROR", "", ""
        duration, throughput = parse_output(stdout)
        if duration is not None:
            return "SUCCESS", duration, throughput
        else:
            return "PARSE_ERROR", "", ""
    except FileNotFoundError:
        print(f"Error: Command '{MAIN_EXECUTABLE}' not found.", file=sys.stderr)
        current_process = None
        return "EXEC_ERROR", "", ""
    except subprocess.TimeoutExpired:
        if current_process:
            current_process.kill()
            current_process.communicate()
        current_process = None
        return "TIMEOUT", "", ""
    except Exception as e:
        print(f"An unexpected error occurred in single run: {e}", file=sys.stderr)
        if current_process and current_process.poll() is None:
            current_process.kill()
            current_process.wait()
        current_process = None
        return "UNKNOWN_ERROR", "", ""
    finally:
        current_process = None


# --- Main Script ---
def main():
    """
    Runs the benchmark suite, aiming for N non-timeout runs per config,
    and prints *every* attempt.
    """

    signal.signal(signal.SIGINT, signal_handler)
    check_executable(MAIN_EXECUTABLE)

    csv_writer = csv.writer(sys.stdout)
    # Header now includes Attempt number and Status
    header = [
        "JoinerType",
        "IndexType",
        "Workers",
        "Attempt",
        "Status",
        "Duration_ms",
        "Throughput_tuples_s",
    ]
    csv_writer.writerow(header)
    sys.stdout.flush()

    main_loop_order = ["joiner_type", "index_type", "workers"]
    max_attempts = NUM_ITERATIONS * MAX_ATTEMPTS_FACTOR

    for config in generate_configs(benchmark_params, main_loop_order):
        # Check for SOSD file once per config
        if config["stream_type"] == "sosd" and not os.path.exists(config["sosd_file"]):
            print(
                f"Error: SOSD file '{config['sosd_file']}' not found. Skipping config.",
                file=sys.stderr,
            )
            for i in range(1, NUM_ITERATIONS + 1):  # Print placeholders
                csv_writer.writerow(
                    [
                        config["joiner_type"],
                        config["index_type"],
                        config["workers"],
                        i,
                        "SOSD_FILE_MISSING",
                        "",
                        "",
                    ]
                )
            sys.stdout.flush()
            continue

        print(
            f"Running Config (Target {NUM_ITERATIONS} non-timeout runs): {config}",
            file=sys.stderr,
        )

        non_timeout_runs = 0
        attempts = 0

        # Loop until we get N non-timeout runs OR hit max attempts
        while non_timeout_runs < NUM_ITERATIONS and attempts < max_attempts:
            attempts += 1
            print(
                f"  Attempt {attempts} (Achieved {non_timeout_runs}/{NUM_ITERATIONS})...",
                file=sys.stderr,
            )

            # Run the test once
            status, duration, throughput = run_single_benchmark(config)

            # Only increment the counter if it wasn't a timeout
            if status != "TIMEOUT":
                # Write the result of this single iteration only if it was successful
                csv_writer.writerow(
                    [
                        config["joiner_type"],
                        config["index_type"],
                        config["workers"],
                        attempts,  # Attempt number
                        status,  # 'SUCCESS', 'TIMEOUT', etc.
                        duration,  # Will be '' on non-success
                        throughput,  # Will be '' on non-success
                    ]
                )
                sys.stdout.flush()
                non_timeout_runs += 1
            else:
                print(
                    f"    Timeout on attempt {attempts}. Will try again (if limits allow).",
                    file=sys.stderr,
                )

        if non_timeout_runs < NUM_ITERATIONS:
            print(
                f"  Warning: Only achieved {non_timeout_runs}/{NUM_ITERATIONS} non-timeout runs after {attempts} attempts.",
                file=sys.stderr,
            )

    print("\nBenchmark finished.", file=sys.stderr)


if __name__ == "__main__":
    main()
