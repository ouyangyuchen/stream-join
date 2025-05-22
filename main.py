import yaml
import subprocess
import signal
import sys
import os

# Default path to the C++ executable
DEFAULT_CPP_EXECUTABLE = "./main"


def build_cpp_args(config_data):
    """Builds a list of command-line arguments for the C++ program from config data."""
    args = []
    arg_map = {
        "window_size": "--window_size",
        "diff": "--diff",
        "tuples_r": "--tuples_r",
        "tuples_s": "--tuples_s",
        "channel_buffer_size": "--channel_buffer_size",
        "workers": "--workers",
        "joiner_type": "--joiner_type",
        "index_type": "--index_type",
        "stream_type": "--stream_type",
        "preload": "--preload",
        "watcher_enabled": "--watcher_enabled",
        "watcher_interval": "--watcher_interval",
        "key_low": "--key_low",
        "key_high": "--key_high",
        "seq_start": "--seq_start",
        "seq_step": "--seq_step",
    }

    for yaml_key, cli_flag in arg_map.items():
        if yaml_key in config_data:
            value = config_data[yaml_key]
            # Special handling for boolean watcher_enabled
            if yaml_key == "watcher_enabled" and isinstance(value, bool):
                args.extend([cli_flag, "1" if value else "0"])
            else:
                args.extend([cli_flag, str(value)])
    return args


def run_cpp_program(config_path, executable_path):
    """
    Parses the YAML config, runs the C++ program, and prints its output.
    """
    if not os.path.exists(executable_path):
        print(f"Error: C++ executable not found at '{executable_path}'")
        print(
            f"Please compile your C++ program (e.g., to '{DEFAULT_CPP_EXECUTABLE}') or provide the correct path."
        )
        sys.exit(1)

    if not os.path.exists(config_path):
        print(f"Error: Configuration file not found at '{config_path}'")
        sys.exit(1)

    try:
        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML configuration file '{config_path}': {e}")
        sys.exit(1)
    except IOError as e:
        print(f"Error reading configuration file '{config_path}': {e}")
        sys.exit(1)

    if not config_data:
        print(f"Warning: Configuration file '{config_path}' is empty or invalid.")
        cpp_args = []
    else:
        cpp_args = build_cpp_args(config_data)

    command = [executable_path] + cpp_args

    print(f"Running C++ program with command: {' '.join(command)}")
    print("--- C++ Program Output ---")
    print("\n")
    try:
        # Run the C++ program and capture its output
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )

        # Stream output line by line
        if process.stdout:
            for line in process.stdout:
                print(line, end="")

        process.wait()  # Wait for the process to complete

        if process.returncode != 0:
            print(f"\n--- C++ Program exited with error code: {process.returncode} ---")

    except KeyboardInterrupt:
        print(
            "\n--- Keyboard interrupt received, attempting to terminate C++ program ---"
        )
        if process and process.poll() is None:  # Check if process exists and is running
            print("Sending SIGINT to C++ process...")
            process.send_signal(signal.SIGINT)
            try:
                # Wait for a short period to allow graceful shutdown
                process.wait(timeout=3)  # seconds
                print("--- C++ Program terminated after SIGINT ---")
            except subprocess.TimeoutExpired:
                print(
                    "C++ process did not terminate with SIGINT in time. Sending SIGKILL..."
                )
                process.kill()  # Force kill if it doesn't respond to SIGINT
                process.wait()  # Ensure it's reaped
                print("--- C++ Program terminated by SIGKILL ---")
            except Exception as e_terminate:
                print(
                    f"An error occurred while trying to terminate the C++ program: {e_terminate}"
                )
        else:
            print(
                "--- C++ Program was not running or already terminated when interrupt occurred ---"
            )
        sys.exit(130)  # Standard exit code for process interrupted by Ctrl+C

    except FileNotFoundError:
        print(
            f"Error: Executable '{executable_path}' not found. Make sure it's in your PATH or provide the full path."
        )
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred while running the C++ program: {e}")
        sys.exit(1)
    finally:
        print("--- End of C++ Program Output ---")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            f"Usage: python {sys.argv[0]} <path_to_config.yml> [path_to_cpp_executable]"
        )
        print(f"Example: python {sys.argv[0]} config.yml")
        print(f"Example: python {sys.argv[0]} my_config.yml ./bin/my_joiner_app")
        sys.exit(1)

    config_file_path = sys.argv[1]
    cpp_executable = DEFAULT_CPP_EXECUTABLE
    if len(sys.argv) > 2:
        cpp_executable = sys.argv[2]

    run_cpp_program(config_file_path, cpp_executable)
