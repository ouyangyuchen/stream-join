import re
import pandas as pd
import matplotlib.pyplot as plt
import argparse
import sys  # For stderr

# --- Updated Regex to extract data ---
# Groups: 1=JoinerType, 2=IndexType, 3=WorkerSize, 4=ThroughputValue, 5=ThroughputUnit (k or M)
# Changed: /workers:(\d+)/ instead of /(\d+)/
pattern = re.compile(
    r"BM_(\w+)Joiner<stream::(\w+)Index<.*?>"  # 1: Joiner, 2: Index
    r"/workers:(\d+)/real_time\s+"  # 3: Worker Size (Matches /workers:N/)
    r".*?"  # Skip timing info
    r"items_per_second=([\d.]+)([kM])/s"  # 4: Value, 5: Unit (k/M)
    r".*$"  # Match rest of the line
)


def parse_benchmark_file(filepath):
    """Reads a benchmark log file and parses relevant lines."""
    extracted_data = []
    print(f"--- Parsing Benchmark Log: {filepath} ---")
    try:
        with open(filepath, "r") as f_in:
            for i, line in enumerate(f_in):
                line = line.strip()  # Remove leading/trailing whitespace
                if not line or line.startswith(
                    "-"
                ):  # Skip empty lines and header lines
                    continue

                match = pattern.match(line)
                if match:
                    joiner_type = match.group(1)
                    index_type = match.group(2)
                    worker_size = int(match.group(3))
                    throughput_val = float(match.group(4))
                    throughput_unit = match.group(5)

                    # Convert throughput to items/second
                    if throughput_unit == "k":
                        throughput = throughput_val * 1000
                    elif throughput_unit == "M":
                        throughput = throughput_val * 1000000
                    else:
                        # Should not happen with k/M units, but handle defensively
                        throughput = throughput_val

                    # Store and print row
                    row = [joiner_type, index_type, worker_size, throughput]
                    extracted_data.append(row)
                    # Print parsed results to console
                    print(
                        f"Parsed: Joiner={joiner_type}, Index={index_type}, Workers={worker_size}, Throughput={throughput:.2f} items/s"
                    )
                else:
                    # Optionally print lines that didn't match for debugging
                    # print(f"Warning: Could not parse line {i+1}: {line}", file=sys.stderr)
                    pass  # Silently ignore non-matching lines

    except FileNotFoundError:
        print(f"Error: Input file not found: {filepath}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error reading or parsing file {filepath}: {e}", file=sys.stderr)
        return None

    print(
        f"--- Finished Parsing. Found {len(extracted_data)} valid benchmark results. ---"
    )
    return extracted_data


def plot_benchmark_data(data, plot_file):
    """Generates the plot from the parsed data."""
    if not data:
        print("No data available to plot.", file=sys.stderr)
        return

    print(f"\n--- Generating Plot: {plot_file} ---")

    try:
        # Create DataFrame from the list of lists
        df = pd.DataFrame(
            data, columns=["joiner_type", "index_type", "worker_size", "throughput"]
        )

        # Create the plot
        fig, ax = plt.subplots(figsize=(10, 6))  # Adjust figure size if needed

        # Group data by joiner and index type
        grouped = df.groupby(["joiner_type", "index_type"])

        # Define markers and colors (optional, for better distinction)
        markers = ["o", "s", "^", "D", "v", "<", ">", "p", "*", "h"]
        colors = plt.cm.tab10.colors  # Use a colormap
        marker_idx = 0
        color_idx = 0

        for name, group in grouped:
            joiner, index = name
            label = f"{joiner}-{index}"

            # Sort by worker_size for correct line plotting
            group = group.sort_values("worker_size")

            if len(group["worker_size"]) > 0:  # Only plot if there's data
                ax.plot(
                    group["worker_size"],
                    group["throughput"],
                    label=label,
                    marker=markers[marker_idx % len(markers)],
                    color=colors[color_idx % len(colors)],
                    linestyle="-",
                )
                marker_idx += 1
                color_idx += 1
            else:
                print(
                    f"Warning: No data points found for group {label}", file=sys.stderr
                )

        # Customize the plot
        ax.set_xlabel("Number of Workers")
        ax.set_ylabel("Throughput (items/second)")
        ax.set_title("Benchmark Throughput vs. Worker Size")

        # Ensure ticks cover all unique worker sizes present in the data
        unique_workers = sorted(df["worker_size"].unique())
        if unique_workers:
            # Use integers for x-axis ticks if they are integers
            ax.set_xticks(unique_workers)
            ax.set_xticklabels(
                [str(w) for w in unique_workers]
            )  # Ensure they are displayed as strings
        else:
            print(
                "Warning: No worker size data found for x-axis ticks.", file=sys.stderr
            )

        ax.legend(title="Joiner-Index")
        ax.grid(True, which="both", linestyle="--", linewidth=0.5)
        # Use scientific notation for y-axis if numbers are large
        ax.ticklabel_format(style="sci", axis="y", scilimits=(0, 0))

        # Optional: Use log scale for y-axis if throughput varies greatly
        # ax.set_yscale('log')
        # ax.set_ylabel("Throughput (items/second, log scale)")

        plt.tight_layout()  # Adjust layout to prevent labels overlapping
        plt.savefig(plot_file)  # Save the plot to a file
        print(f"Plot saved successfully as '{plot_file}'")
        # plt.show() # Uncomment to display the plot interactively

    except Exception as e:
        print(f"Error during plotting: {e}", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse benchmark log file and plot throughput vs worker size."
    )
    parser.add_argument("input_logfile", help="Path to the benchmark output log file.")
    parser.add_argument(
        "-o",
        "--output_plot",
        default="benchmark_throughput.png",
        help="Filename for the output plot image (default: benchmark_throughput.png)",
    )

    args = parser.parse_args()

    # Parse the data from the specified log file
    parsed_data = parse_benchmark_file(args.input_logfile)

    # If parsing was successful and returned data, plot it
    if parsed_data:
        plot_benchmark_data(parsed_data, args.output_plot)
    else:
        print("Exiting due to parsing errors or no data found.", file=sys.stderr)
        sys.exit(1)  # Exit with an error code
