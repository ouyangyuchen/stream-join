import pandas as pd
import re
import sys
import os
import matplotlib.pyplot as plt
import argparse
import numpy as np  # Import numpy for NaN


def parse_benchmark_results(file_content):
    """
    Parses benchmark results from a string content, handling both
    BroadcastJoiner and HandshakeJoiner formats.

    Args:
        file_content (str): The content of the benchmark file.

    Returns:
        pd.DataFrame: A DataFrame containing the parsed data with columns:
                      'joiner type', 'index type', 'worker count', 'time used (us)',
                      'per window throughput (tuples/s)', 'end to end throughput (tuples/s)'.
                      'per window throughput' will be NaN for HandshakeJoiner results.
                      Returns an empty DataFrame if no results are found.
    """
    # Regex to capture fields from either format.
    # It optionally captures the per-window throughput part.
    #
    # Group 1: Joiner Type (e.g., BroadcastJoiner, HandshakeJoiner)
    # Group 2: Index Type (e.g., ListIndex, BPlusTreeIndex)
    # Group 3: Worker Count (integer)
    # Group 4: Time Used (integer) - Note: unit is 'us' or 'μs', we'll store as 'us'
    # Group 5: Per Window Throughput (float) - This group is optional
    # Group 6: End to End Throughput (float) - This group is always present
    pattern = re.compile(
        r"^(\w+(?:\s+\w+)*)\s+(\w+)/(\d+)w\s*:\s*(\d+)\s*(?:μs|us)\s*\|\s*(?:(\d+\.\d+)\s*tuples/s \(per window\)\s*\|\s*)?(\d+\.\d+)\s*tuples/s \(end to end\).*$"
    )

    data = []

    # Process the content line by line
    for line in file_content.splitlines():
        line = line.strip()  # Remove leading/trailing whitespace
        match = pattern.match(line)
        if match:
            # Extract captured groups
            joiner_type = match.group(1)
            index_type = match.group(2)  # Part before '/'
            worker_count = int(match.group(3))  # The number between '/' and 'w'
            time_used_us = int(match.group(4))  # Time in microseconds (us or μs)

            # Group 5 is the optional per-window throughput
            per_window_throughput_str = match.group(5)
            # Group 6 is the end-to-end throughput
            end_to_end_throughput_str = match.group(6)

            # Convert throughputs, handling the optional per-window field
            per_window_throughput = (
                float(per_window_throughput_str)
                if per_window_throughput_str
                else np.nan
            )
            end_to_end_throughput = float(end_to_end_throughput_str)

            # Append data as a dictionary
            data.append(
                {
                    "joiner type": joiner_type,
                    "index type": index_type,
                    "worker count": worker_count,
                    "time used (us)": time_used_us,
                    "per window throughput (tuples/s)": per_window_throughput,
                    "end to end throughput (tuples/s)": end_to_end_throughput,
                }
            )

    # Create DataFrame from the list of dictionaries
    df = pd.DataFrame(data)

    return df


def plot_throughput_by_workers(df, throughput_column, output_filename=None):
    """
    Plots throughput vs. worker count for different joiner/index type combinations
    and saves or shows the plot.

    Args:
        df (pd.DataFrame): The DataFrame containing benchmark results.
        throughput_column (str): The name of the throughput column to plot
                                 ('per window throughput (tuples/s)' or
                                  'end to end throughput (tuples/s)').
        output_filename (str, optional): The path to save the plot image.
                                         If None, the plot is displayed.
                                         Defaults to None.
    """
    if df.empty:
        print("DataFrame is empty, cannot plot.")
        return

    if throughput_column not in df.columns:
        print(f"Error: Column '{throughput_column}' not found in DataFrame.")
        return

    # Filter out rows where the specified throughput column is NaN (e.g., per-window for Handshake)
    df_plot = df.dropna(subset=[throughput_column]).copy()

    if df_plot.empty:
        print(f"No valid data points for '{throughput_column}' to plot.")
        return

    # Get unique combinations of joiner type and index type
    unique_combinations = (
        df_plot[["joiner type", "index type"]].drop_duplicates().values
    )

    if len(unique_combinations) < 1:
        print("No joiner/index type combinations found in data, cannot plot.")
        return

    # Create a figure and axes for the plot
    fig, ax = plt.subplots(figsize=(10, 6))  # Adjust figure size as needed

    # Plot data for each unique combination
    for joiner_type, index_type in unique_combinations:
        # Filter data for the current combination
        df_subset = df_plot[
            (df_plot["joiner type"] == joiner_type)
            & (df_plot["index type"] == index_type)
        ].copy()

        # Sort by worker count to ensure the line plot is correct
        df_subset = df_subset.sort_values(by="worker count")

        # Create a clear label for the legend
        label = f"{joiner_type} {index_type}"

        # Plot the line
        ax.plot(
            df_subset["worker count"],
            df_subset[throughput_column],
            marker="o",
            linestyle="-",
            label=label,
        )

    # Add plot labels and title
    ax.set_xlabel("Worker Count")
    ax.set_ylabel(throughput_column)
    # Format y-axis to show k tuples/s instead of raw values
    ax.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, _: f"{x / 1000:.1f}k" if x >= 1000 else f"{x:.1f}")
    )
    title_throughput_name = throughput_column.replace(" (tuples/s)", "")
    ax.set_title(f"{title_throughput_name} vs. Worker Count by Joiner and Index Type")

    # Add a legend to identify the lines
    ax.legend(title="Joiner/Index Type")

    # Add a grid for better readability
    ax.grid(True, linestyle="--", alpha=0.6)

    # Ensure x-axis ticks are integers if worker count is always integer
    ax.xaxis.get_major_locator().set_params(integer=True)

    # Improve layout
    plt.tight_layout()

    # Save or show the plot based on output_filename
    if output_filename:
        try:
            # Ensure the directory exists if specified in the path
            output_dir = os.path.dirname(output_filename)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
                print(f"Created directory: {output_dir}")

            plt.savefig(output_filename)
            print(f"Plot saved to '{output_filename}'")
        except Exception as e:
            print(f"Error saving plot to '{output_filename}': {e}")
        finally:
            plt.close(fig)  # Close the figure to free memory
    else:
        plt.show()
        # plt.close(fig) # Keep figure open if showing, user closes it


# --- Main Execution Block ---
if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(
        description="Parse benchmark results (Broadcast and Handshake) and plot throughput vs. worker count.",
        formatter_class=argparse.RawTextHelpFormatter,  # Helps with multiline help
    )
    parser.add_argument("input_file", help="Path to the benchmark results file.")
    parser.add_argument(
        "-o",
        "--output",
        nargs="*",  # Accept zero or more filenames
        metavar="OUTPUT_FILE",
        help="""Output filenames for the plots.
The number of required filenames depends on the input file content:
- Only BroadcastJoiner: 2 filenames (for per-window and end-to-end plots)
- Only HandshakeJoiner: 1 filename (for end-to-end plot)
- Both Joiners:         1 filename (for combined end-to-end plot)
If not provided, plots are displayed interactively.""",
    )

    # Parse command-line arguments
    args = parser.parse_args()

    file_path = args.input_file

    # Check if the input file exists
    if not os.path.exists(file_path):
        print(f"Error: Input file not found at '{file_path}'")
        sys.exit(1)

    # Read the content of the file
    try:
        with open(file_path, "r") as f:
            file_content = f.read()
    except Exception as e:
        print(f"Error reading file '{file_path}': {e}")
        sys.exit(1)

    # Call the parsing function
    df_benchmark = parse_benchmark_results(file_content)

    # Print the resulting DataFrame (optional)
    print("--- Parsed Data ---")
    print(df_benchmark)
    print("-" * 20)

    if df_benchmark.empty:
        print("No benchmark results found in the file.")
        sys.exit(0)  # Exit successfully, but indicate no data

    # Determine which joiner types are present
    unique_joiners = df_benchmark["joiner type"].unique()

    # Define column names
    per_window_col = "per window throughput (tuples/s)"
    e2e_col = "end to end throughput (tuples/s)"

    # Determine which plots to generate and how many output files are needed
    plots_info = []  # List of (plot_type_name, column_name) tuples
    required_outputs = 0

    if len(unique_joiners) == 1:
        joiner = unique_joiners[0]
        if joiner == "BroadcastJoiner":
            print(
                "Detected only BroadcastJoiner. Will plot per-window and end-to-end throughput."
            )
            plots_info = [
                ("Per Window Throughput", per_window_col),
                ("End to End Throughput", e2e_col),
            ]
            required_outputs = 2
        elif joiner == "HandshakeJoiner":
            print("Detected only HandshakeJoiner. Will plot end-to-end throughput.")
            plots_info = [("End to End Throughput", e2e_col)]
            required_outputs = 1
        else:
            print(
                f"Warning: Detected unknown joiner type '{joiner}'. Cannot determine plots."
            )
            sys.exit(1)  # Exit with error for unknown type
    elif len(unique_joiners) > 1:
        print(
            f"Detected multiple joiner types: {list(unique_joiners)}. Will plot combined end-to-end throughput."
        )
        plots_info = [("Combined End to End Throughput", e2e_col)]
        required_outputs = 1
    else:
        # This case should be caught by the empty DataFrame check earlier, but included for completeness
        print("No joiner types detected in the parsed data.")
        sys.exit(0)

    # Validate output filenames based on required_outputs
    output_files = args.output  # This is a list or None

    if output_files is not None:  # If -o was used
        if len(output_files) != required_outputs:
            print(
                f"\nError: Expected {required_outputs} output filename(s) for the detected content, but received {len(output_files)}."
            )
            if required_outputs == 1:
                print("Usage: python your_script_name.py <input_file> -o <output_file>")
            elif required_outputs == 2:
                print(
                    "Usage: python your_script_name.py <input_file> -o <per_window_file> <e2e_file>"
                )
            sys.exit(1)
    else:  # If -o was NOT used, set output filenames to None for plotting function
        output_files = [None] * required_outputs

    # Generate plots based on determined info
    for i, (plot_type_name, column_name) in enumerate(plots_info):
        output_file = (
            output_files[i] if output_files else None
        )  # Get the corresponding output file name

        print(f"\nGenerating plot: {plot_type_name}")
        plot_throughput_by_workers(
            df_benchmark, column_name, output_filename=output_file
        )

    # If plots were shown interactively, keep the script alive until they are closed
    if output_files is None or all(f is None for f in output_files):
        print("\nDisplaying plots. Close plot window(s) to exit.")
        # plt.show() # plot_throughput_by_workers already calls show if output_filename is None
