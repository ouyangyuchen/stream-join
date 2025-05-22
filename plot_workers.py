import pandas as pd
import re
import sys
import os
import matplotlib.pyplot as plt
import argparse
import numpy as np


def parse_benchmark_results(file_content):
    """
    Parses benchmark results from a string content, handling both
    BroadcastJoiner and HandshakeJoiner formats.

    Args:
        file_content (str): The content of the benchmark file.

    Returns:
        pd.DataFrame: A DataFrame containing the parsed data with columns:
                      'joiner type', 'index type', 'worker count', 'time used (ms)',
                      'per window throughput (tuples/s)', 'end to end throughput (tuples/s)'.
                      'per window throughput' will be NaN for HandshakeJoiner results.
                      Returns an empty DataFrame if no results are found.
    """
    # Updated regex pattern to match the benchmark format
    pattern = re.compile(
        r"^(\w+(?:\s+\w+)*)\s+(\w+)/(\d+)w\s*:\s*([\d.]+)\s*ms\s*\|\s*(?:(\d+\.\d+)\s*tuples/s \(per window\)\s*\|\s*)?(\d+\.\d+)\s*tuples/s \(end to end\).*$"
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
            time_used_ms = float(match.group(4))  # Time in milliseconds (ms)

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
                    "time used (ms)": time_used_ms,
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
    using consistent line attributes, and saves or shows the plot.
    Formats 'BPlusTreeIndex' as 'B+ Tree' in the legend.

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

    # Filter out rows where the specified throughput column is NaN
    df_plot = df.dropna(subset=[throughput_column]).copy()

    if df_plot.empty:
        print(f"No valid data points for '{throughput_column}' to plot.")
        return

    # Define consistent attributes based on joiner and index type
    line_styles = {"BroadcastJoiner": "-", "HandshakeJoiner": "--"}
    colors = {"ListIndex": "tab:blue", "BPlusTreeIndex": "tab:orange"}
    markers = {"ListIndex": "o", "BPlusTreeIndex": "s"}
    index_type_legend_names = {"ListIndex": "List", "BPlusTreeIndex": "B+ Tree"}

    unique_combinations = (
        df_plot[["joiner type", "index type"]].drop_duplicates().values
    )

    fig, ax = plt.subplots(figsize=(10, 6))

    for joiner_type, index_type in unique_combinations:
        df_subset = df_plot[
            (df_plot["joiner type"] == joiner_type)
            & (df_plot["index type"] == index_type)
        ].sort_values(by="worker count")

        style = line_styles.get(joiner_type, "-")
        color = colors.get(index_type, "gray")
        marker = markers.get(index_type, ".")
        index_legend_name = index_type_legend_names.get(index_type, index_type)
        label = f"{joiner_type}({index_legend_name})"

        ax.plot(
            df_subset["worker count"],
            df_subset[throughput_column],
            marker=marker,
            linestyle=style,
            color=color,
            label=label,
        )

    ax.set_xlabel("Worker Count")
    ax.set_ylabel(throughput_column)
    ax.set_title(f"{throughput_column.replace(' (tuples/s)', '')} vs. Worker Count")
    ax.legend(title="Join Type (Index Type)")
    ax.grid(True, linestyle="--", alpha=0.6)
    ax.xaxis.get_major_locator().set_params(integer=True)
    plt.tight_layout()

    if output_filename:
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        plt.savefig(output_filename)
        print(f"Plot saved to '{output_filename}'")
        plt.close(fig)
    else:
        plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse benchmark results and plot throughput vs. worker count."
    )
    parser.add_argument("input_file", help="Path to the benchmark results file.")
    parser.add_argument(
        "-o",
        "--output",
        nargs="*",
        metavar="OUTPUT_FILE",
        help="Output filenames for the plots. If not provided, plots are displayed.",
    )

    args = parser.parse_args()
    file_path = args.input_file

    if not os.path.exists(file_path):
        print(f"Error: Input file not found at '{file_path}'")
        sys.exit(1)

    try:
        with open(file_path, "r") as f:
            file_content = f.read()
    except Exception as e:
        print(f"Error reading file '{file_path}': {e}")
        sys.exit(1)

    df_benchmark = parse_benchmark_results(file_content)

    if df_benchmark.empty:
        print("No benchmark results found in the file.")
        sys.exit(0)

    unique_joiners = df_benchmark["joiner type"].unique()
    per_window_col = "per window throughput (tuples/s)"
    e2e_col = "end to end throughput (tuples/s)"
    plots_info = []

    if len(unique_joiners) == 1:
        joiner = unique_joiners[0]
        if joiner == "BroadcastJoiner":
            plots_info = [
                ("Per Window Throughput", per_window_col),
                ("End to End Throughput", e2e_col),
            ]
        elif joiner == "HandshakeJoiner":
            plots_info = [("End to End Throughput", e2e_col)]
    elif len(unique_joiners) > 1:
        plots_info = [("Combined End to End Throughput", e2e_col)]

    output_files = args.output or [None] * len(plots_info)

    if len(output_files) != len(plots_info):
        print(
            f"Error: Expected {len(plots_info)} output filenames, but got {len(output_files)}."
        )
        sys.exit(1)

    for i, (plot_type_name, column_name) in enumerate(plots_info):
        print(f"Generating plot: {plot_type_name}")
        plot_throughput_by_workers(
            df_benchmark, column_name, output_filename=output_files[i]
        )
