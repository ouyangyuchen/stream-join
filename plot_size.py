import re
import matplotlib.pyplot as plt
import numpy as np
import argparse
import sys


def parse_log_file(file_path):
    """Parses the log file to extract window data."""
    data = {}
    join_pattern = re.compile(r"\[info\] Window (\d+) join count: (\d+)")
    size_pattern = re.compile(
        r"\[info\] Window (\d+) index r size: (\d+), index s size: (\d+)"
    )

    try:
        with open(file_path, "r") as f:
            for line in f:
                join_match = join_pattern.search(line)
                size_match = size_pattern.search(line)

                if join_match:
                    window_id = int(join_match.group(1))
                    join_count = int(join_match.group(2))
                    if window_id not in data:
                        data[window_id] = {}
                    data[window_id]["join_count"] = join_count

                if size_match:
                    window_id = int(size_match.group(1))
                    r_size = int(size_match.group(2))
                    s_size = int(size_match.group(3))
                    if window_id not in data:
                        data[window_id] = {}
                    data[window_id]["r_size"] = r_size
                    data[window_id]["s_size"] = s_size
    except FileNotFoundError:
        print(f"Error: Input file '{file_path}' not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file '{file_path}': {e}", file=sys.stderr)
        sys.exit(1)

    if not data:
        print(
            f"Error: No relevant window data found in '{file_path}'. Please check the file format.",
            file=sys.stderr,
        )
        sys.exit(1)

    return data


def create_plot(data, output_file):
    """Creates and saves the plot from the parsed data with the legend outside."""
    sorted_windows = sorted(data.keys())
    window_ids = np.array(sorted_windows)

    # Ensure all keys exist before accessing
    join_counts = [data[w].get("join_count", 0) for w in sorted_windows]
    r_sizes = [data[w].get("r_size", 0) for w in sorted_windows]
    s_sizes = [data[w].get("s_size", 0) for w in sorted_windows]

    fig, ax1 = plt.subplots(figsize=(12, 7))  # Increased height slightly for legend

    # Bar chart for index sizes (left y-axis)
    bar_width = 0.35
    index = np.arange(len(window_ids))

    bar1 = ax1.bar(
        index - bar_width / 2,
        r_sizes,
        bar_width,
        label="Index R Size",
        color="tab:blue",
        alpha=0.7,
    )
    bar2 = ax1.bar(
        index + bar_width / 2,
        s_sizes,
        bar_width,
        label="Index S Size",
        color="tab:green",
        alpha=0.7,
    )

    ax1.set_xlabel("Worker ID", fontsize=18)
    ax1.set_ylabel("Index Size", color="tab:blue", fontsize=18)
    ax1.tick_params("both", labelsize=16, labelcolor="tab:blue")
    ax1.set_xticks(index)
    ax1.set_xticklabels(window_ids)
    ax1.set_ylim(0, 600000)

    # Line chart for join result count (right y-axis)
    ax2 = ax1.twinx()
    line1 = ax2.plot(
        index,
        join_counts,
        color="tab:orange",
        marker="o",
        markersize=12,
        linestyle="-",
        linewidth=3,
        label="Join Result Count",
    )

    ax2.set_ylabel("Join Result Count", color="tab:orange", fontsize=18)
    ax2.tick_params(axis="y", labelcolor="tab:orange", labelsize=16)
    ax2.set_ylim(0, 7e9)

    # --- Legend Handling ---
    # Get handles and labels from both axes
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    all_lines = lines1 + lines2
    all_labels = labels1 + labels2

    # Place legend below the plot area
    # 'bbox_to_anchor' positions the legend. (0.5, -0.15) means:
    # 0.5 -> Horizontally centered relative to the axes.
    # -0.15 -> 15% below the bottom edge of the axes.
    # 'ncol' sets the number of columns in the legend.
    # 'loc' sets the anchor point on the legend box itself ('upper center' means the top-middle point of the legend box).
    fig.legend(
        all_lines,
        all_labels,
        loc="upper center",
        bbox_to_anchor=(0.5, 0.0),
        ncol=3,
        fontsize=18,
    )
    # --- End Legend Handling ---

    # Adjust layout to make space for the legend below the plot
    # Increase the 'bottom' value to create padding at the bottom.
    # plt.subplots_adjust(bottom=0.2)

    # Save the figure
    try:
        plt.savefig(output_file, bbox_inches="tight", dpi=300)
        print(f"Plot saved to '{output_file}'")
    except Exception as e:
        print(f"Error saving plot to '{output_file}': {e}", file=sys.stderr)
        sys.exit(1)

    plt.close()  # Close the plot to free memory


def main():
    """Main function to parse arguments and orchestrate plotting."""
    parser = argparse.ArgumentParser(
        description="Plot windowed join data from a log file."
    )
    parser.add_argument("input_file", help="Path to the input log file.")
    parser.add_argument(
        "-o",
        "--output_file",
        default="window_plot.png",
        help="Path to save the output plot image (default: window_plot.png).",
    )
    args = parser.parse_args()

    log_data = parse_log_file(args.input_file)
    create_plot(log_data, args.output_file)


if __name__ == "__main__":
    main()
