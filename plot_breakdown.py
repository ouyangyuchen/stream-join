import re
import pandas as pd
import matplotlib.pyplot as plt
import argparse


def parse_log_file(filename):
    """
    Parses the C++ program output file to extract recorded durations.

    Args:
        filename (str): The path to the input log file.

    Returns:
        pandas.DataFrame: A DataFrame containing window ID, operation type,
                          and time, or None if parsing fails.
    """
    data = []
    # Regex to capture the ID, operation, and time
    line_pattern = re.compile(r"^\s*(\d+)\s+([\w_]+):\s+(\d+)\s+ms")

    try:
        with open(filename, "r") as f:
            in_durations_section = False
            for line in f:
                line = line.strip()
                if line == "--- Recorded Durations ---":
                    in_durations_section = True
                    continue
                if line == "--------------------------":
                    break

                if in_durations_section:
                    match = line_pattern.match(line)
                    if match:
                        window_id = int(match.group(1))
                        operation = match.group(2)
                        time = int(match.group(3))
                        data.append(
                            {
                                "window_id": window_id,
                                "operation": operation,
                                "time": time,
                            }
                        )

    except FileNotFoundError:
        print(f"Error: Input file '{filename}' not found.")
        return None
    except Exception as e:
        print(f"An error occurred during file parsing: {e}")
        return None

    if not data:
        print("No duration data found in the specified format.")
        return None

    return pd.DataFrame(data)


def plot_window_times(df, output_filename):
    """
    Plots the time spent by each window as a stacked bar chart.

    Args:
        df (pandas.DataFrame): DataFrame with window data.
        output_filename (str): The path to save the output plot image.
    """
    if df is None:
        return

    # Group by window_id and operation, summing the times.
    # This handles cases where the same operation might appear multiple times
    # for the same window_id (though unlikely based on the example).
    grouped = df.groupby(["window_id", "operation"])["time"].sum().unstack(fill_value=0)

    # Sort by window_id to ensure the x-axis is ordered
    grouped = grouped.sort_index()

    # Create the stacked bar chart
    ax = grouped.plot(kind="bar", stacked=True, figsize=(15, 7))

    # Set labels and title
    ax.set_xlabel("Window ID")
    ax.set_ylabel("Time (ms)")
    ax.set_title("Time Spent per Window ID (Stacked by Operation)")

    # Rotate x-axis labels if there are many windows
    if len(grouped.index) > 20:
        plt.xticks(rotation=90, fontsize=8)
    else:
        plt.xticks(rotation=0)

    # Add legend
    ax.legend(title="Operation", bbox_to_anchor=(1.05, 1), loc="upper left")

    # Adjust layout to prevent legend overlap
    plt.tight_layout(rect=[0, 0, 0.85, 1])

    # Save the figure
    try:
        plt.savefig(output_filename, bbox_inches="tight")
        print(f"Plot saved successfully to '{output_filename}'")
    except Exception as e:
        print(f"Error saving plot: {e}")

    # Optionally display the plot
    # plt.show()


def main():
    """
    Main function to parse arguments and orchestrate parsing and plotting.
    """
    parser = argparse.ArgumentParser(
        description="Plot window times from C++ program output."
    )
    parser.add_argument(
        "input_filename", help="The path to the input C++ program output file."
    )
    parser.add_argument(
        "output_filename",
        help="The path to save the output plot image (e.g., plot.png).",
    )
    args = parser.parse_args()

    # Parse the log file
    data_df = parse_log_file(args.input_filename)

    # Plot the data
    if data_df is not None:
        plot_window_times(data_df, args.output_filename)


if __name__ == "__main__":
    main()
