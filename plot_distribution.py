import pandas as pd
import matplotlib.pyplot as plt
import argparse
import sys


def plot_throughput_chart(input_filename, output_filename):
    """
    Reads performance data from a CSV file, calculates average throughput,
    and generates a grouped bar chart.

    Args:
        input_filename (str): The path to the input CSV file.
        output_filename (str): The path to save the output plot image.
    """
    try:
        # Read the CSV data into a pandas DataFrame
        df = pd.read_csv(input_filename)

        # Calculate the average throughput for each Data_Source and IndexType
        avg_throughput = (
            df.groupby(["Data_Source", "IndexType"])["Throughput_tuples_s"]
            .mean()
            .unstack()
        )

        # Create the grouped bar chart
        ax = avg_throughput.plot(kind="bar", figsize=(12, 7), rot=0)

        # Set the title and labels
        ax.set_title("Average Throughput by Index Type and Data Source", fontsize=16)
        ax.set_xlabel("Index Type", fontsize=12)
        ax.set_ylabel("Average Throughput (tuples/s)", fontsize=12)
        ax.legend(title="Data Source")
        ax.tick_params(axis="x", labelsize=10)
        ax.tick_params(axis="y", labelsize=10)
        ax.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f"{int(x):,}")
        )  # Format y-axis labels

        # Add grid lines for better readability
        ax.yaxis.grid(True, linestyle="--", alpha=0.7)

        # Adjust layout to prevent labels from overlapping
        plt.tight_layout()

        # Save the figure to the specified output path
        plt.savefig(output_filename)
        print(f"Chart saved successfully to {output_filename}")

    except FileNotFoundError:
        print(f"Error: Input file '{input_filename}' not found.", file=sys.stderr)
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)


if __name__ == "__main__":
    # Set up the argument parser
    parser = argparse.ArgumentParser(
        description="Generate a grouped bar chart of average throughput from performance data."
    )
    parser.add_argument("input_filename", help="The path to the input CSV file.")
    parser.add_argument(
        "output_filename",
        help="The path to save the output plot image (e.g., plot.png).",
    )

    # Parse the arguments
    args = parser.parse_args()

    # Call the function to generate the plot
    plot_throughput_chart(args.input_filename, args.output_filename)
