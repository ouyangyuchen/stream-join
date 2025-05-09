import pandas as pd
import re
import sys
import os
import matplotlib.pyplot as plt
import numpy as np


def plot_timing_breakdown(df: pd.DataFrame, title: str):
    """
    Plots the timing breakdown for each window using a stacked bar chart.

    Args:
        df: pandas DataFrame with window index as index and timing columns
            (e.g., 'Loop', 'ProcessLeft', 'ProcessRight', 'Communication (others)').
            The DataFrame should contain columns representing components that sum up
            to the 'Loop' time (or are intended to be stacked).
        title: Title for the plot (e.g., "Handshake Joiner Timing Breakdown").
    """
    if df.empty:
        print(f"No data to plot for {title}.")
        return

    potential_components = [
        "ProcessLeft",
        "ProcessRight",
        "ForwardTuples",
        "FlushPendings",
        "ProcessR",
        "ProcessS",
        "Communication",
    ]
    component_cols = [col for col in potential_components if col in df.columns]

    if "Loop" not in df.columns:
        print(
            f"Warning: 'Loop' column not found in DataFrame for {title}. Plotting sum of components instead of verifying against Loop."
        )
        if not component_cols:
            print(
                f"Error: No 'Loop' or component columns found in DataFrame for {title}. Cannot plot."
            )
            return
    else:
        if not component_cols:
            print(
                f"Warning: 'Loop' column found, but no standard component columns ({potential_components}) in DataFrame for {title}. Cannot plot breakdown."
            )
            # Optionally plot just the Loop total if no components
            plt.figure(figsize=(12, 7))
            df["Loop"].plot(kind="bar")
            plt.xlabel("Window Index")
            plt.ylabel("Total Loop Time (ms)")
            plt.title(f"{title} - Total Loop Time")
            plt.xticks(rotation=0)
            plt.grid(axis="y")
            plt.tight_layout()
            plt.show()
            return

    window_indices = df.index
    x_positions = np.arange(len(window_indices))

    plt.figure(figsize=(12, 7))  # Adjust figure size as needed

    bottom_values = np.zeros(len(window_indices))

    # Plot stacked bars for each component column
    for col in component_cols:
        # Use x_positions for bar placement, and window_indices for labels
        plt.bar(x_positions, df[col], bottom=bottom_values, label=col)
        bottom_values += df[col]  # Update bottom for the next stack

    # Optional: Plot the 'Loop' total as a line on top for comparison
    # This helps visually verify that the stacked bars sum up correctly to the Loop time
    # Make sure 'Loop' column exists before attempting to plot it
    if "Loop" in df.columns:
        plt.plot(
            x_positions,
            df["Loop"],
            color="black",
            linestyle="--",
            marker="x",
            label="Loop (Total)",
        )

    plt.xlabel("Window Index")
    plt.ylabel("Time (ms)")
    plt.title(title)
    # Set the x-axis ticks to be the window indices
    plt.xticks(x_positions, window_indices, rotation=0)
    plt.grid(axis="y")  # Add horizontal grid lines
    plt.legend(title="Metric")
    plt.tight_layout()  # Adjust layout

    plt.show()


log_pattern = re.compile(r"(Handshake|BroadcastWindow): ([^\[]+)\[(\d+)\]: (\d+) ms")

# Dictionary to store aggregated data: {joiner_type: {index: {metric_name: total_duration}}}
aggregated_data = {}

# State variable to track being inside the durations section
in_durations_section = False

# --- Command-line Argument Handling ---
if len(sys.argv) != 2:
    print("Usage: python your_script_name.py <log_file_path>")
    sys.exit(1)  # Exit with an error code

log_file_path = sys.argv[1]

# Check if the file exists
if not os.path.exists(log_file_path):
    print(f"Error: File not found at '{log_file_path}'")
    sys.exit(1)  # Exit with an error code

# --- File Reading and Processing ---
try:
    with open(log_file_path, "r") as log_file:
        # Read the file line by line
        for line in log_file:
            line = line.strip()  # Remove leading/trailing whitespace

            # Check if we are entering or exiting the durations section
            if line == "--- Recorded Durations ---":
                in_durations_section = True
                continue  # Skip this line
            elif line == "--------------------------":
                in_durations_section = False
                continue  # Skip this line

            # If we are in the correct section, try to parse the line
            if in_durations_section:
                match = log_pattern.match(line)
                if match:
                    joiner_type = match.group(
                        1
                    )  # e.g., "Handshake" or "BroadcastWindow"
                    metric_name = match.group(2).strip()  # e.g., "ProcessRight"
                    index = int(match.group(3))  # e.g., 1
                    duration = int(match.group(4))  # e.g., 246

                    # Aggregate the duration for the specific joiner type, index, and metric
                    if joiner_type not in aggregated_data:
                        aggregated_data[joiner_type] = {}
                    if index not in aggregated_data[joiner_type]:
                        aggregated_data[joiner_type][index] = {}
                    if metric_name not in aggregated_data[joiner_type][index]:
                        aggregated_data[joiner_type][index][metric_name] = 0

                    aggregated_data[joiner_type][index][metric_name] += duration

except Exception as e:
    print(f"An error occurred while reading or processing the file: {e}")
    sys.exit(1)  # Exit with an error code

# --- Data Processing and Calculation for Each Joiner Type ---

for joiner_type, index_data in aggregated_data.items():
    print(f"\n--- {joiner_type} Statistics by Index (from {log_file_path}) ---")

    # Convert the aggregated data for this joiner type into a pandas DataFrame
    df = pd.DataFrame.from_dict(index_data, orient="index")

    # Define calculation columns and formula based on joiner type
    if joiner_type == "Handshake":
        calculation_cols = [
            "ProcessLeft",
            "ProcessRight",
            "ForwardTuples",
            "FlushPendings",
        ]
        # Formula: Communication = Loop - (ProcessLeft + ProcessRight + ForwardTuples + FlushPendings)
    elif joiner_type == "BroadcastWindow":
        calculation_cols = ["ProcessR", "ProcessS"]
        # Formula: Communication = Loop - (ProcessR + ProcessS)
    else:
        # Should not happen with the current regex, but good practice
        print(f"Warning: Unknown joiner type '{joiner_type}'. Skipping calculation.")
        calculation_cols = []  # No calculation possible

    # Ensure 'Loop' and calculation columns exist, filling missing ones with 0
    cols_to_check = ["Loop"] + calculation_cols
    for col in cols_to_check:
        if col not in df.columns:
            df[col] = 0

    # Fill any NaN values in the relevant columns with 0 before calculation
    df[cols_to_check] = df[cols_to_check].fillna(0)

    # Perform the calculation if calculation columns were defined
    if calculation_cols:
        df["Communication"] = df["Loop"] - df[calculation_cols].sum(axis=1)

        output_cols = ["Loop"] + calculation_cols + ["Communication"]
        other_cols = [col for col in df.columns if col not in output_cols]
        full_cols = [col for col in output_cols if col in df.columns] + other_cols

        df = df[output_cols]

    # Sort by index for better readability (optional)
    df = df.sort_index()

    # Print the resulting DataFrame
    print(df)
    print("-" * (len(joiner_type) + 20))  # Separator

    plot_timing_breakdown(df, f"{joiner_type} Timing Breakdown")

# If no data was found for any joiner type
if not aggregated_data:
    print(
        f"No Handshake or BroadcastWindow timing data found in '{log_file_path}' within the '--- Recorded Durations ---' section."
    )
