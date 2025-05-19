import pandas as pd
import re
import sys
import os
import matplotlib.pyplot as plt
import numpy as np

# Regex to capture window index, index r size, and index s size
index_size_pattern = re.compile(
    r"Window (\d+) index r size: (\d+), index s size: (\d+)"
)

# Regex to capture window index and join count
join_count_pattern = re.compile(r"Window (\d+) join count: (\d+)")

# Dictionary to store all data: {window_index: {'r_size': size_r, 's_size': size_s, 'join_count': count}}
window_data = {}

# State variable to track being inside the HandshakeJoiner test block
in_handshake_test = False

# --- Command-line Argument Handling ---
if len(sys.argv) != 3:
    print("Usage: python your_script_name.py <log_file_path> <output_plot_path>")
    print("Example: python plot_window_data.py log.txt window_data.png")
    sys.exit(1)  # Exit with an error code

log_file_path = sys.argv[1]
output_plot_path = sys.argv[2]

# Check if the input log file exists
if not os.path.exists(log_file_path):
    print(f"Error: Input log file not found at '{log_file_path}'")
    sys.exit(1)  # Exit with an error code

# --- File Reading and Processing ---
try:
    with open(log_file_path, "r") as log_file:
        # Read the file line by line
        for line in log_file:
            line = line.strip()  # Remove leading/trailing whitespace

            # Check if we are entering the HandshakeJoiner test block
            if line.startswith("[ RUN      ]"):
                if "WindowTest.HandshakeJoiner" in line:
                    in_handshake_test = True
                else:
                    # If we were in HandshakeJoiner and hit another RUN, we've exited it
                    if in_handshake_test:
                        in_handshake_test = False

            # Check if we are exiting the HandshakeJoiner test block
            # The index size and join count lines appear *before* the durations section or the OK line
            if in_handshake_test and (
                line.startswith("[       OK ]") or line == "--- Recorded Durations ---"
            ):
                in_handshake_test = False

            # If we are inside the HandshakeJoiner block, try to parse relevant lines
            if in_handshake_test:
                # Try to match index size line
                match_size = index_size_pattern.search(line)
                if match_size:
                    window_index = int(match_size.group(1))
                    r_size = int(match_size.group(2))
                    s_size = int(match_size.group(3))

                    # Initialize dictionary for this window if it doesn't exist
                    if window_index not in window_data:
                        window_data[window_index] = {}

                    # Store the sizes
                    window_data[window_index]["r_size"] = r_size
                    window_data[window_index]["s_size"] = s_size
                    continue  # Move to the next line

                # Try to match join count line
                match_count = join_count_pattern.search(line)
                if match_count:
                    window_index = int(match_count.group(1))
                    join_count = int(match_count.group(2))

                    # Initialize dictionary for this window if it doesn't exist
                    if window_index not in window_data:
                        window_data[window_index] = {}

                    # Store the join count
                    window_data[window_index]["join_count"] = join_count
                    continue  # Move to the next line


except Exception as e:
    print(f"An error occurred while reading or processing the file: {e}")
    sys.exit(1)  # Exit with an error code

# --- Data Organization and Output ---

if not window_data:
    print(
        f"No index size or join count data found for WindowTest.HandshakeJoiner in '{log_file_path}'."
    )
    sys.exit(0)  # Exit gracefully if no data

# Convert the dictionary to a pandas DataFrame
# The keys (window indices) become the DataFrame index
df_window_data = pd.DataFrame.from_dict(window_data, orient="index")

# Sort by index for better readability and plotting order
df_window_data = df_window_data.sort_index()

# Fill any potential missing values with 0 (e.g., if a window had size but no count line)
df_window_data = df_window_data.fillna(0)

print(
    f"Handshake Joiner Window Data (Index Sizes and Join Counts) from {log_file_path}:"
)
print(df_window_data)

# --- Plotting ---
print(f"\nGenerating plot and saving to '{output_plot_path}'...")

# Create figure and primary axes
fig, ax1 = plt.subplots(figsize=(12, 7))  # Adjust figure size as needed

# Create secondary axes sharing the same x-axis
ax2 = ax1.twinx()

# --- Plotting Bars (Index Sizes) on Primary Axis (ax1) ---
bar_width = 0.35
# Positions for the bars on the x-axis
x_positions = np.arange(len(df_window_data.index))

# Plot r_size bars
# Ensure 'r_size' column exists before plotting
if "r_size" in df_window_data.columns:
    rects1 = ax1.bar(
        x_positions - bar_width / 2,
        df_window_data["r_size"],
        bar_width,
        label="Index R Size",
        color="skyblue",
    )
    ax1.set_ylabel("Index Size", color="blue")
    ax1.tick_params(axis="y", labelcolor="blue")
else:
    print(
        "Warning: 'r_size' column not found in data. R size bars will not be plotted."
    )

# Plot s_size bars
# Ensure 's_size' column exists before plotting
if "s_size" in df_window_data.columns:
    rects2 = ax1.bar(
        x_positions + bar_width / 2,
        df_window_data["s_size"],
        bar_width,
        label="Index S Size",
        color="lightcoral",
    )
    # Only set ylabel and tick_params if r_size wasn't plotted (to avoid overwriting)
    if "r_size" not in df_window_data.columns:
        ax1.set_ylabel("Index Size", color="blue")
        ax1.tick_params(axis="y", labelcolor="blue")
else:
    print(
        "Warning: 's_size' column not found in data. S size bars will not be plotted."
    )

ax1.set_ylim(bottom=0)


# --- Plotting Line (Join Counts) on Secondary Axis (ax2) ---
# Ensure 'join_count' column exists before plotting
if "join_count" in df_window_data.columns:
    line1 = ax2.plot(
        x_positions,
        df_window_data["join_count"],
        color="forestgreen",
        marker="o",
        linestyle="-",
        label="Join Count",
    )
    # Set secondary y-axis label
    ax2.set_ylabel("Join Count", color="forestgreen")
    ax2.tick_params(axis="y", labelcolor="forestgreen")
    # Set the secondary y-axis to start from 0
    ax2.set_ylim(bottom=0)
else:
    print(
        "Warning: 'join_count' column not found in data. Join count line will not be plotted."
    )
    line1 = []  # Empty list if no join count data


# --- Common Plot Settings ---
ax1.set_xlabel("Window Index")
plt.title("Handshake Joiner Index Sizes and Join Counts per Window")

# Set the x-axis ticks to be the window indices
ax1.set_xticks(x_positions)
ax1.set_xticklabels(df_window_data.index)

# Add grid lines (optional, maybe only for the primary axis)
ax1.grid(axis="y", linestyle="--", alpha=0.7)
# ax2.grid(axis='y', linestyle=':', alpha=0.5) # Optional grid for secondary axis

# Combine legends from both axes
# Get handles and labels from both axes
handles1, labels1 = ax1.get_legend_handles_labels()
handles2, labels2 = ax2.get_legend_handles_labels()
# Combine them
ax2.legend(handles1 + handles2, labels1 + labels2, loc="upper left")


plt.tight_layout()  # Adjust layout

# --- Save the figure instead of showing ---
try:
    plt.savefig(output_plot_path)
    print(f"Plot successfully saved to '{output_plot_path}'")
except Exception as e:
    print(f"Error saving plot to '{output_plot_path}': {e}")
    sys.exit(1)

# Close the plot figure to free memory
plt.close(fig)
