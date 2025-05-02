import pandas as pd
import re
import sys
import os
import matplotlib.pyplot as plt

# Regex to capture window index, index r size, and index s size
# It looks for lines containing "Window X index r size: Y, index s size: Z"
index_size_pattern = re.compile(
    r"Window (\d+) index r size: (\d+), index s size: (\d+)"
)

# Dictionary to store index sizes: {window_index: {'r_size': size_r, 's_size': size_s}}
index_sizes = {}

# State variable to track being inside the HandshakeJoiner test block
in_handshake_test = False

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

            # Check if we are entering the HandshakeJoiner test block
            if line.startswith("[ RUN      ]"):
                if "WindowTest.HandshakeJoiner" in line:
                    in_handshake_test = True
                else:
                    # If we were in HandshakeJoiner and hit another RUN, we've exited it
                    if in_handshake_test:
                        in_handshake_test = False

            # Check if we are exiting the HandshakeJoiner test block
            # The index size lines appear *before* the durations section or the OK line
            if in_handshake_test and (
                line.startswith("[       OK ]") or line == "--- Recorded Durations ---"
            ):
                in_handshake_test = False

            # If we are inside the HandshakeJoiner block, try to parse index size lines
            if in_handshake_test:
                # Use search because the pattern is not at the very start of the line
                match = index_size_pattern.search(line)
                if match:
                    window_index = int(match.group(1))
                    r_size = int(match.group(2))
                    s_size = int(match.group(3))

                    # Store the sizes for this window index
                    # If a window index appears multiple times, this will store the last one found
                    index_sizes[window_index] = {"r_size": r_size, "s_size": s_size}

except Exception as e:
    print(f"An error occurred while reading or processing the file: {e}")
    sys.exit(1)  # Exit with an error code

# --- Data Organization and Output ---

if not index_sizes:
    print(
        f"No index size data found for WindowTest.HandshakeJoiner in '{log_file_path}'."
    )
    sys.exit(0)  # Exit gracefully if no data

# Convert the dictionary to a pandas DataFrame
# The keys (window indices) become the DataFrame index
df_sizes = pd.DataFrame.from_dict(index_sizes, orient="index")

# Sort by index for better readability and plotting order
df_sizes = df_sizes.sort_index()

print(f"Handshake Joiner Index Sizes by Window (from {log_file_path}):")
print(df_sizes)

# --- Plotting ---
print("\nGenerating plot...")

plt.figure(figsize=(10, 6))  # Optional: Adjust figure size

# Plot r_size and s_size against the window index (DataFrame index)
plt.plot(
    df_sizes.index, df_sizes["r_size"], marker="o", linestyle="-", label="Index R Size"
)
plt.plot(
    df_sizes.index, df_sizes["s_size"], marker="o", linestyle="-", label="Index S Size"
)

plt.yscale("log")  # Optional: Set y-axis to logarithmic scale for better visibility
plt.xlabel("Sub-Windows")
plt.ylabel("Index Size")
plt.title("Handshake Joiner Index Workload per Window")
plt.xticks(df_sizes.index)  # Ensure x-axis ticks are at each window index
plt.grid(True)
plt.legend()
plt.tight_layout()  # Adjust layout to prevent labels overlapping

# Show the plot
plt.show()
