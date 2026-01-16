import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Paths
results_dir = Path("benchmarks/results")
results_dir.mkdir(parents=True, exist_ok=True)

csv_path = results_dir / "scaling.csv"

# Load benchmark results
df = pd.read_csv(csv_path)

# Compute throughput (requests per second)
df["throughput_rps"] = df["successful_requests"] / df["duration_s"]

# Plot 1: Average latency vs duration
plt.figure()
plt.plot(df["duration_s"], df["avg_latency_ms"], marker="o")
plt.xlabel("Test duration (seconds)")
plt.ylabel("Average latency (ms)")
plt.title("Average Query Latency vs Test Duration")
plt.grid(True)
plt.tight_layout()
plt.savefig(results_dir / "latency_vs_duration.png")
plt.close()

# Plot 2: Throughput vs duration
plt.figure()
plt.plot(df["duration_s"], df["throughput_rps"], marker="o")
plt.xlabel("Test duration (seconds)")
plt.ylabel("Throughput (requests per second)")
plt.title("Throughput vs Test Duration")
plt.grid(True)
plt.tight_layout()
plt.savefig(results_dir / "throughput_vs_duration.png")
plt.close()

# Plot 3: Total processed requests (optional)
plt.figure()
plt.plot(df["duration_s"], df["successful_requests"], marker="o")
plt.xlabel("Test duration (seconds)")
plt.ylabel("Total successful requests")
plt.title("Total Processed Requests vs Test Duration")
plt.grid(True)
plt.tight_layout()
plt.savefig(results_dir / "total_requests_vs_duration.png")
plt.close()

print("Scaling plots generated successfully in:")
print(results_dir.resolve())
