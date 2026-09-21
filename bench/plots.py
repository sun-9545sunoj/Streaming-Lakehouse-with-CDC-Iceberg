#!/usr/bin/env python3
import os
import glob
import pandas as pd
import matplotlib.pyplot as plt

def plot_e1():
    e1_file = "results/e1/planning_vs_files.csv"
    if not os.path.exists(e1_file):
        print("E1 results not found.")
        return
        
    df = pd.read_csv(e1_file)
    
    uncompacted = df[df["state"] == "uncompacted"]
    compacted = df[df["state"] == "compacted"]
    
    os.makedirs("docs/figures", exist_ok=True)

    # Both arms are now measured at every target, so this is two real series
    # against the file count each arm actually had.
    plt.figure(figsize=(10, 6))
    plt.plot(uncompacted["actual_files"], uncompacted["plan_ms"],
             marker='o', label="Before compaction", linewidth=2, color="blue")
    if not compacted.empty:
        plt.plot(compacted["actual_files"], compacted["plan_ms"],
                 marker='s', label="After compaction", linewidth=2, color="red")
    plt.xscale('log')
    plt.xlabel("Number of Data Files (log scale)")
    plt.ylabel("Planning Time (ms)")
    plt.title("E1: Query Planning Cost vs Data File Count")
    plt.grid(True, which="both", ls="-", alpha=0.2)
    plt.legend()
    plt.savefig("docs/figures/e1_planning.png", dpi=300, bbox_inches='tight')
    plt.close()
    print("Saved docs/figures/e1_planning.png")

    # Metadata growth is the mechanism behind the planning curve, so it gets its
    # own figure rather than a sentence in the report.
    if uncompacted["meta_bytes"].max() > 0:
        plt.figure(figsize=(10, 6))
        plt.plot(uncompacted["actual_files"], uncompacted["meta_bytes"] / 1024,
                 marker='o', label="Before compaction", color="blue")
        plt.plot(compacted["actual_files"], compacted["meta_bytes"] / 1024,
                 marker='s', label="After compaction", color="red")
        plt.xscale('log')
        plt.xlabel("Number of Data Files (log scale)")
        plt.ylabel("Metadata Tree Size (KB)")
        plt.title("E1: Iceberg Metadata Size vs Data File Count")
        plt.grid(True, which="both", ls="-", alpha=0.2)
        plt.legend()
        plt.savefig("docs/figures/e1_metadata.png", dpi=300, bbox_inches='tight')
        plt.close()
        print("Saved docs/figures/e1_metadata.png")

def plot_e2():
    e2_files = sorted(glob.glob("results/e2/cow_vs_mor_p*.csv"))
    if not e2_files:
        print("E2 results not found.")
        return

    os.makedirs("docs/figures", exist_ok=True)
    for file in e2_files:
        tag = os.path.basename(file).replace("cow_vs_mor_p", "").replace(".csv", "")
        pct = tag.replace("_", ".")
        df = pd.read_csv(file)
        
        cow = df[df["table_type"] == "cow"]
        mor = df[df["table_type"] == "mor"]
        
        # Write Cost Plot
        plt.figure(figsize=(10, 6))
        plt.plot(cow["round"], cow["write_ms"], marker='o', label="COW (Write)", color="blue")
        plt.plot(mor["round"], mor["write_ms"], marker='s', label="MOR (Write)", color="green")
        plt.xlabel("Update Round")
        plt.ylabel("MERGE INTO Time (ms)")
        plt.title(f"E2: Write Amplification (Update Ratio = {pct}%)")
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.savefig(f"docs/figures/e2_write_cost_p{tag}.png", dpi=300, bbox_inches='tight')
        plt.close()

        # Read Cost Plot
        plt.figure(figsize=(10, 6))
        plt.plot(cow["round"], cow["scan_ms"], marker='o', label="COW (Read)", color="blue")
        plt.plot(mor["round"], mor["scan_ms"], marker='s', label="MOR (Read)", color="green")
        plt.xlabel("Update Round")
        plt.ylabel("Full Scan Time (ms)")
        plt.title(f"E2: Read Amplification (Update Ratio = {pct}%)")
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.savefig(f"docs/figures/e2_read_cost_p{tag}.png", dpi=300, bbox_inches='tight')
        plt.close()

        # Cumulative bytes written - the write-amplification story in bytes
        plt.figure(figsize=(10, 6))
        plt.plot(cow["round"], cow["added_bytes"].cumsum() / 1e6, marker='o', label="COW", color="blue")
        plt.plot(mor["round"], mor["added_bytes"].cumsum() / 1e6, marker='s', label="MOR", color="green")
        plt.xlabel("Update Round")
        plt.ylabel("Cumulative Bytes Written (MB)")
        plt.title(f"E2: Cumulative Write Volume (Update Ratio = {pct}%)")
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.savefig(f"docs/figures/e2_bytes_written_p{tag}.png", dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved E2 plots for {pct}%")

def main():
    plot_e1()
    plot_e2()

if __name__ == "__main__":
    main()
