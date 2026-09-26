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

    # Left: what the planner pays (Iceberg planTasks, and Spark EXPLAIN for
    # contrast). Right: what the query pays end to end. Both are plotted against
    # target_files so the before/after points for one table line up vertically.
    fig, (ax_plan, ax_query) = plt.subplots(1, 2, figsize=(14, 6))
    x = uncompacted["target_files"]
    ax_plan.plot(x, uncompacted["iceberg_plan_ms"], marker='o', color="blue",
                 label="Iceberg planTasks - before compaction")
    ax_plan.plot(x, compacted["iceberg_plan_ms"].values, marker='s', color="red",
                 label="Iceberg planTasks - after compaction")
    ax_plan.plot(x, uncompacted["plan_ms"], marker='^', ls="--", color="gray",
                 label="Spark EXPLAIN (count query) - before")
    ax_plan.set_ylabel("Planning Time (ms, median)")
    ax_plan.set_title("Planning cost")

    ax_query.plot(x, uncompacted["agg_ms"], marker='o', color="blue", label="GROUP BY - before")
    ax_query.plot(x, compacted["agg_ms"].values, marker='s', color="red", label="GROUP BY - after")
    ax_query.plot(x, uncompacted["point_ms"], marker='o', ls="--", color="blue", label="Point lookup - before")
    ax_query.plot(x, compacted["point_ms"].values, marker='s', ls="--", color="red", label="Point lookup - after")
    ax_query.set_ylabel("Query Time (ms, median)")
    ax_query.set_title("End-to-end query cost")

    for ax in (ax_plan, ax_query):
        ax.set_xscale('log')
        ax.set_xlabel("Target Data Files Before Compaction (log scale)")
        ax.grid(True, which="both", ls="-", alpha=0.2)
        ax.legend(fontsize=8)
    fig.suptitle(f"E1: Cost vs Data File Count ({int(df['rows'].iloc[0]):,} rows, constant)")
    fig.savefig("docs/figures/e1_planning.png", dpi=300, bbox_inches='tight')
    plt.close(fig)
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

    plot_e2_overview(e2_files)


def plot_e2_overview(e2_files):
    """One grid for the report: per-round write, per-round read, cumulative cost."""
    frames = sorted((pd.read_csv(f) for f in e2_files), key=lambda d: d["p_pct"].iloc[0])
    fig, axes = plt.subplots(3, len(frames), figsize=(4.5 * len(frames), 11), squeeze=False)
    for col, df in enumerate(frames):
        cow = df[df["table_type"] == "cow"]
        mor = df[df["table_type"] == "mor"]
        pct = df["p_pct"].iloc[0]
        panels = [
            ("write_ms", "MERGE time (s)", lambda s: s / 1000),
            ("scan_ms", "Full-scan GROUP BY (ms)", lambda s: s),
            (None, "Cumulative write + scan (s)", None),
        ]
        for row, (metric, label, scale) in enumerate(panels):
            ax = axes[row][col]
            if metric:
                ax.plot(cow["round"], scale(cow[metric]), marker='o', ms=3, color="blue", label="COW")
                ax.plot(mor["round"], scale(mor[metric]), marker='s', ms=3, color="green", label="MOR")
            else:
                ax.plot(cow["round"], (cow["write_ms"] + cow["scan_ms"]).cumsum() / 1000,
                        color="blue", label="COW")
                ax.plot(mor["round"], (mor["write_ms"] + mor["scan_ms"]).cumsum() / 1000,
                        color="green", label="MOR")
            if row == 0:
                ax.set_title(f"update ratio {pct}% ({int(cow['update_count'].mean()):,} rows/round)")
            if col == 0:
                ax.set_ylabel(label)
            if row == 2:
                ax.set_xlabel("Update round")
            ax.grid(True, alpha=0.3)
            ax.legend(fontsize=8)
    rows = int(frames[0]["rows"].iloc[0])
    fig.suptitle(f"E2: copy-on-write vs merge-on-read, {rows:,} rows, no compaction between rounds")
    fig.savefig("docs/figures/e2_overview.png", dpi=200, bbox_inches='tight')
    plt.close(fig)
    print("Saved docs/figures/e2_overview.png")

def plot_e3():
    arms = [("results/e3/failure_trials.csv", "Checkpoint on"),
            ("results/e3/failure_trials_broken.csv", "Throwaway checkpoint (control)")]
    present = [(pd.read_csv(path), label) for path, label in arms if os.path.exists(path)]
    if not present:
        print("E3 results not found.")
        return

    os.makedirs("docs/figures", exist_ok=True)
    fig, axes = plt.subplots(1, len(present), figsize=(7 * len(present), 5), squeeze=False)
    for ax, (df, label) in zip(axes[0], present):
        colors = ["green" if p else "red" for p in df["passed"].astype(str).str.lower() == "true"]
        ax.bar(df["trial"], df["kills"], color=colors)
        passed = (df["passed"].astype(str).str.lower() == "true").sum()
        ax.set_title(f"{label}: {passed}/{len(df)} trials passed")
        ax.set_xlabel("Trial")
        ax.set_ylabel("kill -9 of the driver during the trial")
        ax.grid(True, axis="y", alpha=0.3)
    fig.suptitle("E3: Exactly-once under induced failure (green = table matched ground truth)")
    fig.savefig("docs/figures/e3_trials.png", dpi=300, bbox_inches='tight')
    plt.close(fig)
    print("Saved docs/figures/e3_trials.png")


def main():
    plot_e1()
    plot_e2()
    plot_e3()

if __name__ == "__main__":
    main()
