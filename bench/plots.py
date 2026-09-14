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
    
    plt.figure(figsize=(10, 6))
    
    plt.plot(uncompacted["actual_files"], uncompacted["plan_ms"], marker='o', label="Uncompacted", linewidth=2)
    
    if not compacted.empty:
        # compacted is a single point, but we draw a horizontal line for comparison
        c_plan = compacted["plan_ms"].iloc[0]
        plt.axhline(y=c_plan, color='r', linestyle='--', label=f"Compacted (plan: {c_plan} ms)")
        
    plt.xscale('log')
    plt.xlabel("Number of Data Files (log scale)")
    plt.ylabel("Planning Time (ms)")
    plt.title("E1: Query Planning Cost vs Data File Count")
    plt.grid(True, which="both", ls="-", alpha=0.2)
    plt.legend()
    
    os.makedirs("docs/figures", exist_ok=True)
    plt.savefig("docs/figures/e1_planning.png", dpi=300, bbox_inches='tight')
    print("Saved docs/figures/e1_planning.png")

def plot_e2():
    e2_files = glob.glob("results/e2/cow_vs_mor_*.csv")
    if not e2_files:
        print("E2 results not found.")
        return
        
    for file in e2_files:
        pct = file.split("_")[-1].replace("pct.csv", "")
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
        plt.savefig(f"docs/figures/e2_write_{pct}pct.png", dpi=300, bbox_inches='tight')
        
        # Read Cost Plot
        plt.figure(figsize=(10, 6))
        plt.plot(cow["round"], cow["read_ms"], marker='o', label="COW (Read)", color="blue")
        plt.plot(mor["round"], mor["read_ms"], marker='s', label="MOR (Read)", color="green")
        plt.xlabel("Update Round")
        plt.ylabel("Full Scan Time (ms)")
        plt.title(f"E2: Read Amplification (Update Ratio = {pct}%)")
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.savefig(f"docs/figures/e2_read_{pct}pct.png", dpi=300, bbox_inches='tight')
        print(f"Saved E2 plots for {pct}%")

def main():
    plot_e1()
    plot_e2()

if __name__ == "__main__":
    main()
