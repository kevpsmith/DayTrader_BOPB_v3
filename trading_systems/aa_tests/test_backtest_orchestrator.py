#!/usr/bin/env python3
import sys, os
# add the project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import os
import glob
import pandas as pd

# ensure project imports work
import sys
SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir))
sys.path.insert(0, PROJECT_ROOT)

from orchestrator.backtest_orchestrator import main as backtest_main

def collect_trades_csvs(results_dir="results"):
    pattern = os.path.join(results_dir, "*_trades.csv")
    files = glob.glob(pattern)
    if not files:
        print(f"[WARN] No trade files found in {results_dir}")
        return pd.DataFrame()
    df_list = []
    for fp in files:
        df = pd.read_csv(fp)
        df["source_file"] = os.path.basename(fp)
        df_list.append(df)
    return pd.concat(df_list, ignore_index=True)

def make_summary_df(trades_df):
    if trades_df.empty:
        return pd.DataFrame()
    trades_df["date"] = pd.to_datetime(trades_df["exit_time"]).dt.date
    return (
        trades_df
        .groupby(["date","symbol"])
        .agg(
            trades     = ("entry_time","count"),
            avg_return = ("return_pct","mean"),
            win_rate   = ("return_pct", lambda x: (x>0).mean())
        )
        .reset_index()
    )

def run_debug():
    # 1) pick your default dates here:
    dates = ["2025-01-03", "2025-01-07", "2025-01-10"]
    # 2) run the orchestrator
    print(f"[DEBUG] Running backtest for dates: {dates}")
    backtest_main(dates)

    # 3) collect & export
    trades = collect_trades_csvs("results")
    summary = make_summary_df(trades)

    out_excel = "results/debug_backtest_report.xlsx"
    os.makedirs("results", exist_ok=True)
    with pd.ExcelWriter(out_excel, engine="openpyxl") as writer:
        trades.to_excel(writer, sheet_name="Trades", index=False)
        summary.to_excel(writer, sheet_name="Summary", index=False)

    print(f"[DEBUG] Wrote combined report to {out_excel}")
    print("► Done.")

if __name__ == "__main__":
    run_debug()