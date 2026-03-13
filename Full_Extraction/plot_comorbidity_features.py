#!/usr/bin/env python3
"""
Generate figures comparing comorbidity feature variants (all-dx vs POA)
for all four systems: AHRQ Elixhauser, Quan Elixhauser, CMS-HCC V28, and AHRQ CCSR.

Produces email figures:
  1. Summary comparison across all 4 systems (mean counts + % with ≥1)
  2. Combined POA impact — top 10 categories affected per system (4 panels)

Plus detailed reference figures for each system.
"""

import csv
import os
import re

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "Filtered_Combined_2026-02-20")
CMS_DIR = os.path.join(SCRIPT_DIR, "cms_hcc_v28_2026")
CCSR_CSV = os.path.join(SCRIPT_DIR, "ccsr_v2026_1", "DXCCSR_v2026-1.csv")
FIG_DIR = os.path.join(DATA_DIR, "figures")
os.makedirs(FIG_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# AHRQ display labels
# ---------------------------------------------------------------------------
AHRQ_LABELS = {
    "aids": "AIDS/HIV", "alcohol": "Alcohol Abuse", "anemdef": "Anemia (Deficiency)",
    "autoimmune": "Autoimmune", "bldloss": "Blood Loss", "cancer_leuk": "Leukemia",
    "cancer_lymph": "Lymphoma", "cancer_mets": "Metastatic Cancer",
    "cancer_nsitu": "Cancer In Situ", "cancer_solid": "Solid Tumor",
    "cbvd": "Cerebrovascular", "coag": "Coagulopathy", "dementia": "Dementia",
    "depress": "Depression", "diab_cx": "Diabetes (Complicated)",
    "diab_uncx": "Diabetes (Uncomplicated)", "drug_abuse": "Drug Abuse",
    "hf": "Heart Failure", "htn_cx": "Hypertension (Complicated)",
    "htn_uncx": "Hypertension (Uncomplicated)", "liver_mld": "Liver Disease (Mild)",
    "liver_sev": "Liver Disease (Severe)", "lung_chronic": "Chronic Lung Disease",
    "neuro_movt": "Movement Disorder", "neuro_oth": "Other Neuro",
    "neuro_seiz": "Seizure/Epilepsy", "obese": "Obesity", "paralysis": "Paralysis",
    "perivasc": "Peripheral Vascular", "psychoses": "Psychoses",
    "pulmcirc": "Pulmonary Circulation", "renlfl_mod": "Renal Failure (Moderate)",
    "renlfl_sev": "Renal Failure (Severe)", "thyroid_hypo": "Hypothyroidism",
    "thyroid_oth": "Other Thyroid", "ulcer_peptic": "Peptic Ulcer",
    "valve": "Valvular Disease", "wghtloss": "Weight Loss",
}

QUAN_LABELS = {
    "chf": "CHF", "carit": "Arrhythmia", "valv": "Valvular", "pcd": "Pulm. Circ.",
    "pvd": "Periph. Vasc.", "hypunc": "HTN (Uncomp.)", "hypc": "HTN (Comp.)",
    "para": "Paralysis", "ond": "Other Neuro", "cpd": "Chronic Pulm.",
    "diabunc": "DM (Uncomp.)", "diabc": "DM (Comp.)", "hypothy": "Hypothyroid",
    "rf": "Renal Failure", "ld": "Liver Disease", "pud": "Peptic Ulcer",
    "aids": "AIDS/HIV", "lymph": "Lymphoma", "metacanc": "Metastatic",
    "solidtum": "Solid Tumor", "rheumd": "Rheumatic", "coag": "Coagulopathy",
    "obes": "Obesity", "wloss": "Weight Loss", "fed": "Fluid/Electrolyte",
    "blane": "Blood Loss Anemia", "dane": "Deficiency Anemia",
    "alcohol": "Alcohol", "drug": "Drug Abuse", "psycho": "Psychoses",
    "depre": "Depression",
}

# POA-dependent categories
AHRQ_POA_DEPENDENT = {
    "anemdef", "bldloss", "cbvd", "coag", "hf", "liver_mld", "liver_sev",
    "neuro_movt", "neuro_oth", "neuro_seiz", "paralysis", "psychoses",
    "pulmcirc", "renlfl_mod", "renlfl_sev", "ulcer_peptic", "valve", "wghtloss",
}
QUAN_POA_DEPENDENT = {
    "chf", "carit", "valv", "pcd", "ond", "rf", "ld", "pud",
    "rheumd", "coag", "wloss", "fed", "blane", "dane",
}


def load_hcc_labels():
    """Parse CMS V28 HCC labels from the SAS label file."""
    labels = {}
    path = os.path.join(CMS_DIR, "V28115L3.TXT")
    with open(path) as f:
        for line in f:
            m = re.match(r'\s*HCC(\d+)\s*="(.+?)"\s*', line)
            if m:
                labels[int(m.group(1))] = m.group(2).strip()
    return labels


def load_ccsr_labels():
    """Parse CCSR category labels from the AHRQ CSV mapping file."""
    labels = {}
    with open(CCSR_CSV, "r") as f:
        reader = csv.reader(f)
        header = [h.strip("' ") for h in next(reader)]
        for row in reader:
            for i in range(1, 7):
                cat_idx = header.index(f"CCSR CATEGORY {i}")
                desc_idx = header.index(f"CCSR CATEGORY {i} DESCRIPTION")
                cat = row[cat_idx].strip("' ")
                if cat and cat not in labels:
                    labels[cat] = row[desc_idx].strip()
    return labels


def load_data():
    elix = pd.read_parquet(os.path.join(DATA_DIR, "elixhauser_features.parquet"))
    hcc = pd.read_parquet(os.path.join(DATA_DIR, "hcc_features.parquet"))
    ccsr = pd.read_parquet(os.path.join(DATA_DIR, "ccsr_features.parquet"))
    print(f"Loaded Elixhauser: {elix.shape}", flush=True)
    print(f"Loaded HCC: {hcc.shape}", flush=True)
    print(f"Loaded CCSR: {ccsr.shape}", flush=True)
    return elix, hcc, ccsr


def get_prevalences(df, prefix, label_map, exclude_suffixes=("_vw_score", "_count", "_raf_score")):
    """Get prevalence for each category under a given prefix."""
    cols = sorted([c for c in df.columns
                   if c.startswith(prefix) and not any(c.endswith(s) for s in exclude_suffixes)])
    n = len(df)
    records = []
    for col in cols:
        cat = col[len(prefix):]
        label = label_map.get(cat, cat)
        count = int(df[col].sum())
        records.append({"category": cat, "label": label, "count": count, "pct": count / n * 100})
    return pd.DataFrame(records).sort_values("pct", ascending=False).reset_index(drop=True)


def plot_grouped_prevalence(all_prev, poa_prev, title, poa_dep_set, filename, n_show=20):
    """Side-by-side bar chart of all-dx vs POA prevalence."""
    merged = all_prev.merge(poa_prev, on=["category", "label"],
                            suffixes=("_all", "_poa"))
    merged = merged.sort_values("pct_all", ascending=True).tail(n_show)

    fig, ax = plt.subplots(figsize=(10, max(6, n_show * 0.35)))
    y = np.arange(len(merged))
    h = 0.35

    ax.barh(y + h / 2, merged["pct_all"], h, label="All-Dx", color="#4C72B0", alpha=0.85)
    ax.barh(y - h / 2, merged["pct_poa"], h, label="POA-Filtered", color="#DD8452", alpha=0.85)

    labels = []
    for _, row in merged.iterrows():
        lbl = row["label"]
        if poa_dep_set and row["category"] in poa_dep_set:
            lbl += " *"
        labels.append(lbl)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=9)
    ax.set_xlabel("Prevalence (%)", fontsize=11)
    ax.set_title(title, fontsize=13, fontweight="bold")
    ax.legend(loc="lower right", fontsize=10)
    ax.xaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))

    if poa_dep_set:
        ax.annotate("* = POA-dependent category", xy=(0.99, 0.01),
                    xycoords="axes fraction", ha="right", va="bottom",
                    fontsize=8, fontstyle="italic", color="gray")

    plt.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, filename), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {filename}", flush=True)


def plot_poa_delta(all_prev, poa_prev, title, filename, max_show=25):
    """Bar chart showing categories with the largest absolute drop from all-dx to POA."""
    merged = all_prev.merge(poa_prev, on=["category", "label"],
                            suffixes=("_all", "_poa"))
    merged["delta_pct"] = merged["pct_all"] - merged["pct_poa"]
    merged = merged[merged["delta_pct"] > 0.1].sort_values("delta_pct", ascending=True).tail(max_show)

    if len(merged) == 0:
        print(f"  Skipping {filename} — no deltas", flush=True)
        return

    fig, ax = plt.subplots(figsize=(9, max(4, len(merged) * 0.4)))
    y = np.arange(len(merged))
    colors = ["#C44E52" if d > 2 else "#DD8452" if d > 1 else "#CCB974"
              for d in merged["delta_pct"]]
    ax.barh(y, merged["delta_pct"], color=colors, alpha=0.85)
    ax.set_yticks(y)
    ax.set_yticklabels(merged["label"], fontsize=9)
    ax.set_xlabel("Prevalence Drop (percentage points)", fontsize=11)
    ax.set_title(title, fontsize=13, fontweight="bold")

    for i, (_, row) in enumerate(merged.iterrows()):
        ax.text(row["delta_pct"] + 0.1, i,
                f'{row["pct_all"]:.1f}% \u2192 {row["pct_poa"]:.1f}%',
                va="center", fontsize=8, color="gray")

    plt.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, filename), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {filename}", flush=True)


def plot_elixhauser_distributions(df, filename):
    """Distribution of VW scores and counts: all-dx vs POA for AHRQ and Quan."""
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))

    for row_idx, (system, prefix_all, prefix_poa) in enumerate([
        ("AHRQ", "ahrq_all_", "ahrq_poa_"),
        ("Quan", "quan_all_", "quan_poa_"),
    ]):
        # VW Score
        ax = axes[row_idx, 0]
        vw_all = df[f"{prefix_all}vw_score"]
        vw_poa = df[f"{prefix_poa}vw_score"]
        bins = np.arange(max(-20, min(vw_all.min(), vw_poa.min()) - 1),
                         min(50, max(vw_all.max(), vw_poa.max()) + 2), 1)
        ax.hist(vw_all, bins=bins, alpha=0.6, label=f"All-Dx (mean={vw_all.mean():.1f})",
                color="#4C72B0", density=True)
        ax.hist(vw_poa, bins=bins, alpha=0.6, label=f"POA (mean={vw_poa.mean():.1f})",
                color="#DD8452", density=True)
        ax.set_xlabel("Van Walraven Score")
        ax.set_ylabel("Density")
        ax.set_title(f"{system} — VW Score", fontweight="bold")
        ax.legend(fontsize=9)

        # Count
        ax = axes[row_idx, 1]
        cnt_all = df[f"{prefix_all}count"]
        cnt_poa = df[f"{prefix_poa}count"]
        bins_cnt = np.arange(-0.5, min(max(cnt_all.max(), cnt_poa.max()), 25) + 1.5, 1)
        ax.hist(cnt_all, bins=bins_cnt, alpha=0.6,
                label=f"All-Dx (mean={cnt_all.mean():.1f})", color="#4C72B0", density=True)
        ax.hist(cnt_poa, bins=bins_cnt, alpha=0.6,
                label=f"POA (mean={cnt_poa.mean():.1f})", color="#DD8452", density=True)
        ax.set_xlabel("Number of Comorbidities")
        ax.set_ylabel("Density")
        ax.set_title(f"{system} — Comorbidity Count", fontweight="bold")
        ax.legend(fontsize=9)

    plt.suptitle("Elixhauser Scores: All-Dx vs POA-Filtered",
                 fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, filename), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {filename}", flush=True)


def plot_hcc_distributions(df, filename):
    """Distribution of RAF scores and HCC counts: all-dx vs POA."""
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    # RAF Score
    ax = axes[0]
    raf_all = df["hcc_all_raf_score"]
    raf_poa = df["hcc_poa_raf_score"]
    bins = np.linspace(0, min(raf_all.quantile(0.99), 10), 60)
    ax.hist(raf_all, bins=bins, alpha=0.6,
            label=f"All-Dx (mean={raf_all.mean():.2f})", color="#4C72B0", density=True)
    ax.hist(raf_poa, bins=bins, alpha=0.6,
            label=f"POA (mean={raf_poa.mean():.2f})", color="#DD8452", density=True)
    ax.set_xlabel("CMS-HCC RAF Score")
    ax.set_ylabel("Density")
    ax.set_title("CMS-HCC V28 — RAF Score", fontweight="bold")
    ax.legend(fontsize=10)

    # HCC Count
    ax = axes[1]
    cnt_all = df["hcc_all_count"]
    cnt_poa = df["hcc_poa_count"]
    bins_cnt = np.arange(-0.5, min(max(cnt_all.max(), cnt_poa.max()), 30) + 1.5, 1)
    ax.hist(cnt_all, bins=bins_cnt, alpha=0.6,
            label=f"All-Dx (mean={cnt_all.mean():.1f})", color="#4C72B0", density=True)
    ax.hist(cnt_poa, bins=bins_cnt, alpha=0.6,
            label=f"POA (mean={cnt_poa.mean():.1f})", color="#DD8452", density=True)
    ax.set_xlabel("Number of HCCs")
    ax.set_ylabel("Density")
    ax.set_title("CMS-HCC V28 — HCC Count", fontweight="bold")
    ax.legend(fontsize=10)

    plt.suptitle("CMS-HCC Risk Scores: All-Dx vs POA-Filtered",
                 fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, filename), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {filename}", flush=True)


def plot_summary_comparison(elix, hcc, ccsr, filename):
    """Summary bar chart comparing all 4 systems: mean counts and % with ≥1, all-dx vs POA."""
    systems = ["AHRQ\nElixhauser", "Quan\nElixhauser", "CMS-HCC\nV28", "AHRQ\nCCSR"]

    # Mean comorbidity counts
    count_all = [
        elix["ahrq_all_count"].mean(),
        elix["quan_all_count"].mean(),
        hcc["hcc_all_count"].mean(),
        ccsr["ccsr_all_count"].mean(),
    ]
    count_poa = [
        elix["ahrq_poa_count"].mean(),
        elix["quan_poa_count"].mean(),
        hcc["hcc_poa_count"].mean(),
        ccsr["ccsr_poa_count"].mean(),
    ]

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    x = np.arange(len(systems))
    w = 0.3

    # Mean comorbidity count
    ax = axes[0]
    bars1 = ax.bar(x - w / 2, count_all, w, label="All-Dx", color="#4C72B0", alpha=0.85)
    bars2 = ax.bar(x + w / 2, count_poa, w, label="POA-Filtered", color="#DD8452", alpha=0.85)
    ax.set_xticks(x)
    ax.set_xticklabels(systems, fontsize=10)
    ax.set_ylabel("Mean Categories per Encounter", fontsize=11)
    ax.set_title("Mean Category Count", fontweight="bold", fontsize=12)
    ax.legend(fontsize=10)
    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                f"{bar.get_height():.1f}", ha="center", va="bottom", fontsize=9)
    for bar in bars2:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                f"{bar.get_height():.1f}", ha="center", va="bottom", fontsize=9)

    # Encounters with >=1 comorbidity
    ahrq_all_cols = [c for c in elix.columns if c.startswith("ahrq_all_") and not c.endswith(("_vw_score", "_count"))]
    ahrq_poa_cols = [c for c in elix.columns if c.startswith("ahrq_poa_") and not c.endswith(("_vw_score", "_count"))]
    quan_all_cols = [c for c in elix.columns if c.startswith("quan_all_") and not c.endswith(("_vw_score", "_count"))]
    quan_poa_cols = [c for c in elix.columns if c.startswith("quan_poa_") and not c.endswith(("_vw_score", "_count"))]
    hcc_all_cols = [c for c in hcc.columns if c.startswith("hcc_all_hcc")]
    hcc_poa_cols = [c for c in hcc.columns if c.startswith("hcc_poa_hcc")]
    ccsr_all_cols = [c for c in ccsr.columns if c.startswith("ccsr_all_") and not c.endswith(("_count", "_sys_count"))]
    ccsr_poa_cols = [c for c in ccsr.columns if c.startswith("ccsr_poa_") and not c.endswith(("_count", "_sys_count"))]
    n = len(elix)

    pct_all = [
        (elix[ahrq_all_cols].sum(axis=1) > 0).sum() / n * 100,
        (elix[quan_all_cols].sum(axis=1) > 0).sum() / n * 100,
        (hcc[hcc_all_cols].sum(axis=1) > 0).sum() / n * 100,
        (ccsr[ccsr_all_cols].sum(axis=1) > 0).sum() / n * 100,
    ]
    pct_poa = [
        (elix[ahrq_poa_cols].sum(axis=1) > 0).sum() / n * 100,
        (elix[quan_poa_cols].sum(axis=1) > 0).sum() / n * 100,
        (hcc[hcc_poa_cols].sum(axis=1) > 0).sum() / n * 100,
        (ccsr[ccsr_poa_cols].sum(axis=1) > 0).sum() / n * 100,
    ]

    ax = axes[1]
    bars1 = ax.bar(x - w / 2, pct_all, w, label="All-Dx", color="#4C72B0", alpha=0.85)
    bars2 = ax.bar(x + w / 2, pct_poa, w, label="POA-Filtered", color="#DD8452", alpha=0.85)
    ax.set_xticks(x)
    ax.set_xticklabels(systems, fontsize=10)
    ax.set_ylabel("% Encounters", fontsize=11)
    ax.set_title("Encounters with \u22651 Category", fontweight="bold", fontsize=12)
    ax.legend(fontsize=10)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                f"{bar.get_height():.1f}%", ha="center", va="bottom", fontsize=9)
    for bar in bars2:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                f"{bar.get_height():.1f}%", ha="center", va="bottom", fontsize=9)

    plt.suptitle(f"Comorbidity Feature Summary (N={n:,} encounters)",
                 fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, filename), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {filename}", flush=True)


def plot_combined_poa_impact(ahrq_all, ahrq_poa, quan_all, quan_poa,
                             hcc_all, hcc_poa, ccsr_all, ccsr_poa, filename):
    """Single combined figure: top POA-affected categories from all 4 systems."""
    fig, axes = plt.subplots(2, 2, figsize=(18, 14))
    n_show = 10

    panels = [
        (axes[0, 0], ahrq_all, ahrq_poa, "AHRQ Elixhauser"),
        (axes[0, 1], quan_all, quan_poa, "Quan Elixhauser"),
        (axes[1, 0], hcc_all, hcc_poa, "CMS-HCC V28"),
        (axes[1, 1], ccsr_all, ccsr_poa, "AHRQ CCSR"),
    ]

    for ax, all_prev, poa_prev, title in panels:
        merged = all_prev.merge(poa_prev, on=["category", "label"],
                                suffixes=("_all", "_poa"))
        merged["delta_pct"] = merged["pct_all"] - merged["pct_poa"]
        merged = merged[merged["delta_pct"] > 0.1].sort_values(
            "delta_pct", ascending=True).tail(n_show)

        y = np.arange(len(merged))
        colors = ["#C44E52" if d > 2 else "#DD8452" if d > 1 else "#CCB974"
                  for d in merged["delta_pct"]]
        ax.barh(y, merged["delta_pct"], color=colors, alpha=0.85)
        ax.set_yticks(y)
        ax.set_yticklabels(merged["label"], fontsize=9)
        ax.set_xlabel("Prevalence Drop (pp)", fontsize=10)
        ax.set_title(title, fontweight="bold", fontsize=11)

        for i, (_, row) in enumerate(merged.iterrows()):
            ax.text(row["delta_pct"] + 0.15, i,
                    f'{row["pct_all"]:.1f}% \u2192 {row["pct_poa"]:.1f}%',
                    va="center", fontsize=7.5, color="gray")

    plt.suptitle("Categories Most Affected by POA Filtering (Top 10 per System)",
                 fontsize=14, fontweight="bold", y=1.01)
    plt.tight_layout()
    fig.savefig(os.path.join(FIG_DIR, filename), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {filename}", flush=True)


def main():
    print("=" * 60, flush=True)
    print("Generating comorbidity feature comparison figures", flush=True)
    print("=" * 60, flush=True)

    elix, hcc, ccsr = load_data()
    hcc_labels = load_hcc_labels()
    hcc_label_map = {f"hcc{n}": f"HCC{n}: {desc[:40]}" for n, desc in hcc_labels.items()}
    ccsr_raw_labels = load_ccsr_labels()
    ccsr_label_map = {c.lower(): f"{c}: {d[:35]}" for c, d in ccsr_raw_labels.items()}

    # Get prevalences
    ahrq_all = get_prevalences(elix, "ahrq_all_", AHRQ_LABELS)
    ahrq_poa = get_prevalences(elix, "ahrq_poa_", AHRQ_LABELS)
    quan_all = get_prevalences(elix, "quan_all_", QUAN_LABELS)
    quan_poa = get_prevalences(elix, "quan_poa_", QUAN_LABELS)
    hcc_all = get_prevalences(hcc, "hcc_all_", hcc_label_map)
    hcc_poa = get_prevalences(hcc, "hcc_poa_", hcc_label_map)
    ccsr_all_prev = get_prevalences(ccsr, "ccsr_all_", ccsr_label_map)
    ccsr_poa_prev = get_prevalences(ccsr, "ccsr_poa_", ccsr_label_map)

    # ---- EMAIL FIGURES (2 total) ----
    print("\nGenerating email figures...", flush=True)

    print("  Figure 1: Summary comparison...", flush=True)
    plot_summary_comparison(elix, hcc, ccsr, "comorbidity_summary.png")

    print("  Figure 2: Combined POA impact...", flush=True)
    plot_combined_poa_impact(ahrq_all, ahrq_poa, quan_all, quan_poa,
                             hcc_all, hcc_poa, ccsr_all_prev, ccsr_poa_prev,
                             "poa_impact_combined.png")

    # ---- DETAILED REFERENCE FIGURES ----
    print("\nGenerating detailed reference figures...", flush=True)

    plot_grouped_prevalence(
        ahrq_all, ahrq_poa,
        "AHRQ Elixhauser: All-Dx vs POA-Filtered Prevalence (Top 20)",
        AHRQ_POA_DEPENDENT, "ahrq_prevalence_comparison.png",
    )
    plot_poa_delta(ahrq_all, ahrq_poa,
                   "AHRQ: Categories Most Affected by POA Filtering",
                   "ahrq_poa_delta.png")

    plot_grouped_prevalence(
        quan_all, quan_poa,
        "Quan Elixhauser: All-Dx vs POA-Filtered Prevalence (Top 20)",
        QUAN_POA_DEPENDENT, "quan_prevalence_comparison.png",
    )
    plot_poa_delta(quan_all, quan_poa,
                   "Quan: Categories Most Affected by POA Filtering",
                   "quan_poa_delta.png")

    plot_grouped_prevalence(
        hcc_all, hcc_poa,
        "CMS-HCC V28: All-Dx vs POA-Filtered Prevalence (Top 25)",
        None, "hcc_prevalence_comparison.png", n_show=25,
    )
    plot_poa_delta(hcc_all, hcc_poa,
                   "CMS-HCC: Categories Most Affected by POA Filtering",
                   "hcc_poa_delta.png")

    plot_grouped_prevalence(
        ccsr_all_prev, ccsr_poa_prev,
        "AHRQ CCSR: All-Dx vs POA-Filtered Prevalence (Top 25)",
        None, "ccsr_prevalence_comparison.png", n_show=25,
    )
    plot_poa_delta(ccsr_all_prev, ccsr_poa_prev,
                   "AHRQ CCSR: Categories Most Affected by POA Filtering",
                   "ccsr_poa_delta.png")

    plot_elixhauser_distributions(elix, "elixhauser_score_distributions.png")
    plot_hcc_distributions(hcc, "hcc_score_distributions.png")

    print(f"\nAll figures saved to {FIG_DIR}/", flush=True)
    print("\nFor the email, attach:", flush=True)
    print(f"  1. {FIG_DIR}/comorbidity_summary.png", flush=True)
    print(f"  2. {FIG_DIR}/poa_impact_combined.png", flush=True)


if __name__ == "__main__":
    main()
