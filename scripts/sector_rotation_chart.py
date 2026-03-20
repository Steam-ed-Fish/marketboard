import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
import pandas as pd
from pathlib import Path


sectors_data = {
    "Sector": [
        "Cons. Discretionary", "Cons. Staples", "Energy", "Financials",
        "Health Care", "Industrials", "Materials", "Real Estate",
        "Technology", "Communications", "Utilities",
    ],
    "Weekly Return %": [-1.05, -1.78, 2.88, 0.20, -2.12, 0.20, -2.95, -1.75, 1.19, -1.14, -0.89],
    "Flow Percentile (52W)": [64.70, 17.60, 39.20, 98.00, 92.10, 35.20, 82.30, 100.00, 0.00, 7.80, 88.20],
    "Flow Percentile (1M Ago)": [49.00, 76.40, 92.40, 9.80, 35.20, 67.90, 100.00, 80.30, 27.40, 49.00, 45.00],
    "PE Percentile (52W)": [25.40, 39.20, 98.10, 1.90, 0.00, 1.80, 29.40, 94.10, 56.80, 92.10, 94.10],
}

industries_data = {
    "Sector": [
        "Metals & Mining", "REITs", "Gold Miners", "MLPs", "Homebuilders",
        "Oil Services", "Regional Banks", "Retail", "Agriculture", "Internet",
        "Biotech", "Semiconductors", "Oil & Gas E&P", "Clean Energy", "Insurance",
        "Water Resources", "Software", "Solar", "Airlines",
    ],
    "Weekly Return %": [-3.54, -0.62, -11.11, -0.62, -2.62, 5.79, 0.13, -0.15, -1.36, -0.63, -0.63, 1.96, 4.26, 1.96, -1.89, -1.10, -3.39, 2.83, 2.66],
    "Flow Percentile (52W)": [13.70, 29.40, 0.00, 47.00, 94.10, 62.70, 31.30, 9.80, 100.00, 72.50, 82.30, 100.00, 100.00, 15.60, 56.80, 27.40, 90.10, 90.10, 96.00],
    "Flow Percentile (1M Ago)": [74.50, 43.10, 13.70, 19.60, 25.40, 5.80, 37.20, 49.00, 21.50, 66.60, 13.70, 47.00, 98.00, 7.80, 50.90, 98.00, 92.10, 27.40, 50.90],
    "PE Percentile (52W)": [0.00, 0.00, 0.00, 100.00, 0.00, 100.00, 0.00, 0.00, 1.90, 11.70, 0.00, 54.90, 100.00, 0.00, 0.00, 0.00, 17.60, 100.00, 1.90],
}


def create_chart(data_dict, title, output_path):
    df = pd.DataFrame(data_dict)
    df["Flow Momentum"] = (df["Flow Percentile (52W)"] - df["Flow Percentile (1M Ago)"]).abs()

    fig, ax = plt.subplots(figsize=(14, 10))
    cmap = LinearSegmentedColormap.from_list("valuation", ["#2ECC71", "#F1C40F", "#E74C3C"], N=256)
    norm = plt.Normalize(0, 100)

    bubble_sizes = 5100 + df["Flow Momentum"] * 125
    ax.axvline(x=0, color="#7F8C8D", linestyle="--", linewidth=1.5, alpha=0.7, zorder=1)
    ax.axhline(y=50, color="#7F8C8D", linestyle="--", linewidth=1.5, alpha=0.7, zorder=1)

    x_min, x_max = df["Weekly Return %"].min() - 0.8, df["Weekly Return %"].max() + 0.8
    ax.fill_between([0, x_max], 50, 100, alpha=0.05, color="green", zorder=0)
    ax.fill_between([x_min, 0], 50, 100, alpha=0.05, color="blue", zorder=0)
    ax.fill_between([0, x_max], 0, 50, alpha=0.05, color="orange", zorder=0)
    ax.fill_between([x_min, 0], 0, 50, alpha=0.05, color="red", zorder=0)

    scatter = ax.scatter(
        df["Weekly Return %"],
        df["Flow Percentile (52W)"],
        s=bubble_sizes,
        c=df["PE Percentile (52W)"],
        cmap=cmap,
        norm=norm,
        alpha=0.75,
        edgecolors="white",
        linewidths=2,
        zorder=3,
    )

    for _, row in df.iterrows():
        ax.annotate(
            row["Sector"],
            xy=(row["Weekly Return %"], row["Flow Percentile (52W)"]),
            ha="center",
            va="center",
            fontsize=10 if len(df) > 11 else 12,
            fontweight="bold",
            color="#2C3E50",
            zorder=4,
            bbox=dict(boxstyle="round,pad=0.2", facecolor="white", edgecolor="none", alpha=0.8),
        )

    ax.set_xlabel("Weekly Return %\n← Loss | Gain →", fontsize=12, fontweight="bold", labelpad=10)
    ax.set_ylabel("Flow Percentile (52W)\n← Outflows | Inflows →", fontsize=12, fontweight="bold", labelpad=10)
    ax.set_title(title, fontsize=16, fontweight="bold", pad=20, color="#2C3E50")
    ax.set_xlim(x_min, x_max)
    ax.set_ylim(0, 100)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.1f}%"))
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:.0f}%"))

    cbar = plt.colorbar(scatter, ax=ax, shrink=0.45, aspect=20, pad=0.015, anchor=(0, 1.0))
    cbar.set_label("PE Percentile (52W)\n← Cheap | Expensive →", fontsize=9, fontweight="bold")
    cbar.set_ticks([0, 25, 50, 75, 100])
    cbar.set_ticklabels(["0% (Cheap)", "25%", "50% (Fair)", "75%", "100% (Expensive)"])

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)


if __name__ == "__main__":
    out_dir = Path(__file__).resolve().parent
    sector_path = out_dir / "sector_rotation_chart.png"
    industry_path = out_dir / "industry_rotation_chart.png"
    create_chart(sectors_data, "Sector Rotation Analysis", sector_path)
    create_chart(industries_data, "Industries Rotation Analysis", industry_path)
    print(f"Saved: {sector_path}")
    print(f"Saved: {industry_path}")
