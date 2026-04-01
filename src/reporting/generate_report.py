"""
HEDIS metrics report generator.

Reads data/processed/hedis_metrics.json and produces:
  - data/processed/denial_rate_by_payer.png
  - data/processed/top_10_procedures.png
  - data/processed/provider_performance.png
  - data/processed/report.html  (self-contained; charts embedded as base64)

Usage:
    source venv/bin/activate
    python src/reporting/generate_report.py
"""

import base64
import io
import json
import logging
import sys
from datetime import date
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # non-interactive backend; must be set before pyplot import
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.colors as mcolors

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REPO_ROOT    = Path(__file__).resolve().parents[2]
LOG_DIR      = REPO_ROOT / "logs"
PROCESSED    = REPO_ROOT / "data" / "processed"
METRICS_PATH = PROCESSED / "hedis_metrics.json"
HTML_PATH    = PROCESSED / "report.html"

CHART_DPI    = 150
PALETTE      = "#2563EB"   # primary blue
PALETTE_WARN = "#DC2626"   # red for high-denial emphasis
PALETTE_OK   = "#16A34A"   # green for low-denial

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def configure_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("generate_report")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(LOG_DIR / "reporting.log", mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_metrics() -> dict:
    with open(METRICS_PATH, encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Chart helpers
# ---------------------------------------------------------------------------

def _save_png(fig: plt.Figure, path: Path) -> None:
    fig.savefig(path, dpi=CHART_DPI, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def _fig_to_base64(path: Path) -> str:
    """Read a saved PNG and return a data-URI string for HTML embedding."""
    with open(path, "rb") as f:
        encoded = base64.b64encode(f.read()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


# ---------------------------------------------------------------------------
# Chart 1 — Denial rate by payer (vertical bar)
# ---------------------------------------------------------------------------

def chart_denial_rate_by_payer(data: list[dict], out_path: Path) -> Path:
    rows = sorted(data, key=lambda r: r["denial_rate_pct"], reverse=True)
    labels = [r["insurance_type"] for r in rows]
    rates  = [r["denial_rate_pct"] for r in rows]
    totals = [r["total_claims"] for r in rows]

    # Color bars: above overall mean → red-ish, below → green-ish
    mean_rate = sum(rates) / len(rates)
    colors = [PALETTE_WARN if r >= mean_rate else PALETTE_OK for r in rates]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(labels, rates, color=colors, edgecolor="white", linewidth=0.8, zorder=3)

    # Value labels on top of each bar
    for bar, rate, total in zip(bars, rates, totals):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.3,
            f"{rate:.1f}%\n(n={total:,})",
            ha="center", va="bottom", fontsize=8.5, color="#1e293b",
        )

    # Mean reference line
    ax.axhline(mean_rate, color="#64748b", linestyle="--", linewidth=1, zorder=2)
    ax.text(
        len(labels) - 0.5, mean_rate + 0.2,
        f"mean {mean_rate:.1f}%",
        ha="right", va="bottom", fontsize=8, color="#64748b",
    )

    ax.set_title("Claim Denial Rate by Payer", fontsize=13, fontweight="bold", pad=12)
    ax.set_ylabel("Denial Rate (%)", fontsize=10)
    ax.set_xlabel("Insurance Type", fontsize=10)
    ax.set_ylim(0, max(rates) * 1.25)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
    ax.grid(axis="y", color="#e2e8f0", zorder=0)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()

    _save_png(fig, out_path)
    return out_path


# ---------------------------------------------------------------------------
# Chart 2 — Top 10 procedures horizontal bar (dual axis: volume + cost)
# ---------------------------------------------------------------------------

def chart_top_10_procedures(data: list[dict], out_path: Path) -> Path:
    # JSON is already DESC by total_claims; reverse for horizontal bar
    # (bottom of chart = rank #1)
    rows = list(reversed(data))
    labels = [f"{r['procedure_code']}\n{r['description'][:38]}" for r in rows]
    volumes = [r["total_claims"] for r in rows]
    costs   = [r["total_claim_amount"] for r in rows]

    # Normalize cost to a color gradient
    norm = mcolors.Normalize(vmin=min(costs), vmax=max(costs))
    cmap = plt.cm.YlOrRd  # low cost = yellow, high cost = red
    bar_colors = [cmap(norm(c)) for c in costs]

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(labels, volumes, color=bar_colors, edgecolor="white",
                   linewidth=0.6, zorder=3)

    # Annotate each bar with claim count and total billed
    for bar, vol, cost in zip(bars, volumes, costs):
        ax.text(
            bar.get_width() + 0.4,
            bar.get_y() + bar.get_height() / 2,
            f"{vol} claims  |  ${cost:,.0f} billed",
            va="center", ha="left", fontsize=8, color="#1e293b",
        )

    # Colorbar legend for billed amount
    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, pad=0.01, fraction=0.02)
    cbar.set_label("Total Billed ($)", fontsize=8)
    cbar.ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))

    ax.set_title("Top 10 Procedures by Claim Volume", fontsize=13, fontweight="bold", pad=12)
    ax.set_xlabel("Number of Claims", fontsize=10)
    ax.set_xlim(0, max(volumes) * 1.55)
    ax.grid(axis="x", color="#e2e8f0", zorder=0)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()

    _save_png(fig, out_path)
    return out_path


# ---------------------------------------------------------------------------
# Chart 3 — Provider performance scatter (total claims vs denial rate)
# ---------------------------------------------------------------------------

def chart_provider_performance(data: list[dict], out_path: Path) -> Path:
    xs      = [r["total_claims"]    for r in data]
    ys      = [r["denial_rate_pct"] for r in data]
    avgs    = [r["avg_claim_amount"] for r in data]
    labels  = [r["provider_id"]     for r in data]

    # Bubble size proportional to avg_claim_amount
    min_a, max_a = min(avgs), max(avgs)
    sizes = [40 + 200 * (a - min_a) / (max_a - min_a + 1) for a in avgs]

    # Color by denial rate
    norm = mcolors.Normalize(vmin=min(ys), vmax=max(ys))
    cmap = plt.cm.RdYlGn_r   # green = low denial, red = high denial

    fig, ax = plt.subplots(figsize=(10, 6))
    sc = ax.scatter(xs, ys, s=sizes, c=ys, cmap=cmap, norm=norm,
                    alpha=0.80, edgecolors="#1e293b", linewidths=0.5, zorder=3)

    # Annotate outlier providers (top 3 denial rate and lowest denial rate)
    sorted_by_denial = sorted(data, key=lambda r: r["denial_rate_pct"])
    to_label = {r["provider_id"] for r in sorted_by_denial[-3:] + sorted_by_denial[:1]}
    for r in data:
        if r["provider_id"] in to_label:
            ax.annotate(
                r["provider_id"],
                xy=(r["total_claims"], r["denial_rate_pct"]),
                xytext=(5, 4), textcoords="offset points",
                fontsize=7.5, color="#1e293b",
            )

    # Mean reference lines
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    ax.axhline(mean_y, color="#94a3b8", linestyle="--", linewidth=1, zorder=2)
    ax.axvline(mean_x, color="#94a3b8", linestyle="--", linewidth=1, zorder=2)
    ax.text(max(xs) - 0.3, mean_y + 0.4, f"mean {mean_y:.1f}%",
            ha="right", va="bottom", fontsize=8, color="#64748b")

    # Colorbar
    cbar = fig.colorbar(sc, ax=ax, pad=0.01, fraction=0.03)
    cbar.set_label("Denial Rate (%)", fontsize=8)

    # Bubble size legend
    for size_label, avg_val in [("Low avg $", min_a), ("High avg $", max_a)]:
        s = 40 + 200 * (avg_val - min_a) / (max_a - min_a + 1)
        ax.scatter([], [], s=s, c="#94a3b8", alpha=0.7,
                   edgecolors="#1e293b", linewidths=0.5, label=f"{size_label} (${avg_val:,.0f})")
    ax.legend(title="Bubble size = Avg Claim $", fontsize=8, title_fontsize=8,
              loc="upper right", framealpha=0.9)

    ax.set_title("Provider Performance: Claim Volume vs Denial Rate",
                 fontsize=13, fontweight="bold", pad=12)
    ax.set_xlabel("Total Claims", fontsize=10)
    ax.set_ylabel("Denial Rate (%)", fontsize=10)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f%%"))
    ax.grid(color="#e2e8f0", zorder=0)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()

    _save_png(fig, out_path)
    return out_path


# ---------------------------------------------------------------------------
# HTML report
# ---------------------------------------------------------------------------

def build_html(metrics: dict, chart_paths: dict[str, Path]) -> str:
    dm   = metrics["diabetes_care_rate"][0]
    denial_rows = metrics["denial_rate_by_payer"]
    overall_denial = sum(r["denied_claims"] for r in denial_rows) / \
                     sum(r["total_claims"]  for r in denial_rows) * 100
    top_proc = metrics["top_10_procedures_by_volume"][0]
    provider_denial_rates = [r["denial_rate_pct"] for r in metrics["provider_performance"]]
    worst_provider = max(metrics["provider_performance"], key=lambda r: r["denial_rate_pct"])
    best_provider  = min(metrics["provider_performance"], key=lambda r: r["denial_rate_pct"])

    imgs = {k: _fig_to_base64(p) for k, p in chart_paths.items()}
    generated = date.today().isoformat()

    denial_table_rows = "".join(
        f"""<tr>
              <td>{r['insurance_type']}</td>
              <td class="num">{r['total_claims']:,}</td>
              <td class="num">{r['denied_claims']:,}</td>
              <td class="num {'hi' if r['denial_rate_pct'] >= overall_denial else 'lo'}">{r['denial_rate_pct']:.2f}%</td>
            </tr>"""
        for r in sorted(denial_rows, key=lambda r: r["denial_rate_pct"], reverse=True)
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>HEDIS Metrics Report — {generated}</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body  {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
             background: #f8fafc; color: #1e293b; line-height: 1.55; }}
    header {{ background: #1e40af; color: #fff; padding: 28px 40px; }}
    header h1 {{ font-size: 1.6rem; font-weight: 700; }}
    header p  {{ font-size: 0.88rem; opacity: 0.75; margin-top: 4px; }}
    main  {{ max-width: 1100px; margin: 0 auto; padding: 32px 24px 64px; }}
    h2    {{ font-size: 1.15rem; font-weight: 600; color: #1e40af;
             border-left: 4px solid #2563eb; padding-left: 10px;
             margin: 40px 0 16px; }}
    /* KPI cards */
    .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                 gap: 16px; margin-bottom: 8px; }}
    .kpi {{ background: #fff; border-radius: 10px; padding: 20px 24px;
            box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
    .kpi .value {{ font-size: 2rem; font-weight: 700; color: #2563eb; }}
    .kpi .label {{ font-size: 0.8rem; color: #64748b; margin-top: 4px; text-transform: uppercase;
                   letter-spacing: .04em; }}
    .kpi .sub   {{ font-size: 0.78rem; color: #94a3b8; margin-top: 6px; }}
    /* Charts */
    .chart-wrap {{ background: #fff; border-radius: 10px; padding: 20px;
                   box-shadow: 0 1px 4px rgba(0,0,0,.08); margin-bottom: 28px; }}
    .chart-wrap img {{ width: 100%; height: auto; display: block; }}
    /* Table */
    table  {{ width: 100%; border-collapse: collapse; font-size: 0.875rem; }}
    thead th {{ background: #1e40af; color: #fff; padding: 10px 14px; text-align: left; }}
    tbody tr:nth-child(even) {{ background: #f1f5f9; }}
    tbody td {{ padding: 9px 14px; border-bottom: 1px solid #e2e8f0; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .hi  {{ color: #dc2626; font-weight: 600; }}
    .lo  {{ color: #16a34a; font-weight: 600; }}
    footer {{ text-align: center; font-size: 0.78rem; color: #94a3b8; padding: 24px; }}
  </style>
</head>
<body>
<header>
  <h1>Healthcare Claims — HEDIS Metrics Report</h1>
  <p>Generated {generated} &nbsp;|&nbsp; Source: data/processed/hedis_metrics.json</p>
</header>
<main>

  <h2>Key Metrics at a Glance</h2>
  <div class="kpi-grid">
    <div class="kpi">
      <div class="value">{dm['diabetes_care_rate_pct']:.1f}%</div>
      <div class="label">Diabetes Care Rate</div>
      <div class="sub">{dm['diabetic_patient_count']} of {dm['total_patient_count']} patients with E11.9 dx</div>
    </div>
    <div class="kpi">
      <div class="value">{overall_denial:.1f}%</div>
      <div class="label">Overall Denial Rate</div>
      <div class="sub">{sum(r['denied_claims'] for r in denial_rows):,} denied of {sum(r['total_claims'] for r in denial_rows):,} claims</div>
    </div>
    <div class="kpi">
      <div class="value">{top_proc['procedure_code']}</div>
      <div class="label">Top Procedure by Volume</div>
      <div class="sub">{top_proc['description']}<br>{top_proc['total_claims']} claims &nbsp;|&nbsp; ${top_proc['total_claim_amount']:,.0f} billed</div>
    </div>
    <div class="kpi">
      <div class="value">{worst_provider['provider_id']}</div>
      <div class="label">Highest Provider Denial Rate</div>
      <div class="sub">{worst_provider['denial_rate_pct']:.1f}% &nbsp;({worst_provider['denied_claims']} of {worst_provider['total_claims']} claims denied)</div>
    </div>
    <div class="kpi">
      <div class="value">{best_provider['provider_id']}</div>
      <div class="label">Lowest Provider Denial Rate</div>
      <div class="sub">{best_provider['denial_rate_pct']:.1f}% &nbsp;({best_provider['denied_claims']} of {best_provider['total_claims']} claims denied)</div>
    </div>
  </div>

  <h2>1. Claim Denial Rate by Payer</h2>
  <div class="chart-wrap">
    <img src="{imgs['denial_rate']}" alt="Denial rate by payer bar chart" />
  </div>
  <table>
    <thead>
      <tr>
        <th>Insurance Type</th>
        <th class="num">Total Claims</th>
        <th class="num">Denied</th>
        <th class="num">Denial Rate</th>
      </tr>
    </thead>
    <tbody>
      {denial_table_rows}
    </tbody>
  </table>

  <h2>2. Top 10 Procedures by Volume &amp; Billed Amount</h2>
  <div class="chart-wrap">
    <img src="{imgs['top_procedures']}" alt="Top 10 procedures horizontal bar chart" />
  </div>

  <h2>3. Provider Performance: Volume vs Denial Rate</h2>
  <div class="chart-wrap">
    <img src="{imgs['provider_scatter']}" alt="Provider performance scatter plot" />
  </div>
  <p style="font-size:0.8rem;color:#64748b;margin-top:-12px;">
    Bubble size reflects average claim amount per provider. Dashed lines show cross-provider means.
    Labeled providers are the highest-denial outliers and the lowest-denial performer.
  </p>

</main>
<footer>Healthcare Claims Pipeline &nbsp;·&nbsp; HEDIS-Style Analytics &nbsp;·&nbsp; {generated}</footer>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logger = configure_logging()
    logger.info("generate_report starting")

    PROCESSED.mkdir(parents=True, exist_ok=True)
    metrics = load_metrics()
    logger.info("Metrics JSON loaded")

    logger.info("Rendering chart 1: denial rate by payer")
    p1 = chart_denial_rate_by_payer(
        metrics["denial_rate_by_payer"],
        PROCESSED / "denial_rate_by_payer.png",
    )

    logger.info("Rendering chart 2: top 10 procedures")
    p2 = chart_top_10_procedures(
        metrics["top_10_procedures_by_volume"],
        PROCESSED / "top_10_procedures.png",
    )

    logger.info("Rendering chart 3: provider performance scatter")
    p3 = chart_provider_performance(
        metrics["provider_performance"],
        PROCESSED / "provider_performance.png",
    )

    logger.info("Building HTML report")
    html = build_html(
        metrics,
        {"denial_rate": p1, "top_procedures": p2, "provider_scatter": p3},
    )

    HTML_PATH.write_text(html, encoding="utf-8")
    logger.info(f"Report written to {HTML_PATH}")

    sizes = {p.name: f"{p.stat().st_size / 1024:.1f} KB" for p in [p1, p2, p3, HTML_PATH]}
    for name, size in sizes.items():
        logger.info(f"  {name}: {size}")


if __name__ == "__main__":
    main()
