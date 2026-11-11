"""one look for every chart of the notebooks, the colors, the type sizes and a few helpers.

colors are fixed per meaning so a region or a model keeps its color from one chart to the next.
the series colors stay apart for readers with a color vision deficiency, values on a scale use a
single blue ramp and signed values a blue to red ramp with a neutral middle. matplotlib comes
with the notebook extra, so it is imported here and not in the rest of the package.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
from cycler import cycler
from matplotlib.axes import Axes
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.figure import Figure

SURFACE = "#ffffff"
INK = "#0b0b0b"
SECONDARY_INK = "#52514e"
MUTED_INK = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"

# series colors in a fixed order, neighbors stay distinct and the first three in any pairing.
SERIES = ("#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4")
REGION_COLORS = {"north": SERIES[0], "central": SERIES[1], "south": SERIES[2]}
# five steps of one hue for ordered classes, light is low.
ORDINAL = ("#86b6ef", "#5598e7", "#2a78d6", "#1c5cab", "#0d366b")
REFERENCE = "#898781"

SEQUENTIAL = LinearSegmentedColormap.from_list(
    "airpulse_blue",
    ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"],
)
DIVERGING = LinearSegmentedColormap.from_list(
    "airpulse_blue_red", ["#2a78d6", "#f0efec", "#e34948"]
)


def apply_style() -> None:
    """set the matplotlib defaults, thin marks, a quiet grid and text in ink instead of color."""
    plt.rcParams.update(
        {
            "figure.figsize": (8.0, 4.2),
            "figure.dpi": 110,
            "savefig.dpi": 160,
            "figure.facecolor": SURFACE,
            "axes.facecolor": SURFACE,
            "savefig.facecolor": SURFACE,
            "font.size": 10,
            "text.color": INK,
            "axes.titlesize": 12,
            "axes.titleweight": "bold",
            "axes.titlelocation": "left",
            "axes.titlepad": 12,
            "axes.labelsize": 10,
            "axes.labelcolor": SECONDARY_INK,
            "axes.edgecolor": AXIS,
            "axes.linewidth": 0.8,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "axes.grid.axis": "y",
            "axes.axisbelow": True,
            "axes.prop_cycle": cycler(color=SERIES),
            "grid.color": GRID,
            "grid.linewidth": 0.8,
            "grid.linestyle": "-",
            "xtick.color": AXIS,
            "ytick.color": AXIS,
            "xtick.labelcolor": SECONDARY_INK,
            "ytick.labelcolor": SECONDARY_INK,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "lines.linewidth": 2.0,
            "lines.solid_capstyle": "round",
            "lines.solid_joinstyle": "round",
            "legend.frameon": False,
            "legend.fontsize": 9,
        }
    )


def label_line_ends(ax: Axes, fmt: str = "{label}") -> None:
    """write the name of every labeled line next to its last point, in ink, not in its color."""
    for line in ax.get_lines():
        label = line.get_label()
        xs, ys = line.get_xdata(), line.get_ydata()
        if label.startswith("_") or len(xs) == 0:
            continue
        ax.annotate(
            fmt.format(label=label, value=ys[-1]),
            xy=(xs[-1], ys[-1]),
            xytext=(6, 0),
            textcoords="offset points",
            va="center",
            fontsize=9,
            color=INK,
        )


def legend_above(ax: Axes, ncols: int = 3) -> None:
    """put the legend on the title line at the right, where it cannot cover the data."""
    ax.legend(
        loc="lower right",
        bbox_to_anchor=(1.0, 1.0),
        ncols=ncols,
        borderaxespad=0.3,
        handlelength=1.4,
        columnspacing=1.2,
    )


def reference_line(ax: Axes, value: float, label: str, vertical: bool = False) -> None:
    """a thin line for a limit with its name, past the right edge or along the bottom."""
    text = {"fontsize": 9, "color": SECONDARY_INK, "textcoords": "offset points"}
    if vertical:
        ax.axvline(value, color=REFERENCE, linewidth=1)
        ax.annotate(
            label, xy=(value, 0.02), xycoords=("data", "axes fraction"), xytext=(4, 0), **text
        )
    else:
        ax.axhline(value, color=REFERENCE, linewidth=1)
        ax.annotate(
            label,
            xy=(1.0, value),
            xycoords=("axes fraction", "data"),
            xytext=(4, 0),
            va="center",
            **text,
        )


def save_figure(fig: Figure, directory: Path, name: str) -> Path:
    """write the figure as a png named after its content and return the path."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.png"
    fig.savefig(path, bbox_inches="tight")
    return path
