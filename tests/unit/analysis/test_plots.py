import pytest

# matplotlib belongs to the notebook extra, the dev install alone skips these tests.
matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402

from airpulse.analysis import plots  # noqa: E402


def test_style_sets_a_quiet_grid_and_the_series_order() -> None:
    plots.apply_style()

    assert plt.rcParams["grid.linestyle"] == "-"
    assert plt.rcParams["axes.spines.top"] is False
    assert plt.rcParams["axes.prop_cycle"].by_key()["color"] == list(plots.SERIES)


def test_line_ends_are_labeled_in_ink() -> None:
    fig, ax = plt.subplots()
    ax.plot([0, 1, 2], [5.0, 6.0, 7.5], label="north", color=plots.REGION_COLORS["north"])
    ax.plot([0, 1, 2], [1.0, 1.0, 1.0])

    plots.label_line_ends(ax, "{label} {value:.1f}")

    assert [(text.get_text(), text.xy) for text in ax.texts] == [("north 7.5", (2, 7.5))]
    assert ax.texts[0].get_color() == plots.INK
    plt.close(fig)


def test_legend_sits_above_the_axes_and_limits_carry_their_name() -> None:
    fig, ax = plt.subplots()
    ax.plot([0, 1], [10.0, 20.0], label="north")
    ax.plot([0, 1], [5.0, 8.0], label="south")

    plots.legend_above(ax, ncols=2)
    plots.reference_line(ax, 15.0, "WHO guideline 15")
    plots.reference_line(ax, 0.5, "half way", vertical=True)

    legend = ax.get_legend()
    assert [text.get_text() for text in legend.get_texts()] == ["north", "south"]
    assert legend.get_bbox_to_anchor().y0 >= ax.bbox.y1
    assert [text.get_text() for text in ax.texts] == ["WHO guideline 15", "half way"]
    assert len(ax.get_lines()) == 4
    plt.close(fig)


def test_save_figure_writes_a_png_named_after_the_content(tmp_path) -> None:
    fig, ax = plt.subplots()
    ax.plot([0, 1], [0, 1])

    path = plots.save_figure(fig, tmp_path / "images", "eda_daily_pm2_5")

    assert path.name == "eda_daily_pm2_5.png"
    assert path.stat().st_size > 0
    plt.close(fig)
