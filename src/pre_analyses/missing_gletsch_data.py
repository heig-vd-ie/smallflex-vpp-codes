import polars as pl
import numpy as np
import seaborn as sns
from matplotlib import pyplot as plt
from matplotlib.legend import Legend
from matplotlib.lines import Line2D
import warnings


def missing_matrix_gletsch(all_data):
    """
    show missing values matrix
    :param all_data: the polars dataframe of all data
    :return:
    """
    df = all_data.with_columns(pl.col("prediction_date").dt.truncate(every="12h")).set_sorted("prediction_date", descending=False).unique(subset=["prediction_date", "datetime"], maintain_order=True, keep="last").groupby("prediction_date").agg(pl.all().drop_nulls()).upsample(time_column="prediction_date", every="12h").with_columns(pl.all().exclude(["prediction_date"]).fill_null([np.nan]).apply(lambda x2: len(x2))).with_columns(pl.all().exclude(["prediction_date"]).apply(lambda x2: 0 if x2 < 10 else 1e15 if x2 < 1000 else 2 * 1e15))
    warnings.filterwarnings("ignore")
    colours = ['r', 'b', 'y']
    plt.figure()
    # specify the colours - red is missing. blue is not missing with 1 hour forecasting resolution. yellow is not missing with 1 minute forecasting resolution.
    ax = sns.heatmap(df.select(pl.all().exclude("prediction_date")), cmap=sns.color_palette(colours), cbar=False)
    ytick = df.with_columns(pl.Series(name="index", values=range(df.shape[0]))).with_columns(pl.col("prediction_date").dt.truncate(every="6mo")).unique("prediction_date", keep="first")[["prediction_date", "index"]][1::]
    ax.set(xticks=range(df.shape[1]), xticklabels=df.columns, yticks=ytick["index"].to_list(), yticklabels=ytick["prediction_date"].to_list())
    patches = []
    for x in colours:
        patches.append(Line2D([0], [0], linewidth=1.0, linestyle='-', color=x, alpha=1.0))
    # And add these patches (with their group labels) to the new
    # legend item and place it on the plot.
    leg = Legend(ax, patches, labels=['missing', 'non-missing (1 hour)', 'non-missing (1 minute)'], loc='center left', frameon=True)
    ax.add_artist(leg)
    plt.gcf().set_size_inches(10, 6)
    plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
    return ax
