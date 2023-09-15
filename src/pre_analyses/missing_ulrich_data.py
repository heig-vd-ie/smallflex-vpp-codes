import polars as pl
import seaborn as sns
from matplotlib import pyplot as plt
from matplotlib.legend import Legend
from matplotlib.lines import Line2D
import warnings
from auxiliary.auxiliary import load_configs


def missing_matrix_ulrich(all_data):
    """
    show missing values matrix
    :param all_data: the polars dataframe of all data
    :return:
    """
    log, _ = load_configs()
    df = all_data.set_sorted("datetime", descending=False).upsample(time_column="datetime", every="10m").with_columns(pl.all().exclude(["datetime"]).fill_null(-1e10).fill_nan(-1e10).apply(lambda y: 0 if y < -1e9 else 1))
    warnings.filterwarnings("ignore")
    colours = ['r', 'b']
    plt.figure()
    # specify the colours - red is missing. blue is not missing
    ax = sns.heatmap(df.select(pl.all().exclude(["datetime"])), cmap=sns.color_palette(colours), cbar=False)
    ytick = df.with_columns(pl.Series(name="index", values=range(df.shape[0]))).with_columns(pl.col("datetime").dt.truncate(every="2y")).unique("datetime", keep="first")[["datetime", "index"]][1::]
    ax.set(xticks=range(df.shape[1]), xticklabels=df.columns, yticks=ytick["index"].to_list(), yticklabels=ytick["datetime"].to_list())
    patches = []
    for x in colours:
        patches.append(Line2D([0], [0], linewidth=1.0, linestyle='-', color=x, alpha=1.0))
    # And add these patches (with their group labels) to the new
    # legend item and place it on the plot.
    leg = Legend(ax, patches, labels=['missing', 'non-missing'], loc='center left', frameon=True)
    ax.add_artist(leg)
    plt.gcf().set_size_inches(10, 6)
    plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)
    log.info(all_data.set_sorted("datetime", descending=False).upsample(time_column="datetime", every="10m").describe())
    return ax
