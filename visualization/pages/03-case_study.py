import json
import streamlit as st
import pandas as pd
import numpy as np
import folium
from streamlit_folium import st_folium, folium_static
import altair as alt


def hillchart(data):
    table = data["piecewise_table"]
    qn = data["q_n"]
    df = pd.DataFrame(table)
    q_vec = np.arange(0, qn, 0.1)

    results = pd.DataFrame(columns=["dv", "q", "power"])
    for i, d in table.items():
        head = d["head"]
        beta = d["beta"]
        dv = "[" + str(int(d["vmin_piece"])) + "-" + str(int(d["vmax_piece"])) + "]"
        power = q_vec * 998 * head + beta
        power_df = pd.DataFrame(power, columns=["power"])
        power_df["q"] = q_vec
        power_df["dv"] = dv
        results = pd.concat([results, power_df])

    base = alt.Chart(results).encode(
        alt.Color("dv").legend(None)
    ).transform_filter(
        "datum.dv !== 'IBM'"
    ).properties(
        width=500
    )

    line = base.mark_line().encode(x="q", y="power")


    last_power = base.mark_circle().encode(
        alt.X("last_q['q']:Q"),
        alt.Y("last_q['power']:Q")
    ).transform_aggregate(
        last_q="argmax(q)",
        groupby=["dv"]
    )

    volume_name = last_power.mark_text(align="left", dx=4).encode(text="dv")

    chart = (line + last_power + volume_name).encode(
        x=alt.X().title("q"),
        y=alt.Y().title("power")
    )
    return chart



if  __name__ == "__main__":
    m = folium.Map([46.458619013405825, 8.243703472316218], zoom_start=12, tiles=folium.TileLayer('opentopomap', control=False))
    layer_right = folium.TileLayer('opentopomap', control=False)
    layer_left = folium.TileLayer('Stadia_StamenToner', control=False)
    sbs = folium.plugins.SideBySideLayers(layer_left=layer_left, layer_right=layer_right)
    layer_left.add_to(m)
    layer_right.add_to(m)
    sbs.add_to(m)
    units = json.load(open("src/units.json"))
    design_schemes = {}
    possible_design_schemes = sorted(list(set(sum([l["design_scheme"] for l in list(units.values())], []))))
    for i, g in enumerate(possible_design_schemes):
        design_schemes[g] = folium.FeatureGroup(name=g, overlay=False, show=False if i>0 else True)
        m.add_child(design_schemes[g])
    for name, data in units.items():
        for g in data["design_scheme"]: 
            icon0 = folium.map.Icon(icon="industry", color="blue", prefix="fa") if data["type"] == "hydro" else \
                folium.map.Icon(icon="wind", color="green", prefix="fa") if data["type"] == "wind" else \
                folium.map.Icon(icon="solar-panel", color="red", prefix="fa") if data["type"] == "pv" else \
                folium.map.Icon(icon="water", color="purple", prefix="fa") if data["type"] == "pump" else \
                None
            icon1 = folium.features.DivIcon(icon_size=(100,0), icon_anchor=(0,0), html='<div style="font-size: 10pt; color: white; background-color: black; padding: 0px;">{}</div>'.format(name))
            marker = folium.Marker(location=data["geo"], tooltip=name, icon= icon0)
            df = pd.DataFrame(data.items())
            html = df[~df[0].isin(["geo", "piecewise_table"])].T.to_html(classes="table table-striped table-hover table-condensed table-responsive")
            popup1 = folium.Popup(html)
            popup1.add_to(marker)
            marker.add_to(design_schemes[g])
            marker2 = folium.Marker(location=data["geo"], icon= icon1)
            if data["type"] == "hydro":
                chart = hillchart(data)
                vega_lite = folium.VegaLite(chart, width="100%", height="100%",)
                popup2 = folium.Popup()
                vega_lite.add_to(popup2)
                popup2.add_to(marker2)
            marker2.add_to(design_schemes[g])
    folium.LayerControl(collapsed=False, autoZIndex=False).add_to(m)
    st_data = folium_static(m, width=1700, height=900)
