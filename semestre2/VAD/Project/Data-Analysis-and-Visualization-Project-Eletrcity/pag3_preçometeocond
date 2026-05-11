from pathlib import Path

import pandas as pd
import plotly.graph_objects as go


try:
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    BASE_DIR = Path.cwd()

DATA_DIR = BASE_DIR / "data"

df_diarios = pd.read_csv(DATA_DIR / "dados_diarios.csv")
df_diarios["Data"] = pd.to_datetime(df_diarios["Data"])
df_diarios = df_diarios.sort_values("Data")


PRICE_COL = "avg_price_eur_mwh"

METEO_CONFIGS = {
    "temperatura": {
        "label": "Temperatura media",
        "column": "temp_C_mean",
        "unit": "C",
        "color": "#5fa8d3",
        "aliases": ("temp", "temperatura", "temperature")
    },
    "precipitacao": {
        "label": "Precipitacao",
        "column": "precip_mm_sum",
        "unit": "mm",
        "color": "#1565c0",
        "aliases": ("precip", "precipitacao", "precipitation")
    },
    "nebulosidade": {
        "label": "Nebulosidade media",
        "column": "mean_cloud",
        "unit": "%",
        "color": "#90a4ae",
        "aliases": ("cloud", "nuvens", "nebulosidade")
    },
    "luz": {
        "label": "Luz solar",
        "column": "Sunlight (em minutos)",
        "unit": "min",
        "color": "#f9a825",
        "aliases": ("luz", "sunlight", "solar")
    },
    "vento": {
        "label": "Vento medio",
        "column": "wind_speed_mean",
        "unit": "m/s",
        "color": "#66bb6a",
        "aliases": ("vento", "wind", "wind_speed")
    }
}

PRICE_CONFIG = {
    "label": "Preco medio diario",
    "column": PRICE_COL,
    "unit": "EUR/MWh",
    "color": "#c62828"
}

dropdown_options_preco_meteo = [
    {"label": "Temperatura media", "value": "temperatura"},
    {"label": "Precipitacao", "value": "precipitacao"},
    {"label": "Nebulosidade media", "value": "nebulosidade"},
    {"label": "Luz solar", "value": "luz"},
    {"label": "Vento medio", "value": "vento"}
]


def _normalize_key(value):
    if value is None:
        return "temperatura"

    value = str(value).strip().casefold()

    for key, config in METEO_CONFIGS.items():
        if value == key or value in config["aliases"]:
            return key

    valid_values = list(METEO_CONFIGS)
    raise ValueError(f"Condicao invalida. Usa uma de: {valid_values}")


def _selected_meteo_config(condition):
    condition = _normalize_key(condition)
    config = METEO_CONFIGS[condition]

    if config["column"] not in df_diarios.columns:
        raise ValueError(f"A coluna {config['column']} nao existe nos dados.")

    return config


def _scale_0_1(series):
    min_value = series.min()
    max_value = series.max()

    if pd.isna(min_value) or pd.isna(max_value) or max_value == min_value:
        return series * 0

    return (series - min_value) / (max_value - min_value)


def _prepare_mirror_data(condition="temperatura", rolling_window=7):
    meteo_config = _selected_meteo_config(condition)
    configs = [PRICE_CONFIG, meteo_config]
    cols = ["Data", PRICE_CONFIG["column"], meteo_config["column"]]

    stream_df = df_diarios[cols].dropna().copy()

    for config in configs:
        col = config["column"]
        scaled_col = f"{col}_scaled"

        stream_df[scaled_col] = _scale_0_1(stream_df[col].astype(float))

        if rolling_window and rolling_window > 1:
            stream_df[scaled_col] = (
                stream_df[scaled_col]
                .rolling(window=rolling_window, min_periods=1)
                .mean()
            )

    stream_df["Preco_normalizado"] = stream_df[f"{PRICE_CONFIG['column']}_scaled"]
    stream_df["Meteo_normalizado"] = -stream_df[f"{meteo_config['column']}_scaled"]

    return stream_df, meteo_config


def create_preco_meteocond_streamgraph(condicao="temperatura", rolling_window=7):
    stream_df, meteo_config = _prepare_mirror_data(
        condition=condicao,
        rolling_window=rolling_window
    )

    fig = go.Figure()
    price_col = PRICE_CONFIG["column"]
    meteo_col = meteo_config["column"]

    fig.add_trace(go.Scatter(
        x=stream_df["Data"],
        y=stream_df["Preco_normalizado"],
        mode="lines",
        fill="tozeroy",
        fillcolor="rgba(198, 40, 40, 0.72)",
        line=dict(
            color=PRICE_CONFIG["color"],
            width=1.3,
            shape="spline",
            smoothing=1.1
        ),
        name=PRICE_CONFIG["label"],
        customdata=stream_df[[price_col]].to_numpy(),
        hovertemplate=(
            f"{PRICE_CONFIG['label']}"
            "<br>Data: %{x|%Y-%m-%d}"
            f"<br>Valor real: %{{customdata[0]:.2f}} {PRICE_CONFIG['unit']}"
            "<extra></extra>"
        )
    ))

    fig.add_trace(go.Scatter(
        x=stream_df["Data"],
        y=stream_df["Meteo_normalizado"],
        mode="lines",
        fill="tozeroy",
        fillcolor=meteo_config["color"],
        line=dict(
            color=meteo_config["color"],
            width=1.3,
            shape="spline",
            smoothing=1.1
        ),
        opacity=0.72,
        name=meteo_config["label"],
        customdata=stream_df[[meteo_col]].to_numpy(),
        hovertemplate=(
            f"{meteo_config['label']}"
            "<br>Data: %{x|%Y-%m-%d}"
            f"<br>Valor real: %{{customdata[0]:.2f}} {meteo_config['unit']}"
            "<extra></extra>"
        )
    ))

    fig.add_hline(
        y=0,
        line_width=1.2,
        line_color="#2F3A45"
    )

    fig.add_annotation(
        x=0.01,
        y=0.96,
        xref="paper",
        yref="paper",
        text="Preco",
        showarrow=False,
        font=dict(size=13, color=PRICE_CONFIG["color"]),
        xanchor="left"
    )

    fig.add_annotation(
        x=0.01,
        y=0.04,
        xref="paper",
        yref="paper",
        text=meteo_config["label"],
        showarrow=False,
        font=dict(size=13, color=meteo_config["color"]),
        xanchor="left"
    )

    fig.update_layout(
        template="plotly_white",
        width=None,
        autosize=True,
        height=540,
        title_x=0.5,
        hovermode="x unified",
        xaxis_title="Data",
        yaxis_title="Valor normalizado espelhado",
        legend_title="Variaveis",
        font=dict(size=14),
        margin=dict(l=55, r=35, t=45, b=95),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.32,
            xanchor="center",
            x=0.5
        )
    )

    fig.update_xaxes(
        showgrid=True,
        rangeslider_visible=True
    )

    fig.update_yaxes(
        showgrid=False,
        zeroline=False,
        range=[-1.05, 1.05],
        tickmode="array",
        tickvals=[-1, -0.5, 0, 0.5, 1],
        ticktext=["alto", "", "0", "", "alto"]
    )

    return fig


def create_price_weather_streamgraph(condicao="temperatura", rolling_window=7):
    return create_preco_meteocond_streamgraph(
        condicao=condicao,
        rolling_window=rolling_window
    )


def create_streamgraph_preco_meteocond(condicao="temperatura", rolling_window=7):
    return create_preco_meteocond_streamgraph(
        condicao=condicao,
        rolling_window=rolling_window
    )


if __name__ == "__main__":
    fig = create_preco_meteocond_streamgraph("temperatura")
    fig.show()
