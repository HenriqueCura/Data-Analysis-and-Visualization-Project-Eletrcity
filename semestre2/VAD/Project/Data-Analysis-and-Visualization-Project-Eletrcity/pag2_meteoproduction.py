import colorsys
import unicodedata

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


df_diarios = pd.read_csv("data/dados_diarios.csv")
df_hora = pd.read_csv("data/dados_hora.csv")

df_diarios["Data"] = pd.to_datetime(df_diarios["Data"])
df_hora["Data"] = pd.to_datetime(df_hora["Data"])

df_diarios = df_diarios.sort_values("Data")
df_hora = df_hora.sort_values("Data")

prod_diaria = (
    df_hora
    .groupby("Data", as_index=False)
    .sum(numeric_only=True)
)

df_prod_meteo = pd.merge(
    prod_diaria,
    df_diarios,
    on="Data",
    how="inner"
)


# =========================
# CORES DO GRÁFICO POR DROPDOWN
# =========================

DEFAULT_PRODUCTION_COLOR = "#1f4e79"
DEFAULT_METEO_COLOR = "#c62828"

PRODUCTION_COLORS = {
    "total": "#1f4e79",
    "eolica": "#2e7d32",
    "fotovoltaica": "#f9a825",
    "hidrica": "#0277bd",
    "outras": "#6a1b9a"
}

METEO_COLORS = {
    "temperatura": "#ef6c00",
    "vento": "#00897b",
    "precipitacao": "#1565c0",
    "nebulosidade": "#757575",
    "sunlight": "#fdd835"
}

METEO_CONTRAST_COLORS = {
    "temperatura": ["#d84315", "#ad1457"],
    "vento": ["#00acc1", "#00695c"],
    "precipitacao": ["#64b5f6", "#3949ab"],
    "nebulosidade": ["#424242", "#90a4ae"],
    "sunlight": ["#ff7043", "#f57f17"]
}

SIMILAR_HUE_DISTANCE = 0.16
MIN_RGB_DISTANCE_SQUARED = 90 ** 2

LINE_WIDTH = 1.7
LINE_OPACITY = 0.65


def _normalize(text):
    normalized = unicodedata.normalize("NFKD", str(text))
    return "".join(
        char for char in normalized
        if not unicodedata.combining(char)
    ).casefold()


def _find_column(df, *tokens):
    normalized_tokens = [_normalize(token) for token in tokens]

    for col in df.columns:
        normalized_col = _normalize(col)

        if all(token in normalized_col for token in normalized_tokens):
            return col

    return None


def _build_production_options():
    configs = [
        ("total", "Produção total", ("rede", "distribuicao")),
        ("eolica", "Eólica", ("eolica",)),
        ("fotovoltaica", "Fotovoltaica", ("fotovoltaica",)),
        ("hidrica", "Hídrica", ("hidrica",)),
        ("outras", "Outras Tecnologias", ("outras", "tecnologias"))
    ]

    options = {}

    for key, label, tokens in configs:
        col = _find_column(df_prod_meteo, *tokens)

        if col:
            options[key] = (label, col)
            options[col] = (label, col)

    return options


producao_cols = _build_production_options()


def _build_production_color_options():
    colors = {}

    for key, (_, col) in producao_cols.items():
        if key in PRODUCTION_COLORS:
            colors[key] = PRODUCTION_COLORS[key]
            colors[col] = PRODUCTION_COLORS[key]

    return colors


producao_cores = _build_production_color_options()


meteo_cols = {
    "temperatura": ("Temperatura média", "temp_C_mean"),
    "vento": ("Velocidade do vento", "wind_speed_mean"),
    "precipitacao": ("Precipitação", "precip_mm_sum"),
    "nebulosidade": ("Nebulosidade média", "mean_cloud"),
    "sunlight": ("Luz solar", "Sunlight (em minutos)")
}

meteo_cols = {
    label: config
    for label, config in meteo_cols.items()
    if config[1] in df_prod_meteo.columns
}


def _hex_to_rgb(color):
    color = color.lstrip("#")
    return tuple(
        int(color[index:index + 2], 16)
        for index in (0, 2, 4)
    )


def _rgb_distance_squared(color_a, color_b):
    rgb_a = _hex_to_rgb(color_a)
    rgb_b = _hex_to_rgb(color_b)

    return sum(
        (component_a - component_b) ** 2
        for component_a, component_b in zip(rgb_a, rgb_b)
    )


def _hex_to_hls(color):
    red, green, blue = _hex_to_rgb(color)

    return colorsys.rgb_to_hls(
        red / 255,
        green / 255,
        blue / 255
    )


def _hue_distance(color_a, color_b):
    hue_a, _, saturation_a = _hex_to_hls(color_a)
    hue_b, _, saturation_b = _hex_to_hls(color_b)

    if min(saturation_a, saturation_b) < 0.15:
        return 1

    distance = abs(hue_a - hue_b)
    return min(distance, 1 - distance)


def _colors_are_too_close(color_a, color_b):
    return (
        _hue_distance(color_a, color_b) <= SIMILAR_HUE_DISTANCE
        or _rgb_distance_squared(color_a, color_b) <= MIN_RGB_DISTANCE_SQUARED
    )


def _get_meteo_color(meteo, prod_color):
    base_color = METEO_COLORS.get(meteo, DEFAULT_METEO_COLOR)

    if not _colors_are_too_close(prod_color, base_color):
        return base_color

    alternative_colors = METEO_CONTRAST_COLORS.get(meteo, [])

    for color in alternative_colors:
        if not _colors_are_too_close(prod_color, color):
            return color

    return max(
        alternative_colors or [base_color],
        key=lambda color: _rgb_distance_squared(prod_color, color)
    )


def beautify_figure(fig, title, yaxis_title):
    fig.update_layout(
        title=title,
        template="plotly_white",
        width=None,
        autosize=True,
        yaxis_title=yaxis_title,
        title_x=0.5,
        hovermode="x unified",
        legend_title="Variáveis",
        font=dict(size=14),
        margin=dict(l=50, r=40, t=0, b=50)
    )

    fig.update_xaxes(
        showgrid=True,
        rangeslider_visible=True
    )

    fig.update_yaxes(showgrid=True)

    return fig


def plot_sunlight(df):
    fig = px.line(
        df,
        x=df.index,
        y="Sunlight (em minutos)",
        labels={
            "Sunlight (em minutos)": "Minutos de Luz Solar"
        }
    )

    fig.update_layout(
        template="plotly_white",
        width=None,
        autosize=True,
        xaxis_title="Data",
        yaxis_title="Minutos de Luz Solar",
        title_x=0.5,
        hovermode="x unified",
        legend_title="Variáveis",
        font=dict(size=14),
        margin=dict(l=50, r=40, t=20, b=50)
    )

    fig.update_xaxes(
        showgrid=True,
        rangeslider_visible=True
    )

    fig.update_yaxes(
        showgrid=True,
        range=[0, df["Sunlight (em minutos)"].max() + 30]
    )

    return fig


def create_meteovsprod(meteo: str, tec: str):
    if meteo not in meteo_cols:
        raise ValueError(
            f"Valor meteorológico incorreto. Deve ser um de {list(meteo_cols)}"
        )

    if tec not in producao_cols:
        raise ValueError(
            f"Tecnologia incorreta. Deve ser uma de {list(producao_cols)}"
        )

    meteo_label, meteo_col = meteo_cols[meteo]
    prod_label, prod_col = producao_cols[tec]
    prod_color = producao_cores.get(tec, DEFAULT_PRODUCTION_COLOR)
    meteo_color = _get_meteo_color(meteo, prod_color)

    fig = go.Figure()

    # Linha da produção: muda de cor consoante a tecnologia escolhida
    fig.add_trace(go.Scatter(
        x=df_prod_meteo["Data"],
        y=df_prod_meteo[prod_col],
        name=prod_label,
        mode="lines",
        yaxis="y1",
        opacity=LINE_OPACITY,
        line=dict(
            color=prod_color,
            width=LINE_WIDTH
        )
    ))

    # Linha meteorológica: muda de cor consoante o fator escolhido
    fig.add_trace(go.Scatter(
        x=df_prod_meteo["Data"],
        y=df_prod_meteo[meteo_col],
        name=meteo_label,
        mode="lines",
        yaxis="y2",
        opacity=LINE_OPACITY,
        line=dict(
            color=meteo_color,
            width=LINE_WIDTH
        )
    ))

    fig.update_layout(
        template="plotly_white",
        width=None,
        autosize=True,
        title_x=0.5,
        hovermode="x unified",
        xaxis=dict(
            title="Data",
            rangeslider=dict(visible=True),
            showgrid=True
        ),
        yaxis=dict(
            title="Produção Energética (kWh)",
            side="left",
            showgrid=True
        ),
        yaxis2=dict(
            title="Variável Meteorológica",
            side="right",
            overlaying="y",
            showgrid=False
        ),
        legend=dict(
            orientation="h",
            y=-0.4,
            itemclick=False,
            itemdoubleclick=False
        ),
        margin=dict(l=60, r=90, t=10, b=80)
    )

    return fig


if __name__ == "__main__":
    fig = create_meteovsprod("vento", "eolica")
    fig.show()
