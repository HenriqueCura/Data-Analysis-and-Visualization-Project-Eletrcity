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


# =========================
# CORES DAS TECNOLOGIAS
# =========================

tecnologias = [
    "Eólica (kWh)",
    "Fotovoltaica (kWh)",
    "Hídrica (kWh)",
    "Outras Tecnologias (kWh)"
]

cores = px.colors.qualitative.Set3[:4]

cores_dic = {
    k: v
    for k, v in zip(tecnologias, cores)
}


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


def _get_production_color(tec, prod_label, prod_col):
    if prod_label == "Produção total":
        return "#000000"

    if tec in cores_dic:
        return cores_dic[tec]

    if prod_col in cores_dic:
        return cores_dic[prod_col]

    label_as_column = f"{prod_label} (kWh)"

    if label_as_column in cores_dic:
        return cores_dic[label_as_column]

    return "#1f4e79"


meteo_cols = {
    "temperatura": ("Temperatura média", "temp_C_mean", "#c62828"),
    "vento": ("Velocidade do vento", "wind_speed_mean", "#66bb6a"),
    "precipitacao": ("Precipitação", "precip_mm_sum", "#1565c0"),
    "nebulosidade": ("Nebulosidade média", "mean_cloud", "#90a4ae"),
    "sunlight": ("Luz solar", "Sunlight (em minutos)", "#f9a825")
}

meteo_cols = {
    label: config
    for label, config in meteo_cols.items()
    if config[1] in df_prod_meteo.columns
}


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
        margin=dict(l=50, r=40, t=70, b=50)
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

    meteo_label, meteo_col, meteo_color = meteo_cols[meteo]
    prod_label, prod_col = producao_cols[tec]

    prod_color = _get_production_color(
        tec=tec,
        prod_label=prod_label,
        prod_col=prod_col
    )

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_prod_meteo["Data"],
        y=df_prod_meteo[prod_col],
        name=prod_label,
        mode="lines",
        yaxis="y1",
        opacity=0.75,
        line=dict(
            color=prod_color,
            width=2
        )
    ))

    fig.add_trace(go.Scatter(
        x=df_prod_meteo["Data"],
        y=df_prod_meteo[meteo_col],
        name=meteo_label,
        mode="lines",
        yaxis="y2",
        opacity=0.75,
        line=dict(
            color=meteo_color,
            width=2
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
            y=-0.3
        ),
        margin=dict(l=60, r=90, t=90, b=80)
    )

    return fig

if __name__ == "__main__":
    fig = create_meteovsprod("vento", "hidrica")
    fig.show()