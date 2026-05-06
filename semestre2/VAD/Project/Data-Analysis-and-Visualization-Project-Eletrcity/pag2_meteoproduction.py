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
    return "".join(char for char in normalized if not unicodedata.combining(char)).casefold()

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
        ("hidrica", "Hídrica", ("hidrica",))
    ]

    options = {}
    for key, label, tokens in configs:
        col = _find_column(df_prod_meteo, *tokens)
        if col:
            options[key] = (label, col)
            options[col] = (label, col)
    return options

producao_cols = _build_production_options()

meteo_cols = {
    "temperatura": ("Temperatura média", "temp_C_mean", "#c62828"),
    "vento": ("Velocidade do vento", "wind_speed_mean", "#66bb6a"),
    "precipitacao": ("Precipitação", "precip_mm_sum", "#1565c0"),
    "nebulosidade": ("Nebulosidade média", "mean_cloud", "#90a4ae"),
    "sunlight": ("Luz solar", "Sunlight (em minutos)", "#f9a825")
}

meteo_cols = {
    label: config for label, config in meteo_cols.items()
    if config[1] in df_prod_meteo.columns
}

def beautify_figure(fig, title, yaxis_title):
    fig.update_layout(
        title=title,
        template="plotly_white",
        width=1400,
        height=500,
        xaxis_title="Data",
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
        #title="Luz Solar Diária",
        labels={"Sunlight (em minutos)": "Minutos de Luz Solar"}
    )

    fig.update_layout(
        template="plotly_white",
        width=1400,
        height=500,
        xaxis_title="Data",
        yaxis_title="Minutos de Luz Solar",
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

    fig.update_yaxes(
        showgrid=True,
        range=[0, df["Sunlight (em minutos)"].max() + 30]
    )
    return fig

def create_meteovsprod(meteo: str, tec: str):
    if meteo not in meteo_cols:
        raise ValueError(f"Valor meteorológico incorreto. Deve ser um de {list(meteo_cols)}")
    if tec not in producao_cols:
        raise ValueError(f"Tecnologia incorreta. Deve ser uma de {list(producao_cols)}")

    meteo_label, meteo_col, meteo_color = meteo_cols[meteo]
    prod_label, prod_col = producao_cols[tec]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_prod_meteo["Data"],
        y=df_prod_meteo[prod_col],
        name=prod_label,
        mode="lines",
        yaxis="y1",
        line=dict(color="#1f4e79", width=3)
    ))

    fig.add_trace(go.Scatter(
        x=df_prod_meteo["Data"],
        y=df_prod_meteo[meteo_col],
        name=meteo_label,
        mode="lines",
        yaxis="y2",
        line=dict(color=meteo_color, width=2.5)
    ))

    fig.update_layout(
        #title=f"{prod_label} vs {meteo_label}",
        template="plotly_white",
        width=1400,
        height=600,
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
            title=meteo_label,
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

def create_meteovsprod_interactive():
    fig = go.Figure()

    prod_options = list(dict.fromkeys(producao_cols.values()))
    prod_labels = [config[0] for config in prod_options]
    prod_columns = [config[1] for config in prod_options]
    meteo_labels = [config[0] for config in meteo_cols.values()]
    meteo_columns = [config[1] for config in meteo_cols.values()]

    fig.add_trace(go.Scatter(
        x=df_prod_meteo["Data"],
        y=df_prod_meteo[prod_columns[0]],
        name=prod_labels[0],
        mode="lines",
        yaxis="y1",
        line=dict(color="#1f4e79", width=3)
    ))

    fig.add_trace(go.Scatter(
        x=df_prod_meteo["Data"],
        y=df_prod_meteo[meteo_columns[0]],
        name=meteo_labels[0],
        mode="lines",
        yaxis="y2",
        line=dict(color="#c62828", width=2.5)
    ))

    buttons_prod = []
    for label, col in zip(prod_labels, prod_columns):
        buttons_prod.append(dict(
            label=label,
            method="restyle",
            args=[
                {
                    "y": [df_prod_meteo[col]],
                    "name": label
                },
                [0]
            ]
        ))

    buttons_meteo = []
    for label, col in zip(meteo_labels, meteo_columns):
        buttons_meteo.append(dict(
            label=label,
            method="restyle",
            args=[
                {
                    "y": [df_prod_meteo[col]],
                    "name": label
                },
                [1]
            ]
        ))

    fig.update_layout(
        #title="Produção Energética vs Condições Meteorológicas",
        template="plotly_white",
        width=1400,
        height=600,
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
        updatemenus=[
            dict(
                buttons=buttons_prod,
                direction="down",
                x=1.02,
                y=1.20,
                showactive=True,
                xanchor="left",
                yanchor="top"
            ),
            dict(
                buttons=buttons_meteo,
                direction="down",
                x=1.02,
                y=1.05,
                showactive=True,
                xanchor="left",
                yanchor="top"
            )
        ],
        annotations=[
            dict(
                text="Produção:",
                x=1.02,
                y=1.26,
                xref="paper",
                yref="paper",
                showarrow=False,
                align="left"
            ),
            dict(
                text="Meteorologia:",
                x=1.02,
                y=1.11,
                xref="paper",
                yref="paper",
                showarrow=False,
                align="left"
            )
        ],
        legend=dict(
            orientation="h",
            y=-0.3
        ),
        margin=dict(l=60, r=180, t=100, b=80)
    )
    return fig
