from pathlib import Path

import pandas as pd
import plotly.graph_objects as go


# Determina a pasta do ficheiro para carregar dados com caminho robusto.
try:
    BASE_DIR = Path(__file__).resolve().parent
except NameError:
    BASE_DIR = Path.cwd()

DATA_DIR = BASE_DIR / "data"

# Carrega os dados diarios, onde existem preco e variaveis meteorologicas.
df_diarios = pd.read_csv(DATA_DIR / "dados_diarios.csv")
df_diarios["Data"] = pd.to_datetime(df_diarios["Data"])
df_diarios = df_diarios.sort_values("Data")


# Nome da coluna do preco medio diario usada na comparacao.
PRICE_COL = "avg_price_eur_mwh"

# Configuracoes das variaveis meteorologicas disponiveis no dropdown.
# Cada entrada define label, coluna, unidade, cor e nomes alternativos aceites.
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

# Configuracao da serie de preco usada sempre no streamgraph.
PRICE_CONFIG = {
    "label": "Preco medio diario",
    "column": PRICE_COL,
    "unit": "EUR/MWh",
    "color": "#c62828"
}

# Opcoes prontas para usar num dropdown de interface.
dropdown_options_preco_meteo = [
    {"label": "Temperatura media", "value": "temperatura"},
    {"label": "Precipitacao", "value": "precipitacao"},
    {"label": "Nebulosidade media", "value": "nebulosidade"},
    {"label": "Luz solar", "value": "luz"},
    {"label": "Vento medio", "value": "vento"}
]


def _normalize_key(value):
    # Converte a escolha do utilizador para uma chave interna valida.
    if value is None:
        return "temperatura"

    value = str(value).strip().casefold()

    for key, config in METEO_CONFIGS.items():
        if value == key or value in config["aliases"]:
            return key

    valid_values = list(METEO_CONFIGS)
    raise ValueError(f"Condicao invalida. Usa uma de: {valid_values}")


def _selected_meteo_config(condition):
    # Obtem a configuracao da variavel meteorologica escolhida e valida a coluna.
    condition = _normalize_key(condition)
    config = METEO_CONFIGS[condition]

    if config["column"] not in df_diarios.columns:
        raise ValueError(f"A coluna {config['column']} nao existe nos dados.")

    return config


def _scale_0_1(series):
    # Normaliza uma serie para o intervalo 0-1, permitindo comparar unidades diferentes.
    min_value = series.min()
    max_value = series.max()

    if pd.isna(min_value) or pd.isna(max_value) or max_value == min_value:
        return series * 0

    return (series - min_value) / (max_value - min_value)


def _prepare_mirror_data(condition="temperatura", rolling_window=7):
    # Prepara os dados que vao ser desenhados no streamgraph espelhado.
    meteo_config = _selected_meteo_config(condition)
    configs = [PRICE_CONFIG, meteo_config]
    cols = ["Data", PRICE_CONFIG["column"], meteo_config["column"]]

    # Mantem apenas as colunas necessarias e remove linhas sem valores.
    stream_df = df_diarios[cols].dropna().copy()

    for config in configs:
        col = config["column"]
        scaled_col = f"{col}_scaled"

        # Normaliza cada variavel para tornar preco e meteorologia comparaveis.
        stream_df[scaled_col] = _scale_0_1(stream_df[col].astype(float))

        # Aplica media movel para suavizar ruido diario, se pedido.
        if rolling_window and rolling_window > 1:
            stream_df[scaled_col] = (
                stream_df[scaled_col]
                .rolling(window=rolling_window, min_periods=1)
                .mean()
            )

    # Preco fica positivo; meteorologia fica negativa para criar efeito espelhado.
    stream_df["Preco_normalizado"] = stream_df[f"{PRICE_CONFIG['column']}_scaled"]
    stream_df["Meteo_normalizado"] = -stream_df[f"{meteo_config['column']}_scaled"]

    return stream_df, meteo_config


def create_preco_meteocond_streamgraph(condicao="temperatura", rolling_window=7):
    # Cria os dados normalizados de acordo com a condicao escolhida.
    stream_df, meteo_config = _prepare_mirror_data(
        condition=condicao,
        rolling_window=rolling_window
    )

    fig = go.Figure()
    price_col = PRICE_CONFIG["column"]
    meteo_col = meteo_config["column"]

    # Area superior: preco normalizado, mantendo o valor real no hover.
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

    # Area inferior: variavel meteorologica normalizada e invertida.
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

    # Linha central que separa preco e meteorologia.
    fig.add_hline(
        y=0,
        line_width=1.2,
        line_color="#2F3A45"
    )

    # Etiqueta visual para a metade superior do grafico.
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

    # Etiqueta visual para a metade inferior do grafico.
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

    # Layout final: legenda, range slider e escala fixa entre -1 e 1.
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
    # Alias em ingles para reutilizar a mesma funcao noutros ficheiros.
    return create_preco_meteocond_streamgraph(
        condicao=condicao,
        rolling_window=rolling_window
    )


def create_streamgraph_preco_meteocond(condicao="temperatura", rolling_window=7):
    # Alias com outro nome para manter compatibilidade com chamadas existentes.
    return create_preco_meteocond_streamgraph(
        condicao=condicao,
        rolling_window=rolling_window
    )


if __name__ == "__main__":
    # Teste local com a condicao meteorologica temperatura.
    fig = create_preco_meteocond_streamgraph("temperatura")
    fig.show()
