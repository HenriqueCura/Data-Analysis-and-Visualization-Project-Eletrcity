import unicodedata

import pandas as pd
import plotly.express as px

df_diarios = pd.read_csv("data/dados_diarios.csv")
df_hora = pd.read_csv("data/dados_hora.csv")

df_diarios["Data"] = pd.to_datetime(df_diarios["Data"])
df_hora["Data"] = pd.to_datetime(df_hora["Data"])

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

def create_correlation_heatmap():
    prod_diaria = (
        df_hora
        .groupby("Data", as_index=False)
        .sum(numeric_only=True)
    )

    df_corr = pd.merge(
        df_diarios,
        prod_diaria,
        on="Data",
        how="inner"
    )

    cols_corr = [
        "temp_C_mean",
        "wind_speed_mean",
        "precip_mm_sum",
        "mean_cloud",
        "Sunlight (em minutos)",
        _find_column(df_corr, "rede", "distribuicao"),
        _find_column(df_corr, "solar"),
        _find_column(df_corr, "fotovoltaica"),
        _find_column(df_corr, "eolica"),
        _find_column(df_corr, "hidrica")
    ]

    cols_corr = [col for col in cols_corr if col and col in df_corr.columns]
    df_corr = df_corr[cols_corr].copy()

    corr_matrix = df_corr.corr()

    fig = px.imshow(
        corr_matrix,
        text_auto=".2f",
        color_continuous_scale="RdBu_r",
        zmin=-1,
        zmax=1,
        #title="Matriz de Correlação entre Meteorologia e Produção Energética"
    )

    fig.update_layout(
        template="plotly_white",
        #width=1400,
        #height=600,
        width=None, 
        autosize=True,
        title_x=0.5,
        xaxis_title="Variáveis",
        yaxis_title="Variáveis",
        font=dict(size=13),
        margin=dict(l=0, r=80, t=90, b=120)
    )

    fig.update_xaxes(tickangle=45)
    return fig
