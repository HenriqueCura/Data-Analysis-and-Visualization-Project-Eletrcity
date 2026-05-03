import pandas as pd
import plotly.express as px

# =========================
# CARREGAR DADOS
# =========================
df_diarios = pd.read_csv("data/dados_diarios.csv")
df_hora = pd.read_csv("data/dados_hora.csv")

# Converter datas
df_diarios["Data"] = pd.to_datetime(df_diarios["Data"])
df_hora["Data"] = pd.to_datetime(df_hora["Data"])

# =========================
# CRIAR PRODUÇÃO DIÁRIA
# =========================
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
    "Rede Distribuição (kWh)",
    "Solar (kWh)",
    "Eólica (kWh)",
    "Hídrica (kWh)",
    "Biomassa (kWh)"
]

cols_corr = [col for col in cols_corr if col in df_corr.columns]

df_corr = df_corr[cols_corr].copy()

corr_matrix = df_corr.corr()

fig_heatmap = px.imshow(
    corr_matrix,
    text_auto=".2f",
    color_continuous_scale="RdBu_r",
    zmin=-1,
    zmax=1,
    title="Matriz de Correlação entre Meteorologia e Produção Energética"
)

fig_heatmap.update_layout(
    template="plotly_white",
    width=1100,
    height=900,
    title_x=0.5,
    xaxis_title="Variáveis",
    yaxis_title="Variáveis",
    font=dict(size=13),
    margin=dict(l=80, r=80, t=90, b=120)
)

fig_heatmap.update_xaxes(tickangle=45)

fig_heatmap.show()