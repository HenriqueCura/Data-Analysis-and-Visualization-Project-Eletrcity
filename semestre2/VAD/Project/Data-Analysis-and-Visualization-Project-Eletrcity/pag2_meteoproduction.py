import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

pio.renderers.default = "browser"

# =========================
# 1. CARREGAMENTO DOS DADOS
# =========================

df_diarios = pd.read_csv("data/dados_diarios.csv")
df_hora = pd.read_csv("data/dados_hora.csv")

df_diarios["Data"] = pd.to_datetime(df_diarios["Data"])
df_hora["Data"] = pd.to_datetime(df_hora["Data"])

df_diarios = df_diarios.sort_values("Data")
df_hora = df_hora.sort_values("Data")

# =========================
# 2. FUNÇÃO DE LAYOUT
# =========================

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

# =========================
# 3. SELEÇÃO DOS DADOS METEOROLÓGICOS
# =========================

cols_meteo = [
    "Data",
    "Sunlight (em minutos)",
    "temp_C_mean",
    "temp_C_min",
    "temp_C_max",
    "wind_speed_mean",
    "wind_speed_min",
    "wind_speed_max",
    "precip_mm_sum",
    "mean_cloud",
    "min_cloud",
    "max_cloud"
]

df_meteo = df_diarios[cols_meteo].copy()
df_meteo.set_index("Data", inplace=True)




def create_meteovsprod(meteo:str, tec:str):
