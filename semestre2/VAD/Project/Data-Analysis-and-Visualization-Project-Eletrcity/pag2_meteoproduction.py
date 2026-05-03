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




def plot_sunlight(df):
    fig_sun = px.line(
        df,
        x=df.index,
        y="Sunlight (em minutos)",
        title="Luz Solar Diária",
        labels={"Sunlight (em minutos)": "Minutos de Luz Solar"}
    )
    fig_sun.update_layout(
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
    fig_sun.update_xaxes(
        showgrid=True,
        rangeslider_visible=True
    )
    fig_sun.update_yaxes(
        showgrid=True,
        range=[0, df["Sunlight (em minutos)"].max() + 30]
    )
    return fig_sun

df_prod_meteo = pd.merge(
    prod_diaria,
    df_diarios,
    on="Data",
    how="inner"
)

producao_cols = {
    "Produção total": "Rede Distribuição (kWh)",
    "Solar": "Solar (kWh)",
    "Eólica": "Eólica (kWh)",
    "Hídrica": "Hídrica (kWh)",
    "Biomassa": "Biomassa (kWh)"
}

meteo_cols = {
    "Temperatura média": "temp_C_mean",
    "Velocidade do vento": "wind_speed_mean",
    "Precipitação": "precip_mm_sum",
    "Nebulosidade média": "mean_cloud",
    "Luz solar": "Sunlight (em minutos)"
}

# Remover colunas que não existam no teu dataset
producao_cols = {
    label: col for label, col in producao_cols.items()
    if col in df_prod_meteo.columns
}

meteo_cols = {
    label: col for label, col in meteo_cols.items()
    if col in df_prod_meteo.columns
}

fig_dual = go.Figure()

prod_labels = list(producao_cols.keys())
prod_columns = list(producao_cols.values())

meteo_labels = list(meteo_cols.keys())
meteo_columns = list(meteo_cols.values())

# Linha de produção inicial
fig_dual.add_trace(go.Scatter(
    x=df_prod_meteo["Data"],
    y=df_prod_meteo[prod_columns[0]],
    name=prod_labels[0],
    mode="lines",
    yaxis="y1",
    line=dict(color="#1f4e79", width=3)
))

# Linha meteorológica inicial
fig_dual.add_trace(go.Scatter(
    x=df_prod_meteo["Data"],
    y=df_prod_meteo[meteo_columns[0]],
    name=meteo_labels[0],
    mode="lines",
    yaxis="y2",
    line=dict(color="#c62828", width=2.5)
))

# Dropdown produção
buttons_prod = []

for label, col in producao_cols.items():
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

# Dropdown meteorologia
buttons_meteo = []

for label, col in meteo_cols.items():
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

fig_dual.update_layout(
    title="Produção Energética vs Condições Meteorológicas",
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

fig_dual.show()
