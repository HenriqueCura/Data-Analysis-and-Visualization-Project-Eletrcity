#%%
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

pio.renderers.default = "browser"   # útil no VS Code

#%%
file_path = "data/dados_diarios.csv"
df = pd.read_csv(file_path)

#%%
df["Data"] = pd.to_datetime(df["Data"])

#%%
cols = [
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

df_filtered = df[cols].copy()
df_filtered.set_index("Data", inplace=True)

print(df_filtered.head())

#%%
# Função para melhorar o layout geral
def beautify_figure(fig, title, yaxis_title):
    fig.update_layout(
        title=title,
        template="plotly_white",
        width=1400,          # estica o gráfico
        height=500,
        xaxis_title="Data",
        yaxis_title=yaxis_title,
        title_x=0.5,         # centra o título
        hovermode="x unified",
        legend_title="Variáveis",
        font=dict(size=14),
        margin=dict(l=50, r=40, t=70, b=50)
    )

    fig.update_xaxes(
        showgrid=True,
        rangeslider_visible=True   # barra de navegação no eixo X
    )

    fig.update_yaxes(showgrid=True)

    return fig

#%%
# 1) SUNLIGHT
fig_sun = px.line(
    df_filtered,
    x=df_filtered.index,
    y="Sunlight (em minutos)",
    title="Sunlight ao longo do tempo"
)

fig_sun.update_traces(line=dict(color="#1f4e79", width=2.5))
fig_sun = beautify_figure(fig_sun, "Sunlight ao longo do tempo", "Minutos")
fig_sun.show()

#%%
# 2) TEMPERATURA - tons de azul
fig_temp = go.Figure()

fig_temp.add_trace(go.Scatter(
    x=df_filtered.index,
    y=df_filtered["temp_C_mean"],
    mode="lines",
    name="Temperatura média",
    line=dict(color="#5fa8d3", width=3)
))

fig_temp.add_trace(go.Scatter(
    x=df_filtered.index,
    y=df_filtered["temp_C_min"],
    mode="lines",
    name="Temperatura mínima",
    line=dict(color="#9fd3f2", width=2)
))

fig_temp.add_trace(go.Scatter(
    x=df_filtered.index,
    y=df_filtered["temp_C_max"],
    mode="lines",
    name="Temperatura máxima",
    line=dict(color="#0b3d91", width=2)
))

fig_temp = beautify_figure(fig_temp, "Temperatura ao longo do tempo", "°C")
fig_temp.show()

#%%
# 3) WIND SPEED - tons de verde
fig_wind = go.Figure()

fig_wind.add_trace(go.Scatter(
    x=df_filtered.index,
    y=df_filtered["wind_speed_mean"],
    mode="lines",
    name="Vento médio",
    line=dict(color="#66bb6a", width=3)
))

fig_wind.add_trace(go.Scatter(
    x=df_filtered.index,
    y=df_filtered["wind_speed_min"],
    mode="lines",
    name="Vento mínimo",
    line=dict(color="#a5d6a7", width=2)
))

fig_wind.add_trace(go.Scatter(
    x=df_filtered.index,
    y=df_filtered["wind_speed_max"],
    mode="lines",
    name="Vento máximo",
    line=dict(color="#1b5e20", width=2)
))

fig_wind = beautify_figure(fig_wind, "Velocidade do vento ao longo do tempo", "m/s")
fig_wind.show()

#%%
# 4) PRECIPITAÇÃO
fig_precip = px.line(
    df_filtered,
    x=df_filtered.index,
    y="precip_mm_sum",
    title="Precipitação ao longo do tempo"
)

fig_precip.update_traces(line=dict(color="#1565c0", width=2.5))
fig_precip = beautify_figure(fig_precip, "Precipitação ao longo do tempo", "mm")
fig_precip.show()

#%%
# 5) CLOUD - tons de cinzento/azul
fig_cloud = go.Figure()

fig_cloud.add_trace(go.Scatter(
    x=df_filtered.index,
    y=df_filtered["mean_cloud"],
    mode="lines",
    name="Cloud média",
    line=dict(color="#90a4ae", width=3)
))

fig_cloud.add_trace(go.Scatter(
    x=df_filtered.index,
    y=df_filtered["min_cloud"],
    mode="lines",
    name="Cloud mínima",
    line=dict(color="#cfd8dc", width=2)
))

fig_cloud.add_trace(go.Scatter(
    x=df_filtered.index,
    y=df_filtered["max_cloud"],
    mode="lines",
    name="Cloud máxima",
    line=dict(color="#455a64", width=2)
))

fig_cloud = beautify_figure(fig_cloud, "Cobertura de nuvens ao longo do tempo", "%")
fig_cloud.show()

# %% 
# 2 Parte
df_hora = pd.read_csv("data/dados_hora.csv")
df_diarios = pd.read_csv("data/dados_diarios.csv")

df_hora["Data"] = pd.to_datetime(df_hora["Data"])
df_diarios["Data"] = pd.to_datetime(df_diarios["Data"])

# Produção diária: soma da Rede Distribuição por dia
prod_diaria = (
    df_hora
    .groupby("Data", as_index=False)["Rede Distribuição (kWh)"]
    .sum()
)

df = pd.merge(prod_diaria, df_diarios, on="Data", how="inner")

meteo_cols = {
    "Temperatura média": "temp_C_mean",
    "Velocidade do vento": "wind_speed_mean",
    "Precipitação": "precip_mm_sum",
    "Nebulosidade média": "mean_cloud",
    "Luz solar": "Sunlight (em minutos)"
}

fig = go.Figure()

# Linha fixa: produção
fig.add_trace(go.Scatter(
    x=df["Data"],
    y=df["Rede Distribuição (kWh)"],
    name="Produção Total - Rede Distribuição",
    mode="lines",
    yaxis="y1"
))

# Linhas meteorológicas, só uma visível de cada vez
for i, (label, col) in enumerate(meteo_cols.items()):
    fig.add_trace(go.Scatter(
        x=df["Data"],
        y=df[col],
        name=label,
        mode="lines",
        yaxis="y2",
        visible=(i == 0)
    ))

buttons = []

for i, (label, col) in enumerate(meteo_cols.items()):
    visible = [True] + [False] * len(meteo_cols)
    visible[i + 1] = True

    buttons.append(dict(
        label=label,
        method="update",
        args=[
            {"visible": visible},
            {
                "title": f"Produção Total vs {label}",
                "yaxis2.title": label
            }
        ]
    ))

fig.update_layout(
    title="Produção Total vs Condições Meteorológicas",
    xaxis_title="Data",

    yaxis=dict(
        title="Produção Total - Rede Distribuição (kWh)",
        side="left"
    ),

    yaxis2=dict(
        title="Temperatura média",
        side="right",
        overlaying="y"
    ),

    updatemenus=[
        dict(
            buttons=buttons,
            direction="down",
            x=1.05,
            y=1.15,
            showactive=True
        )
    ],

    legend=dict(
        orientation="h",
        y=-0.25
    )
)

fig.show()
# %%
# HEATMAP - Juntar meteorologia diária + produção diária 
df_corr = pd.merge(df_diarios, prod_diaria, on="Data", how="inner")

cols_escolhidas = [
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

cols_escolhidas = [col for col in cols_escolhidas if col in df_corr.columns]

df_corr = df_corr[cols_escolhidas]
corr_matrix = df_corr.corr()

df_hora = pd.read_csv("data/dados_hora.csv")
df_diarios = pd.read_csv("data/dados_diarios.csv")

df_hora["Data"] = pd.to_datetime(df_hora["Data"])
df_diarios["Data"] = pd.to_datetime(df_diarios["Data"])

# Agregar produções horárias para valores diários
prod_diaria = (
    df_hora
    .groupby("Data", as_index=False)
    .sum(numeric_only=True)
)

# Remover colunas que não queres na correlação
cols_remover = [
    "Data",
    "Preço",
    "Preco",
    "price",
    "Preço Eletricidade",
    "Preço da Eletricidade"
]

df_corr = df_corr.drop(
    columns=[col for col in cols_remover if col in df_corr.columns],
    errors="ignore"
)

# Manter só colunas numéricas
df_corr = df_corr.select_dtypes(include="number")

# Matriz de correlação
corr_matrix = df_corr.corr()

fig = px.imshow(
    corr_matrix,
    text_auto=".2f",
    color_continuous_scale="RdBu_r",
    zmin=-1,
    zmax=1,
    title="Matriz de Correlação entre Meteorologia e Produção Energética"
)

fig.update_layout(
    width=1000,
    height=850,
    xaxis_title="Variáveis",
    yaxis_title="Variáveis"
)

fig.show()
