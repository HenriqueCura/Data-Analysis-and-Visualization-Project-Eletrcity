#%%
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

pio.renderers.default = "browser"

#%%
# =========================
# 1. CARREGAMENTO DOS DADOS
# =========================

df_diarios = pd.read_csv("data/dados_diarios.csv")
df_hora = pd.read_csv("data/dados_hora.csv")

df_diarios["Data"] = pd.to_datetime(df_diarios["Data"])
df_hora["Data"] = pd.to_datetime(df_hora["Data"])

df_diarios = df_diarios.sort_values("Data")
df_hora = df_hora.sort_values("Data")

#%%
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

#%%
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

print(df_meteo.head())

#%%
# =========================
# 4. SUNLIGHT
# =========================

fig_sun = px.line(
    df_meteo,
    x=df_meteo.index,
    y="Sunlight (em minutos)"
)

fig_sun.update_traces(line=dict(color="#f9a825", width=2.8))

fig_sun = beautify_figure(
    fig_sun,
    "Evolução da Luz Solar ao Longo do Tempo",
    "Minutos"
)

fig_sun.show()

#%%
# =========================
# 5. TEMPERATURA
# =========================

fig_temp = go.Figure()

fig_temp.add_trace(go.Scatter(
    x=df_meteo.index,
    y=df_meteo["temp_C_mean"],
    mode="lines",
    name="Temperatura média",
    line=dict(color="#5fa8d3", width=3)
))

fig_temp.add_trace(go.Scatter(
    x=df_meteo.index,
    y=df_meteo["temp_C_min"],
    mode="lines",
    name="Temperatura mínima",
    line=dict(color="#9fd3f2", width=2)
))

fig_temp.add_trace(go.Scatter(
    x=df_meteo.index,
    y=df_meteo["temp_C_max"],
    mode="lines",
    name="Temperatura máxima",
    line=dict(color="#0b3d91", width=2)
))

fig_temp = beautify_figure(
    fig_temp,
    "Evolução da Temperatura ao Longo do Tempo",
    "Temperatura (°C)"
)

fig_temp.show()

#%%
# =========================
# 6. VELOCIDADE DO VENTO
# =========================

fig_wind = go.Figure()

fig_wind.add_trace(go.Scatter(
    x=df_meteo.index,
    y=df_meteo["wind_speed_mean"],
    mode="lines",
    name="Vento médio",
    line=dict(color="#66bb6a", width=3)
))

fig_wind.add_trace(go.Scatter(
    x=df_meteo.index,
    y=df_meteo["wind_speed_min"],
    mode="lines",
    name="Vento mínimo",
    line=dict(color="#a5d6a7", width=2)
))

fig_wind.add_trace(go.Scatter(
    x=df_meteo.index,
    y=df_meteo["wind_speed_max"],
    mode="lines",
    name="Vento máximo",
    line=dict(color="#1b5e20", width=2)
))

fig_wind = beautify_figure(
    fig_wind,
    "Evolução da Velocidade do Vento ao Longo do Tempo",
    "Velocidade do vento"
)

fig_wind.show()

#%%
# =========================
# 7. PRECIPITAÇÃO
# =========================

fig_precip = px.line(
    df_meteo,
    x=df_meteo.index,
    y="precip_mm_sum"
)

fig_precip.update_traces(line=dict(color="#1565c0", width=2.8))

fig_precip = beautify_figure(
    fig_precip,
    "Evolução da Precipitação ao Longo do Tempo",
    "Precipitação (mm)"
)

fig_precip.show()

#%%
# =========================
# 8. COBERTURA DE NUVENS
# =========================

fig_cloud = go.Figure()

fig_cloud.add_trace(go.Scatter(
    x=df_meteo.index,
    y=df_meteo["mean_cloud"],
    mode="lines",
    name="Nebulosidade média",
    line=dict(color="#90a4ae", width=3)
))

fig_cloud.add_trace(go.Scatter(
    x=df_meteo.index,
    y=df_meteo["min_cloud"],
    mode="lines",
    name="Nebulosidade mínima",
    line=dict(color="#cfd8dc", width=2)
))

fig_cloud.add_trace(go.Scatter(
    x=df_meteo.index,
    y=df_meteo["max_cloud"],
    mode="lines",
    name="Nebulosidade máxima",
    line=dict(color="#455a64", width=2)
))

fig_cloud = beautify_figure(
    fig_cloud,
    "Evolução da Cobertura de Nuvens ao Longo do Tempo",
    "Cobertura de nuvens (%)"
)

fig_cloud.show()

#%%
# =========================
# 9. EVOLUÇÃO DO PREÇO DA ELETRICIDADE
# =========================

df_diarios["price_ma7"] = df_diarios["avg_price_eur_mwh"].rolling(window=7).mean()

fig_price = go.Figure()

fig_price.add_trace(go.Scatter(
    x=df_diarios["Data"],
    y=df_diarios["avg_price_eur_mwh"],
    mode="lines",
    name="Preço diário",
    line=dict(color="#8e24aa", width=2.5)
))

fig_price.add_trace(go.Scatter(
    x=df_diarios["Data"],
    y=df_diarios["price_ma7"],
    mode="lines",
    name="Média móvel 7 dias",
    line=dict(color="#000000", width=2.5, dash="dash")
))

fig_price = beautify_figure(
    fig_price,
    "Evolução do Preço da Eletricidade",
    "Preço médio diário (€/MWh)"
)

fig_price.show()

#%%
# =========================
# 10. PRODUÇÃO DIÁRIA TOTAL
# =========================

prod_diaria = (
    df_hora
    .groupby("Data", as_index=False)
    .sum(numeric_only=True)
)

#%%
# =========================
# 11. PRODUÇÃO TOTAL VS METEOROLOGIA COM FILTRO
# =========================

df_prod_meteo = pd.merge(
    prod_diaria,
    df_diarios,
    on="Data",
    how="inner"
)

meteo_cols = {
    "Temperatura média": "temp_C_mean",
    "Velocidade do vento": "wind_speed_mean",
    "Precipitação": "precip_mm_sum",
    "Nebulosidade média": "mean_cloud",
    "Luz solar": "Sunlight (em minutos)"
}

fig_dual = go.Figure()

fig_dual.add_trace(go.Scatter(
    x=df_prod_meteo["Data"],
    y=df_prod_meteo["Rede Distribuição (kWh)"],
    name="Produção Total - Rede Distribuição",
    mode="lines",
    yaxis="y1",
    line=dict(color="#1f4e79", width=3)
))

for i, (label, col) in enumerate(meteo_cols.items()):
    fig_dual.add_trace(go.Scatter(
        x=df_prod_meteo["Data"],
        y=df_prod_meteo[col],
        name=label,
        mode="lines",
        yaxis="y2",
        visible=(i == 0),
        line=dict(width=2.5)
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

fig_dual.update_layout(
    title="Produção Total vs Condições Meteorológicas",
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
        title="Produção Total - Rede Distribuição (kWh)",
        side="left",
        showgrid=True
    ),
    yaxis2=dict(
        title="Temperatura média",
        side="right",
        overlaying="y",
        showgrid=False
    ),
    updatemenus=[
        dict(
            buttons=buttons,
            direction="down",
            x=1.05,
            y=1.18,
            showactive=True
        )
    ],
    legend=dict(
        orientation="h",
        y=-0.3
    ),
    margin=dict(l=60, r=90, t=90, b=80)
)

fig_dual.show()

#%%
# =========================
# 11. PRODUÇÃO VS METEOROLOGIA COM DUPLO FILTRO
# =========================

# Criar produção diária a partir dos dados horários

df_hora = pd.read_csv("data/dados_hora.csv")
df_hora["Data"] = pd.to_datetime(df_hora["Data"])

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

#%%
# =========================
# 12. HEATMAP - MATRIZ DE CORRELAÇÃO
# =========================

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
# %%