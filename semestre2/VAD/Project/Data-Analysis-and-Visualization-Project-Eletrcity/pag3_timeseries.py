#%%
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

#%%
# Ler dados
df = pd.read_csv("data/dados_diarios.csv")
df["Data"] = pd.to_datetime(df["Data"])
df = df.sort_values("Data")

fig = go.Figure()

# Linha principal (preço)
fig.add_trace(go.Scatter(
    x=df["Data"],
    y=df["avg_price_eur_mwh"],
    mode="lines",
    name="Preço (€/MWh)"
))

# Adicionar a média móvel
df["price_ma7"] = df["avg_price_eur_mwh"].rolling(window=7).mean()

fig.add_trace(go.Scatter(
    x=df["Data"],
    y=df["price_ma7"],
    mode="lines",
    name="Média móvel (7 dias)",
    line=dict(dash="dash")
))

fig.update_layout(
    title="Evolução do Preço da Eletricidade",
    xaxis_title="Data",
    yaxis_title="Preço (€/MWh)",
    template="plotly_white"
)

fig.show()
# %%
