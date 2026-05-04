import pandas as pd
import plotly.graph_objects as go

df = pd.read_csv("data/dados_diarios.csv")
df["Data"] = pd.to_datetime(df["Data"])
df = df.sort_values("Data")

def create_price_timeseries():
    df_price = df.copy()
    df_price["price_ma7"] = df_price["avg_price_eur_mwh"].rolling(window=7).mean()

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_price["Data"],
        y=df_price["avg_price_eur_mwh"],
        mode="lines",
        opacity=0.75,
        line=dict(width=1.8),
        name="Preço (€/MWh)"
    ))

    fig.add_trace(go.Scatter(
        x=df_price["Data"],
        y=df_price["price_ma7"],
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
    return fig
