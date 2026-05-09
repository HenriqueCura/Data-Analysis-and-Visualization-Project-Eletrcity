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
        #title="Evolução do Preço da Eletricidade",
        xaxis_title="Data",
        yaxis_title="Preço (€/MWh)",
        template="plotly_white"
    )
    fig.update_layout(
    legend=dict(
        orientation="h",     # Define a orientação como Horizontal
        yanchor="bottom",    # Ancora a legenda pela parte de baixo
        y=-0.3,              # Posição vertical (valores negativos empurram para baixo do eixo X)
        xanchor="center",    # Ancora a legenda pelo centro horizontal
        x=0.5,               # Posiciona no centro do gráfico (0 a 1)
        font=dict(
            family="Arial",      # Opcional: mudar a fonte
            size=14,             # Aumenta aqui o tamanho (ex: 18 ou 20)
            color="black"        # Cor do texto
        ),),
    # Aumentar a margem inferior para a legenda não ser cortada
    margin=dict(b=100,r=20) 
)

    return fig
