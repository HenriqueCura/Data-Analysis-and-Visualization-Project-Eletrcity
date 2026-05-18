import pandas as pd
import plotly.graph_objects as go

# Carrega os dados diarios, onde esta a coluna do preco medio da eletricidade.
df = pd.read_csv("data/dados_diarios.csv")

# Converte e ordena a data para desenhar uma serie temporal correta.
df["Data"] = pd.to_datetime(df["Data"])
df = df.sort_values("Data")

def create_price_timeseries():
    # Trabalha numa copia para nao alterar diretamente o dataframe global.
    df_price = df.copy()

    # Calcula a media movel de 7 dias para mostrar a tendencia suavizada do preco.
    df_price["price_ma7"] = df_price["avg_price_eur_mwh"].rolling(window=7).mean()

    fig = go.Figure()

    # Linha principal com o preco diario real.
    fig.add_trace(go.Scatter(
        x=df_price["Data"],
        y=df_price["avg_price_eur_mwh"],
        mode="lines",
        opacity=0.75,
        line=dict(width=1.8),
        name="Preço (€/MWh)"
    ))

    # Linha tracejada com a media movel semanal.
    fig.add_trace(go.Scatter(
        x=df_price["Data"],
        y=df_price["price_ma7"],
        mode="lines",
        name="Média móvel (7 dias)",
        line=dict(dash="dash")
    ))

    # Define titulos dos eixos e tema visual do grafico.
    fig.update_layout(
        #title="Evolução do Preço da Eletricidade",
        xaxis_title="Data",
        yaxis_title="Preço (€/MWh)",
        template="plotly_white"
    )

    # Coloca a legenda por baixo do grafico e aumenta a margem inferior para nao cortar texto.
    fig.update_layout(
    legend=dict(
        orientation="h",     # Define a orientação como Horizontal
        yanchor="bottom",    # Ancora a legenda pela parte de baixo
        y=-0.3,              # Posição vertical (valores negativos empurram para baixo do eixo X)
        xanchor="left",    # Ancora a legenda pelo centro horizontal
        x=0,               # Posiciona no centro do gráfico (0 a 1)
        font=dict(
            family="Arial",      # Opcional: mudar a fonte
            size=14,             # Aumenta aqui o tamanho (ex: 18 ou 20)
            color="black"        # Cor do texto
        ),),
    # Aumentar a margem inferior para a legenda não ser cortada
    margin=dict(b=100,r=20) 
)

    return fig
