import numpy as np
import plotly.graph_objects as go
import pandas as pd

dfIII = pd.read_csv('data/dados_producao_mesano.csv')


def create_spiral_histogram(tec:str):
    if tec not in dfIII['Tecnologia'].unique():
        raise ValueError(f"Tecnologia '{tec}' não encontrada. Opções: {dfIII['Tecnologia'].unique()}")
    df_spiral = dfIII[dfIII['Tecnologia'] == tec]
    df_spiral = df_spiral[['Ano', 'Mês', 'Producao']]
    max_val = df_spiral['Producao'].max()
    df_spiral = df_spiral.sort_values(['Ano', 'Mês']).reset_index(drop=True)
    df_spiral = df_spiral[:-1]
    df_spiral = df_spiral[:-1]

    # r_base faz com que cada mês suba um pouco, criando a espiral
    df_spiral['r_base'] = np.arange(len(df_spiral)) * 20
    df_spiral['r_top'] = df_spiral['r_base'] + (df_spiral['Producao'] / max_val * 5)

    # Normalizamos a produção para que o tamanho das barras seja proporcional
    # mas caiba bem na espiral.
    norm_factor = df_spiral['Producao'].max() * 0.05

    # Criamos a base da barra que aumenta continuamente com o tempo (a espiral)
    # Isto faz com que Dezembro e Janeiro do ano seguinte não se sobreponham.
    df_spiral['r_base'] = np.arange(len(df_spiral)) 
    df_spiral['r_top'] = df_spiral['r_base'] + (df_spiral['Producao'] / norm_factor)

    r_total_max = df_spiral['r_top'].max()

    # --- 2. Criação do Gráfico ---
    fig = go.Figure()

    # Usamos loops para adicionar cores e rótulos de forma organizada
    # (Simulando o interaction(month, year) do R)
    for year in df_spiral['Ano'].unique():
        temp = df_spiral[df_spiral['Ano'] == year]
        
        fig.add_trace(go.Barpolar(
            r=temp['Producao'] / norm_factor, # Comprimento da barra
            # Convertemos meses (1-12) em graus (0-360) para ocupar o círculo todo
            theta=temp['Mês'] * (360/12), 
            base=temp['r_base']*2,          # O SEGREDO: O offset que cria a espiral
            name=str(year),
            customdata=temp['Producao'],
            hovertemplate="<b>Produção:</b> %{customdata:.2e} kWh<extra></extra>",
            thetaunit="degrees"
        ))

        r_texto = (temp['r_base'] * 2) + (temp['Producao'] / norm_factor)

        fig.add_trace(go.Scatterpolar(
        r=r_texto,
        theta=temp['Mês'] * (360/12),
        mode='text',
        text=temp['Producao'],
        # Usamos texttemplate para o Plotly formatar automaticamente (k, M, G)
        texttemplate="%{text:.4s}", 
        textposition="top center",
        textfont=dict(
            size=12, # Tamanho menor para não amontoar na parte de dentro da espiral
            #color="black",
            family="Arial Black"
        ),
        hoverinfo='none',
        showlegend=False,
        # Importante: colocar o texto numa camada acima
        cliponaxis=False))

    # --- 3. Layout Estilizado (Limpo e Focado na Forma) ---
    r_real_max = (df_spiral['r_base'].max() * 2) + (df_spiral['Producao'].max() / norm_factor)
    fig.update_layout(
        title="Spiral Histogram: Evolução da Produção Eólica (kWh)",
        font_size=16,
        polar=dict(
            hole=0.2,
            angularaxis=dict(
                # Configuração para que os nomes dos meses apareçam nos graus certos
                tickvals=[30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330, 360],
                ticktext=['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez'],
                direction="clockwise",
                rotation=120, # Janeiro no topo
                tickfont=dict(size=20, family='Arial', style='italic')
            ),
            # Removemos os círculos e números do eixo radial para ficar limpo
            
            radialaxis=dict(
                showticklabels=False,
                ticks="",
                showline=False,
                range=[0, r_real_max * 1.05] # Dá espaço para as últimas barras
            )
        ),
        # Mostramos a legenda apenas para os anos
        showlegend=True,
        #legend=dict(title="Ano de Produção", font=dict(size=12), itemsizing='constant'),
        legend=dict(
        itemclick=False,      # Desativa o clique simples (esconder/mostrar)
        itemdoubleclick=False # Desativa o duplo clique (isolar um ano)
    ),
        height=600,
        width=700,
        margin=dict(b=30, t=80, l=0, r=10),
    )
    return fig