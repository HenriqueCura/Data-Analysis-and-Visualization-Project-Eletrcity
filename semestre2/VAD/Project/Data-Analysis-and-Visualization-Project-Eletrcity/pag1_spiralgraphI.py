import numpy as np
import plotly.graph_objects as go
import pandas as pd

dfIII = pd.read_csv('data/dados_producao_mesano.csv')


def create_spiral_histogram(tec:str):
    if tec not in dfIII['Tecnologia'].unique():
        df_spiral = pd.DataFrame(columns=dfIII.columns)
        i = 0
        for mes,ano in dfIII[['Mês','Ano']].drop_duplicates().values:
            total = dfIII[(dfIII['Ano']==ano) & (dfIII['Mês']==mes)].copy()
            total = total.loc[:,'Producao'].sum()
            df_spiral.loc[i,:] = [ano,mes,'Total',total,str(mes)+'/'+str(ano)]
            i+=1
    else:
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
    cores_anos = {
    2023: "#636EFA", # Azul (Centro)
        2024: "#00CC96", # Verde (Meio)
        2025: "#EC9026", # Laranja (Exterior)
        "Total": "#AB63FA"}


    for year in df_spiral['Ano'].unique():
        """
        temp = df_spiral[df_spiral['Ano'] == year]
        
        cor_do_ano = cores_anos.get(year, "grey")

        fig.add_trace(go.Barpolar(
            r=temp['Producao'] / norm_factor, # Comprimento da barra
            # Convertemos meses (1-12) em graus (0-360) para ocupar o círculo todo
            theta=temp['Mês'] * (360/12), 
            base=temp['r_base']*2,          # O SEGREDO: O offset que cria a espiral
            name=str(year),
            customdata=temp['Producao'],
            hovertemplate="<b>Produção:</b> %{customdata:.3e} kWh<extra></extra>",
            thetaunit="degrees",
            marker=dict(color=cor_do_ano),
            
        ))"""
        temp = df_spiral[df_spiral['Ano'] == year]
    
        # 1. Calcular métricas e formatar strings (podes usar a função format_energy que sugeri antes)
        v_min = temp['Producao'].min()
        v_max = temp['Producao'].max()
        
        # Criar o nome detalhado para a legenda
        # .2g é mais amigável que .2e, mas usa o que preferires
        legend_name=f"<b>Ano {year}</b><br>Mín: {v_min:.2e}<br>Máx: {v_max:.2e}"
        
        # Pegar na cor uma única vez
        cor_do_ano = cores_anos.get(year, "grey")

        # 2. ADICIONAR APENAS UM TRACE POR ANO
        fig.add_trace(go.Barpolar(
            r=temp['Producao'] / norm_factor,
            theta=temp['Mês'] * (360/12), 
            base=temp['r_base'] * 2,
            name=legend_name,               # Usamos logo o nome detalhado aqui
            marker=dict(color=cor_do_ano),
            customdata=temp['Producao'],
                hovertemplate="<b>Produção:</b> %{customdata:.3e} kWh<extra></extra>",
                #<b>Ano: {year}</b><br>" "Mês: %{theta}°<br>"
            
            thetaunit="degrees"
        ))

    # --- 3. Layout Estilizado (Limpo e Focado na Forma) ---
    r_real_max = (df_spiral['r_base'].max() * 2) + (df_spiral['Producao'].max() / norm_factor)
    fig.update_layout(
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
        legend=dict(
        itemclick=False,      # Desativa o clique simples (esconder/mostrar)
        itemdoubleclick=False # Desativa o duplo clique (isolar um ano)
    ),
        height=650,
        width=800,
        margin=dict(b=30, t=80, l=20, r=20),
    )
    fig.update_layout(
    legend=dict(
        orientation="v",       # Vertical para ler melhor os intervalos
        yanchor="middle",
        y=0.8,
        xanchor="left",
        x=1,                 # Afasta um pouco do gráfico
        font=dict(size=15),
        itemsizing='constant',   # Mantém os ícones uniformes
        itemwidth=30
    ),
    margin=dict(r=100)         # Aumenta a margem para a legenda caber
)
    return fig