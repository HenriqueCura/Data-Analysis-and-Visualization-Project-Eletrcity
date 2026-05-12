import pandas as pd
from plotly import express as px
from plotly import graph_objects as go
import numpy as np



df_hora = pd.read_csv('data/dados_hora.csv')

df_hora['Outras Tecnologias (kWh)'] = df_hora['Rede Distribuição (kWh)'] - (df_hora['Eólica (kWh)'] + df_hora['Fotovoltaica (kWh)'] + df_hora['Hídrica (kWh)'] )
tecnologias = ['Eólica (kWh)', 'Fotovoltaica (kWh)', 'Hídrica (kWh)', 'Outras Tecnologias (kWh)']


# O melt mantém 'Ano' e 'Mês' e transforma as colunas de tecnologia em linhas
df_new = df_hora.melt(
    id_vars=['Ano', 'Mês'], 
    value_vars=tecnologias,
    var_name='Tecnologia', 
    value_name='Producao'
)

dfIII = df_new.groupby(by=[df_new['Ano'],df_new['Mês'],df_new['Tecnologia']]).sum().reset_index()

dfIII['Ano'] = dfIII['Ano'].astype(str)
dfIII = dfIII.sort_values(['Ano', 'Mês'])
dfIII['Mes/Ano'] = dfIII['Mês'].astype(str) + '/' + dfIII['Ano'].astype(str)
dfIII['Mes/AnoII'] = dfIII['Mês'].astype(str) + '/' + dfIII['Ano'].astype(str)
dfIII = dfIII.iloc[:-10,:]

def circular_total():
    r = "Producao"
    meses = dict(zip(range(1,13), ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']))
    
    anos_legend= list(dfIII.apply(
        lambda x: meses[x['Mês']]+ "/" + x['Ano'] if x['Mês'] %2 != 0 else "", axis=1
    ))
    anos_legend = [
    f"{a.split('/')[0]}/<b>{a.split('/')[1]}</b>" if '/' in a and 'Jan' in a 
    else a 
    for a in anos_legend
]
    
    fig = px.bar_polar(
        dfIII,
        r= r,
        theta="Mes/Ano",
        color="Tecnologia",
        labels="Ano",
        
        color_discrete_sequence=
        px.colors.qualitative.Set3).update_layout(
            barmode="stack",
            showlegend=True,
            coloraxis_showscale=False,
            
        legend=dict(
            title="Ano de Produção",
            font=dict(size=12),
            # Isto garante que a legenda não fica preta se o fundo for escuro
            itemsizing='constant' 
        ),
        polar=dict(hole = 0.2,
            angularaxis=dict(
                    type="category",
                    # IMPORTANTE: O array de categorias tem de ser a coluna theta completa
                    categoryarray=dfIII['Mes/Ano'].tolist(),
                    categoryorder="array",
                    # O período tem de ser o número total de fatias para fechar o círculo
                    period=len(dfIII['Mes/Ano'].unique()),
                    tickvals=dfIII['Mes/Ano'].tolist(),
                    # O texto é que leva a lista com vazios
                    ticktext=anos_legend,
                    direction="clockwise",
                    rotation=90,
                    tickfont=dict(size=18, family='Arial')
            ),  
            radialaxis=dict(
                showticklabels=True, 
                range=[0, dfIII[r].max()*1.02],
                nticks=5, 
                tickfont=dict(size=18, family='Arial'),
                linecolor='black', 
                linewidth=1,
                layer='above traces', # Mudei para 'below' para as barras não taparem os números
                gridcolor='lightgrey'
            ),   
        ),
        
        height=700,
        width=800,
        margin=dict(b=30, t=80, l=120, r=50),
        )
    max_r = dfIII[r].max()
    for i in range(-1,len(dfIII) - 1):
        ano_atual = dfIII.iloc[i]['Ano']
        ano_proximo = dfIII.iloc[i+1]['Ano']
        
        if ano_atual != ano_proximo:
            # Pegamos no nome da categoria atual (Dezembro) 
            # e na categoria seguinte (Janeiro)
            cat_dez = dfIII.iloc[i]['Mes/Ano']
            cat_jan = dfIII.iloc[i+1]['Mes/Ano']
            
            # Adicionamos a linha usando os nomes das categorias
            # O Plotly desenha a linha na transição entre estas duas
            fig.add_trace(go.Scatterpolar(
                r=[0, max_r * 1.1],
                # Usar a categoria de Janeiro como ponto de referência
                theta=[cat_jan, cat_jan], 
                mode='lines',
                line=dict(color='grey', width=2, dash='dash'),
                hoverinfo='none',
                showlegend=False
            ))
    for trace in fig.data:
    # Filtramos o dataframe apenas para a tecnologia deste trace
        tec_name = trace.name
        dados_focados = dfIII[dfIII['Tecnologia'] == tec_name]
        
        trace.customdata = dados_focados[['Mes/AnoII']]
        trace.hovertemplate = (
            "<b>Tecnologia:</b> %{fullData.name}<br>" +
            "<b>Data:</b> %{customdata[0]}<br>" +
            "<b>Produção:</b> %{r:.2e} kWh<extra></extra>"
        )
    return fig
