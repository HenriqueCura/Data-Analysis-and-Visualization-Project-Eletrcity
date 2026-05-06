import pandas as pd
from plotly import express as px



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
dfIII['Mes/Ano'] = dfIII['Mês'].astype(str) + '/' + dfIII['Ano'].astype(str)

def circular_total():
    r = "Producao"
    meses = dict(zip(range(1,13), ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']))
    anos_legend= list(dfIII.apply(
        lambda x: meses[x['Mês']]+ "/" + x['Ano'] if x['Mês'] %2 != 0 else "", axis=1
    ))
    anos_legend[-1] = "2026"
    fig = px.bar_polar(
        dfIII,
        r= r,
        theta="Mes/Ano",
        color="Tecnologia",
        labels="Ano",
        color_discrete_sequence=px.colors.qualitative.Set3).update_layout(
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
                    tickfont=dict(size=18, family='Arial', style='italic')
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
        margin=dict(b=30, t=80, l=100, r=60),
        )
    return fig
