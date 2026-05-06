import pandas as pd
from plotly import express as px



df_hora = pd.read_csv('data/dados_hora.csv')

df_hora['Outras Tecnologias (kWh)'] = df_hora['Rede Distribuição (kWh)'] - (df_hora['Eólica (kWh)'] + df_hora['Fotovoltaica (kWh)'] + df_hora['Hídrica (kWh)'] )
tecnologias = ['Eólica (kWh)', 'Fotovoltaica (kWh)', 'Hídrica (kWh)', 'Outras Tecnologias (kWh)']


dfII = df_hora.copy()
dfII = dfII.groupby(by=[dfII['Ano'],dfII['Mês']]).sum().reset_index()
dfII = dfII[['Ano', 'Mês', 'Eólica (kWh)', 'Fotovoltaica (kWh)', 'Hídrica (kWh)', 'Outras Tecnologias (kWh)']]
dfII['Ano'] = dfII['Ano'].astype(str)
dfII['Mes/Ano'] = dfII['Mês'].astype(str) + '/' + dfII['Ano'].astype(str)
anos_legend= list(dfII.apply(
    lambda x: str(x['Ano']) if x['Mês'] == 6 else "", axis=1
))
def create_circular_histogram(tec:str):
    if 'Fotovoltaica' in tec:
        cor = px.colors.sequential.Cividis_r
        st = 'black'
    elif 'Hídrica' in tec:
        cor = px.colors.sequential.GnBu_r
        st = 'white'
    else: 
        cor = px.colors.sequential.Teal_r
        st = 'white'
    r = tec
    if r not in dfII.columns:
        raise ValueError(f"Tecnologia '{r}' não encontrada. Opções: {dfII.columns[2:-1].tolist()}")
    anos_legend[-1] = "2026"
    fig = px.bar_polar(
        dfII,
        r= r,
        theta="Mes/Ano",
        color="Ano",
        labels="Ano",
        color_discrete_sequence=cor
    ).update_layout(
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
                    categoryarray=dfII['Mes/Ano'].tolist(),
                    categoryorder="array",
                    # O período tem de ser o número total de fatias para fechar o círculo
                    period=len(dfII),
                    tickvals=dfII['Mes/Ano'].tolist(),
                    # O texto é que leva a lista com vazios
                    ticktext=anos_legend,
                    direction="clockwise",
                    rotation=90,
                    tickfont=dict(size=18, family='Arial', style='italic')
            ),  
            radialaxis=dict(
                showticklabels=True, 
                range=[0, dfII[r].max()*1.02],
                nticks=5, 
                tickfont=dict(
                    size=15, 
                    family='Arial Black', # 'Arial Black' é naturalmente mais grossa e escura
                    color='black'         # Garante que não é o cinzento padrão
                ),
                linecolor='black',
                linewidth=1,
                layer='above traces', # Mudei para 'below' para as barras não taparem os números
                gridcolor='lightgrey'
            ),   
        ),
        
        height=600,
        width=700,
        margin=dict(b=30, t=80, l=50, r=0),
        )
    max_r = dfII[r].max() 


    return fig 