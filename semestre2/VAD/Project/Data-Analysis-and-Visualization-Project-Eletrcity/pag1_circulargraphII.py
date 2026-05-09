import pandas as pd
from plotly import express as px
import re
import colorsys


df_hora = pd.read_csv('data/dados_hora.csv')

df_hora['Outras Tecnologias (kWh)'] = df_hora['Rede Distribuição (kWh)'] - (df_hora['Eólica (kWh)'] + df_hora['Fotovoltaica (kWh)'] + df_hora['Hídrica (kWh)'] )
tecnologias = ['Eólica (kWh)', 'Fotovoltaica (kWh)', 'Hídrica (kWh)', 'Outras Tecnologias (kWh)']

cores = px.colors.qualitative.Set3[:4] 
cores_dic = {k:v for k,v in zip(tecnologias,cores)}

dfII = df_hora.copy()
dfII = dfII.groupby(by=[dfII['Ano'],dfII['Mês']]).sum().reset_index()
dfII = dfII[['Ano', 'Mês', 'Eólica (kWh)', 'Fotovoltaica (kWh)', 'Hídrica (kWh)', 'Outras Tecnologias (kWh)']]
dfII['Ano'] = dfII['Ano'].astype(str)
dfII = dfII.sort_values(['Ano', 'Mês'])
dfII['Mes/Ano'] = dfII['Mês'].astype(str) + '/' + dfII['Ano'].astype(str)
dfII['Mes/AnoII'] = dfII['Mês'].astype(str) + '/' + dfII['Ano'].astype(str)
anos_legend= list(dfII.apply(
    lambda x: str(x['Ano']) if x['Mês'] == 6 else "", axis=1
))

def ajustar_intensidade(rgb, fator):
    canais = re.findall(r'\d+', rgb)
    r, g, b = [int(c) for c in canais]
    
    # 2. Converter RGB (0-255) para HLS (0.0-1.0)
    h, l, s = colorsys.rgb_to_hls(r/255.0, g/255.0, b/255.0)
    
    # 3. Ajustar a luminosidade (Lightness)
    # fator > 1 clareia, fator < 1 escurece
    l = max(0, min(1, l * fator))
    
    # 4. Converter de volta para o formato string "rgb(r,g,b)"
    r_new, g_new, b_new = [round(x * 255) for x in colorsys.hls_to_rgb(h, l, s)]
    return f"rgb({r_new},{g_new},{b_new})"


def create_circular_histogram(tec:str):
    cor = cores_dic[tec]
    r = tec
    if r not in dfII.columns:
        raise ValueError(f"Tecnologia '{r}' não encontrada. Opções: {dfII.columns[2:-1].tolist()}")
    anos_legend[-1] = "2026"
    n_anos = dfII['Ano'].nunique()
    cores = [ajustar_intensidade(cor,1-(f*0.3)) for f in range(n_anos)]
    fig = px.bar_polar(
        dfII,
        r= r,
        theta="Mes/Ano",
        color="Ano",
        labels="Ano",
        color_discrete_sequence=cores
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
        #margin=dict(b=30, t=20, l=1080, r=30),
        )
    fig.update_layout(
    legend=dict(
        orientation="v",       # Vertical para ler melhor os intervalos
        yanchor="middle",
        y=0.9,
        xanchor="left",
        x=1.1,                 # Afasta um pouco do gráfico
        font=dict(size=15),
        itemsizing='constant',   # Mantém os ícones uniformes
        itemwidth=30
    ),
    margin=dict(r=100)         # Aumenta a margem para a legenda caber
)
    for trace in fig.data:
        # trace.name contém o Ano (pois color="Ano" no px.bar_polar)
        ano_do_trace = trace.name
        
        # Filtramos o dataframe dfII para conter apenas os meses desse Ano específico
        dados_ano = dfII[dfII['Ano'] == ano_do_trace]
        
        # Atribuímos o customdata filtrado apenas com os meses/anos corretos desse trace
        trace.customdata = dados_ano[['Mes/AnoII']]
        
        trace.hovertemplate = (
            "<b>Ano:</b> " + ano_do_trace + "<br>" +
            "<b>Mês/Ano:</b> %{customdata[0]}<br>" +
            "<b>Produção:</b> %{r:.2e} kWh<extra></extra>"
        )

    return fig 