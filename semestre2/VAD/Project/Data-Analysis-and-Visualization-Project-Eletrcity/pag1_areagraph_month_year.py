import pandas as pd
import plotly.express as px
from datetime import datetime

df_hora = pd.read_csv('data/dados_hora.csv')
df_hora['Resto (kWh)'] = df_hora['Rede Distribuição (kWh)'] - (df_hora['Eólica (kWh)'] + 
                                                               df_hora['Fotovoltaica (kWh)'] + df_hora['Hídrica (kWh)'] )

def area_month_year_interval(month:int,year:int,interval:str):
    if month not in list(range(1,13)):
        raise ValueError('Mês deve estar no intervalo [1,12]')
    if year not in [2023,2024,2025,2026]:
        raise ValueError('Ano deve ser 2023,2024,2025 ou 2026!')
    if interval not in ['15m','1h','4h','12h','1d']:
        raise ValueError("Interval deve ser um de: ['15m','1h','4h','12h','1d']")
    dic_month = {1:'janeiro',2:'fevereiro',3:'março',4:'abril',5:'maio',6:'junho',7:'julho',8:'agosto',9:'setembro',10:'outubro',
                 11:'novembro',12:'dezembro'}
    df_group = df_hora[(df_hora['Ano']==year) & (df_hora['Mês']==month)].copy()
    if interval in ['1h','4h','12h']:
        horas = df_group['Data/Hora'].str.split('T').str[1].str.split(':').str[0].astype(int).rename('Hora_int')
        if interval in ['4h','12h']:
            inter = int(interval[:-1])
            """
            for hora in range(len(horas)):
                val = hora
                first = None
                if val%inter==0:
                    first = val
                else:
                    hora = first"""
            horas = (horas // inter) * inter
        df_group = df_group.groupby(by=[df_group['Dia'],horas]).sum()
        series = pd.Series([datetime(year,month,day,hour) for day,hour in df_group.index ])
    elif interval == '1d':
        df_group = df_group.groupby(by=df_group['Dia']).sum()
        series = pd.Series([datetime(year,month,day) for day in df_group.index ])

    else:
        horas = df_group['Data/Hora'].str.split('T').str[1].str.split(':').str[0].astype(int).rename('Hora_int')
        minutos = df_group['Data/Hora'].str.split('T').str[1].str.split(':').str[1].astype(int).rename('Minutos_int') 
        df_group = df_group.groupby(by=[df_group['Dia'],horas,minutos]).sum()
        series = pd.Series([datetime(year,month,day,hour,minute) for day,hour,minute in df_group.index ])
    
    df_group = df_group.reset_index()
    df_group = df_group[['Eólica (kWh)','Fotovoltaica (kWh)','Hídrica (kWh)','Resto (kWh)']]
    df_group['Hora'] = series
    tecnologias = ['Eólica (kWh)', 'Fotovoltaica (kWh)', 'Hídrica (kWh)', 'Resto (kWh)']
    df_group.sort_values(by='Hora')
# O melt mantém 'Ano' e 'Mês' e transforma as colunas de tecnologia em linhas
    df_new = df_group.melt(
    id_vars=['Hora'], 
    value_vars=tecnologias,
    var_name='Tecnologia', 
    value_name='Producao'
)
    fig = px.area(df_new, x="Hora", y="Producao", color="Tecnologia", 
                   
                  color_discrete_sequence=px.colors.qualitative.Set3) 
    fig.update_layout(
    height=600,  # Altura em pixéis
    width=850    # Largura em pixéis (podes remover para ser responsivo)
)
    fig.update_layout(
    legend=dict(
        orientation="h",     # Define a orientação como Horizontal
        yanchor="bottom",    # Ancora a legenda pela parte de baixo
        y=-0.3,              # Posição vertical (valores negativos empurram para baixo do eixo X)
        xanchor="center",    # Ancora a legenda pelo centro horizontal
        x=0.5,               # Posiciona no centro do gráfico (0 a 1)
        title_text=""        # Opcional: remove o título "Tecnologia" para poupar espaço vertical
    ),
    # Aumentar a margem inferior para a legenda não ser cortada
    margin=dict(b=100,r=20) 
)
    return fig