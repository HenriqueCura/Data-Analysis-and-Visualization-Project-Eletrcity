import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import altair as alt

df_hora = pd.read_csv('data/dados_hora.csv')
df_diarios = pd.read_csv('data/dados_diarios.csv')


fig = go.Figure()
fig.add_trace(go.Scatter(x=df_hora["Data/Hora"], y=df_hora["Eólica (kWh)"],
    fill='toself',
    mode='lines',
    line_color='purple',
    ))
"""fig.add_trace(go.Scatter(
    x=[1, 2, 3, 4],
    y=[1, 6, 2, 6],
    fill='toself', # fill area between trace0 and trace1
    mode='lines', line_color='indigo'))"""

#fig.show()


def px_area():
    fig = px.area(df_hora, x="Data/Hora", y=[ "Hídrica (kWh)","Eólica (kWh)", "Fotovoltaica (kWh)", "Cogeração (kWh)",'Outras Tecnologias (kWh)'], title='Produção de Energia por Hora')
    fig.show()



def altair_area():
    df_melted = df_hora.melt(id_vars='Data/Hora', var_name='Tecnologia', value_name='kWh')
    chart = alt.Chart(df_melted).mark_area().encode(
        x='Data/Hora:T',
        y='kWh:Q',
        color='Tecnologia:N'
    ).properties(title='Produção de Energia por Hora').interactive()
    return chart

def altair_areaII():
    alt.Chart(df_hora).mark_area().encode(
    alt.X('Data/Hora:T').axis(format='%Y', domain=False, tickSize=0),
    alt.Y('sum(count):Q').stack('center').axis(None),
    alt.Color('series:N').scale(scheme='category20b')
    ).interactive()


def altair_areaIII():
    df_hora['Outras Tecnologias (kWh)'] = df_hora['Rede Distribuição (kWh)'] - (df_hora['Eólica (kWh)'] + df_hora['Fotovoltaica (kWh)'] + df_hora['Hídrica (kWh)'] )
    tecnologias = ['Eólica (kWh)', 'Fotovoltaica (kWh)', 'Hídrica (kWh)', 'Outras Tecnologias (kWh)']

    # 2. "Derreter" o DataFrame
    df_long = df_hora.melt(
        id_vars=['Data/Hora'], 
        value_vars=tecnologias,
        var_name='Tecnologia', 
        value_name='kWh'
    )

    # 3. Criar o gráfico
    chart = alt.Chart(df_long).mark_area().encode(
        x = alt.X('yearmonth(Data/Hora):T').axis(format='%b %Y', titleFontSize=16,   # Tamanho do título "Período Horário"
                labelFontSize=12,   # Tamanho de "00:00", "01:00", etc.
                labelAngle=-45,title='Mês/Ano'),
        y = alt.Y('sum(kWh):Q',
            title='Produção Total (kWh)', # O título vai aqui como argumento do Y
            axis=alt.Axis(
                titleFontSize=16,   # Tamanho do título do eixo
                labelFontSize=12,   # Tamanho dos valores (1G, 2G...)
                format='.2s'  )),      # Formatação simplificada
      
        
        # Adicionamos o 'sort' aqui para ordenar pela soma de kWh

        color = alt.Color('Tecnologia:N', legend=alt.Legend(titleFontSize=18, labelFontSize=16)).scale(scheme='inferno').sort(
            alt.EncodingSortField(field='kWh', op='sum', order='descending')
        ),
        
        order = alt.Order('sum(kWh):Q', sort='descending'),
        
        tooltip=['yearmonth(Data/Hora)', 'Tecnologia', 'sum(kWh)']
    ).properties(
        width='container',
        height=500,
    ).interactive()
    return chart


chart = altair_area()
chart.save('altairI.html')