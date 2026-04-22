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

chart = altair_area()
chart.save('altairI.html')