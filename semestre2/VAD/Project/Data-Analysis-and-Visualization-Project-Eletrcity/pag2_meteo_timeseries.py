#%%
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

#%% =========================
# 1. CARREGAMENTO DOS DADOS
# =========================
# Le os ficheiros base: dados diarios com meteorologia e dados horarios com producao.
df_diarios = pd.read_csv("data/dados_diarios.csv")
df_hora = pd.read_csv("data/dados_hora.csv")

# Converte datas para datetime para permitir analise temporal correta.
df_diarios["Data"] = pd.to_datetime(df_diarios["Data"])
df_hora["Data"] = pd.to_datetime(df_hora["Data"])

# Ordena os registos para que as series temporais sejam desenhadas pela ordem certa.
df_diarios = df_diarios.sort_values("Data")
df_hora = df_hora.sort_values("Data")

def beautify_figure(fig,yaxis_title):
    # Aplica a configuracao visual comum aos graficos meteorologicos.
    fig.update_layout(
        template="plotly_white",
        #width=1400,
        #height=600,
        width=None, 
        autosize=True,
        #xaxis_title="Data",
        yaxis_title=yaxis_title,
        title_x=0.5,
        hovermode="x unified",
        #legend_title="Variáveis",
        font=dict(size=14),
        margin=dict(l=50, r=40, t=25, b=20),
        legend=dict(
            orientation="h",     # "h" de horizontal
            yanchor="bottom",
            y=-0.5,              # Posição vertical (abaixo do eixo X e do rangeslider)
            xanchor="center",
            x=0.5,
            traceorder='normal'               # Centrado horizontalmente        # Opcional: remover título da legenda se ocupar muito espaço
        )
    )

    fig.update_xaxes(
        showgrid=True,
        rangeslider_visible=True
    )

    fig.update_yaxes(showgrid=True)

    return fig


# Seleciona apenas as colunas meteorologicas usadas nesta pagina.
cols_meteo = [
    "Data",
    "Sunlight (em minutos)",
    "temp_C_mean",
    "temp_C_min",
    "temp_C_max",
    "wind_speed_mean",
    "wind_speed_min",
    "wind_speed_max",
    "precip_mm_sum",
    "mean_cloud",
    "min_cloud",
    "max_cloud"
]

# Cria uma copia so com meteorologia e usa Data como indice temporal.
df_meteo = df_diarios[cols_meteo].copy()
df_meteo.set_index("Data", inplace=True)


#%%
def create_weather_timeseries(tec:str):
    # Lista de opcoes que podem ser pedidas pelo dashboard/dropdown.
    tecs = ['sunlight','temperatura','nebulosidade','precipitacao','vento']

    # Garante que a funcao so recebe uma variavel meteorologica valida.
    if tec not in tecs:
        raise ValueError(f'Valor dado incorreto. Deve ser um de {tecs}')

    if tec == 'temperatura':
        # Grafico de temperatura com minimo, media e maximo.
        fig_temp = go.Figure()

        

        # Linha da temperatura minima.
        fig_temp.add_trace(go.Scatter(
            x=df_meteo.index,
            y=df_meteo["temp_C_min"],
            mode="lines",
            name="Temperatura mínima",
            line=dict(color="#9fd3f2", width=2)
        ))
        # Linha principal: temperatura media.
        fig_temp.add_trace(go.Scatter(
            x=df_meteo.index,
            y=df_meteo["temp_C_mean"],
            mode="lines",
            name="Temperatura média",
            line=dict(color="#5fa8d3", width=3)
        ))

        # Linha da temperatura maxima.
        fig_temp.add_trace(go.Scatter(
            x=df_meteo.index,
            y=df_meteo["temp_C_max"],
            mode="lines",
            name="Temperatura máxima",
            line=dict(color="#0b3d91", width=2)
        ))

        fig_temp = beautify_figure(
            fig_temp,
            #"Evolução da Temperatura ao Longo do Tempo",
            yaxis_title="Temperatura (°C)"
        )
        return fig_temp
    elif tec == 'vento':
        # Grafico da velocidade do vento com minimo, medio e maximo.
        fig_wind = go.Figure()

       

        # Linha do vento minimo.
        fig_wind.add_trace(go.Scatter(
            x=df_meteo.index,
            y=df_meteo["wind_speed_min"],
            mode="lines",
            name="Vento mínimo",
            line=dict(color="#a5d6a7", width=2)
        ))
        # Linha principal: vento medio.
        fig_wind.add_trace(go.Scatter(
            x=df_meteo.index,
            y=df_meteo["wind_speed_mean"],
            mode="lines",
            name="Vento médio",
            line=dict(color="#66bb6a", width=3)
        ))

        # Linha do vento maximo.
        fig_wind.add_trace(go.Scatter(
            x=df_meteo.index,
            y=df_meteo["wind_speed_max"],
            mode="lines",
            name="Vento máximo",
            line=dict(color="#1b5e20", width=2)
        ))

        fig_wind = beautify_figure(
            fig_wind,
            #"Evolução da Velocidade do Vento ao Longo do Tempo",
            yaxis_title="Velocidade do vento"
        )
        return fig_wind
    elif tec == 'precipitacao':
        # Grafico simples da precipitacao acumulada diaria.
        fig_precip = px.line(
                                df_meteo,
                                x=df_meteo.index,
                                y="precip_mm_sum"
                            )

        fig_precip.update_traces(line=dict(color="#1565c0", width=2.8))

        fig_precip = beautify_figure(
            fig_precip,
            #"Evolução da Precipitação ao Longo do Tempo",
            yaxis_title="Precipitação (mm)"
        )
        return fig_precip
    elif tec=='nebulosidade':
        # Grafico da cobertura de nuvens com minimo, media e maximo.
        fig_cloud = go.Figure()

        

        # Linha da nebulosidade minima.
        fig_cloud.add_trace(go.Scatter(
            x=df_meteo.index,
            y=df_meteo["min_cloud"],
            mode="lines",
            name="Nebulosidade mínima",
            line=dict(color="#cfd8dc", width=2)
        ))
        # Linha principal: nebulosidade media.
        fig_cloud.add_trace(go.Scatter(
            x=df_meteo.index,
            y=df_meteo["mean_cloud"],
            mode="lines",
            name="Nebulosidade média",
            line=dict(color="#90a4ae", width=3)
        ))

        # Linha da nebulosidade maxima.
        fig_cloud.add_trace(go.Scatter(
            x=df_meteo.index,
            y=df_meteo["max_cloud"],
            mode="lines",
            name="Nebulosidade máxima",
            line=dict(color="#455a64", width=2)
        ))

        fig_cloud = beautify_figure(
            fig_cloud,
            #"Evolução da Cobertura de Nuvens ao Longo do Tempo",
            yaxis_title="Cobertura de nuvens (%)"
        )
        return fig_cloud
    else:
        # Caso restante: grafico dos minutos diarios de luz solar.
        fig_sun = px.line(
                            df_meteo,
                            x=df_meteo.index,
                            y="Sunlight (em minutos)"
                        )

        fig_sun.update_traces(line=dict(color="#f9a825", width=2.8))

        fig_sun = beautify_figure(
            fig_sun,
            #"Evolução da Luz Solar ao Longo do Tempo",
            yaxis_title="Minutos"
        )
        return fig_sun
