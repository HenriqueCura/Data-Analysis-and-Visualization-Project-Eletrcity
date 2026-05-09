import dash
from dash import dcc, html, Input, Output
import dash_vega_components as dvc
from pag1_streamgraph import altair_areaIII
from pag1_circulargraphI import circular_total
from pag1_circulargraphII import create_circular_histogram
from pag1_spiralgraphI import create_spiral_histogram
from pag1_areagraph_month_year import area_month_year_interval
from pag2_meteo_timeseries import create_weather_timeseries
from pag2_heatmap import create_correlation_heatmap
from pag2_meteoproduction import create_meteovsprod
from pag3_priceseries import create_price_timeseries
from pag3_calendarbased import create_cluster_calendar_visualization
import altair as alt
import dash_bootstrap_components as dbc
import dash_loading_spinners as dls; from helpers import get_new_graph

alt.data_transformers.enable("vegafusion")

app = dash.Dash(__name__,external_stylesheets=[dbc.themes.BOOTSTRAP],suppress_callback_exceptions=True)
# Sem isto, o Dash recusa-se a carregar o layout porque vê uma callback a apontar para o dropdown-tecnologia 
# que não está no layout principal.


### Carregar gráficos que não precisam de variáveis para serem inicializados
streamgraph = altair_areaIII()
circularI = circular_total()
precos = create_price_timeseries()
heatmap = create_correlation_heatmap()
calendar = create_cluster_calendar_visualization()


app.layout = html.Div([
    # --- BARRA DE TOPO ---
    html.Div([
        # Título do Dashboard
        html.H3("Energia PT", style={
            'display': 'inline-block', 
            'margin': '0 40px 0 0', 
            'verticalAlign': 'middle',
            'fontFamily': 'Arial'
        }),
        
        # Contentor das Tabs
        html.Div([
            dcc.Tabs(id="tabs-menu", value='tab-1', children=[
                dcc.Tab(label='Sazonalidade', value='tab-1', 
                        style={'padding': '25px'},  # espaço para o limite superior
                        selected_style={'padding': '40px'}),  # espaço para o limite superior quanfo é a tab selecionada
                
                dcc.Tab(label='Condições meteorológicas', value='tab-2', 
                        style={'padding': '15px'}, 
                        selected_style={'padding': '25px'}),
                
                dcc.Tab(label='Preço', value='tab-3', 
                        style={'padding': '25px'}, 
                        selected_style={'padding': '40px'}),
            ], style={'height': '90px','width':'600px'}) # tamanhos de cada tab
        ], style={'display': 'inline-block', 'verticalAlign': 'middle'})
        
    ], style={
        'padding': '25px 40px', # espaço dos tabs para os lados (cima e baixo Y esquerda e direita)
        'backgroundColor': 'white', 
        'borderBottom': '1px solid #ddd', # espaço de uma barra de fronteira cinzenta 
        'display': 'flex',        
        'alignItems': 'center'    
    }),

    # --- CONTEÚDO ---
    # Este Div será preenchido pela callback 'render_content'
    html.Div(id='tabs-content', style={'padding': '40px'}),
])

# quando se clica em cada tab
@app.callback(
    Output('tabs-content', 'children'),
    Input('tabs-menu', 'value')   )


def render_content(tab):
    if tab == 'tab-1':
        return html.Div([html.H1('Sazonalidade na produção energética em Portugal', style={'textAlign': 'center'}),
                         html.H4(),
                        html.H4('Evolução da produção energética discriminada por tipo em Portugal'),
                         # title=f"Evolução da produção energética em {dic_month[month]} de {year}"
                         dvc.Vega(id='streamgraph_prods', spec=streamgraph.to_dict(format='vega'),
                                  opt={'actions': False} ),          # Desativa o menu de exportação e ver código), # gráfico streamgraph das produções
    dbc.Container([ # container para poder colocar dois gráficos lado a lado
    dbc.Row([
        dbc.Col([
            #title=f"Evolução Mensal da Produção de Energia: {r}",
            html.H4("Evolução Mensal por Tecnologia de produção"),
            dcc.Graph(
                id='grafico-circular-geral',
                figure=circularI # gráfico carregado em cima
            )
        ], width=6), # Ocupa metade do ecrã

        dbc.Col([
            
            # title=f"Evolução Mensal da Produção de Energia: {r}",
            
            # Dropdown para selecionar a tecnologia do gráfico da direita
            html.Div([
                html.Label("Selecione a Tecnologia:"),
                dcc.Dropdown(
                    id='dropdown-tecnologia', # id para se usar no callback
                    options=[  # opções para o dropdown
                        {'label': 'Eólica', 'value': 'Eólica (kWh)'},
                        {'label': 'Fotovoltaica', 'value': 'Fotovoltaica (kWh)'},
                        {'label': 'Hídrica', 'value': 'Hídrica (kWh)'}
                    ],
                    value='Eólica (kWh)', # valor default selecionado
                    clearable=False,
                    style={
                'width': '40%' # tamanho da barra, diminuido para não ocupar muito espaço com palavras pequenas
            }
                ),
            ], style={'marginBottom': '25px'}), # espaço para baixo do dropdown
            html.H4(id='titulo-evolucao', style={'textAlign': 'left'}),
            #html.H4(f"Evolução Mensal por {dropdown-tecnologia}", style={'textAlign': 'left'}),
            # Espaço para o gráfico detalhado
            dcc.Graph(id='grafico_tec'),
            
        ])
    ])
], fluid=True),
dbc.Container([
    dbc.Row([
        # --- COLUNA ESQUERDA ---
        dbc.Col([
            html.Div([
                html.H4("Spiral Histogram: Evolução da produção selecionada"),
                html.Label("Selecione a Tecnologia:"),
                dcc.Dropdown(
                    id='dropdown-tecnologiaII',
                    options=[
                        {'label': 'Total das produções','value':'total'},
                        {'label': 'Eólica', 'value': 'Eólica (kWh)'},
                        {'label': 'Fotovoltaica', 'value': 'Fotovoltaica (kWh)'},
                        {'label': 'Hídrica', 'value': 'Hídrica (kWh)'}
                    ],
                    value='total',
                    clearable=False,
                    style={'width': '40%'} # Ajustado para preencher a coluna
                ),
            ], style={'marginBottom': '20px'}),
            #title=
            
            dcc.Graph(id='spiral',config={
        'displayModeBar': False,  # Esconde a toolbar permanentemente
    }),
        ], width=6),

        # --- COLUNA DIREITA ---
        dbc.Col([
            html.H4("Evolução Mensal da Produção de Energia em Portugal"),
            
            # Zona de Filtros
            html.Div([
                html.Label("Filtros de Data e Intervalo:"),
                dbc.Row([
                    # Mês
                    dbc.Col([
                        dcc.Dropdown(
                            id='dropdown-mes',
                            options=[{'label': m.capitalize(), 'value': str(i+1)} 
                                     for i, m in enumerate(['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'])],
                            value='1',
                            placeholder="Mês"
                        )
                    ], width=4),

                    # Ano
                    dbc.Col([
                        dcc.Dropdown(
                            id='radio-ano',
                            options=[{'label': '2023', 'value': '2023'},
                                {'label': '2024', 'value': '2024'},
                                {'label': '2025', 'value': '2025'},],
                            value='2023',
                            clearable=False,
                        )
                    ], width=4),

                    # Intervalo
                    dbc.Col([
                        
                        dcc.Dropdown(
                            id='dropdown-intervalo',
                            options=[
                                {'label': '15 min', 'value': '15m'},
                                {'label': '1 hora', 'value': '1h'},
                                {'label': '4 horas', 'value': '4h'},
                                {'label': '12 horas', 'value': '12h'},
                                {'label': '1 dia', 'value': '1d'},
                            ],
                            value='15m',
                            clearable=False,
                        )
                    ], width=4)
                ], className="g-12") # 'g-2' adiciona um pequeno espaçamento entre colunas
            ], style={'marginBottom': '20px'}),

             # Título principal
            #"subtitle": "Distribuição horária por tecnologia", # Subtítulo opcional
            #"fontSize": 30,
            #"anchor": "middle", # Alinha o título à esquerda
            #"color": "black
            # Gráfico de Área
            dcc.Graph(id='area_graph')
            
        ], width=6)
    ])
], fluid=True)
]       
)
    elif tab == 'tab-2':
        return html.Div([html.H4("Condições meteorológicas em Portugal"),
                         html.Label("Selecione o dado meteorológico:"),
                dcc.Dropdown(
                    id='dropdown-meteoI',
                    options=[
                        {'label': 'Luz diária de sol','value':'sunlight'},
                        {'label': 'Temperatura', 'value': 'temperatura'},
                        {'label': 'Velocidade do vento', 'value': 'vento'},
                        {'label': 'Precipitação diária (em mm)', 'value': 'precipitacao'},
                        {'label': 'Nebulosidade', 'value': 'nebulosidade'}
                    ],
                    value='temperatura',
                    clearable=False,
                    style={'width': '30%'} # Ajustado para preencher a coluna
                ),
            dcc.Graph(id='timeseries_tempo',style={'width': '100%', 'height': '600px'}),
            html.H4("Correlações entre as diferentes produções e fatores meteorológicos"),
            dcc.Graph(id='heatmap', figure=heatmap ,style={'width': '100%', 'height': '700px'},config={'displayModeBar': False,}),
            html.H4("Fator meteorológico vs produção"),
            dbc.Row([
                    # Mês
                    dbc.Col([
                        dcc.Dropdown(
                    id='dropdown-meteoII',
                    className="mb-4",
                    options=[
                        {'label': 'Luz diária de sol','value':'sunlight'},
                        {'label': 'Temperatura', 'value': 'temperatura'},
                        {'label': 'Velocidade do vento', 'value': 'vento'},
                        {'label': 'Precipitação diária (em mm)', 'value': 'precipitacao'},
                        {'label': 'Nebulosidade', 'value': 'nebulosidade'}
                    ],
                    value='temperatura',
                    clearable=False,
                    style={'width': '90%'} # Ajustado para preencher a coluna
                )],width=2),

                    # Ano
                    dbc.Col([
                        dcc.Dropdown(
                    id='dropdown-tecnologiaIII',
                    options=[
                        {'label': 'Total das produções','value':'total'},
                        {'label': 'Eólica', 'value': 'Eólica (kWh)'},
                        {'label': 'Fotovoltaica', 'value': 'Fotovoltaica (kWh)'},
                        {'label': 'Hídrica', 'value': 'Hídrica (kWh)'}
                    ],
                    value='total',
                    clearable=False,
                    style={'width': '90%'} # Ajustado para preencher a coluna
                ),
                    ], width=2),
            ], style={'marginBottom': '0px','marginTop':'250px'}),
            
            dcc.Graph(id='meteovsprod',style={'width': '100%', 'height': '600px'}),
            
            
                         
                         
                         
                         
                         
                         
                         ])
    elif tab == 'tab-3':
        return html.Div([
            html.H4("Menu de análise dos preços da eletricidade"),
            dcc.Graph(id='grafico-precos', figure=precos),
            dbc.Row([
    html.H4('Agrupamento dos preços para o ano selecionado'),
    dbc.Col([
        dcc.Dropdown(
            id='dropdown-cluster',
            options=[
                {'label': 'Preços mais baixos', 'value': 'baixos'},
                {'label': 'Preços intermédios', 'value': 'medios'},
                {'label': 'Preços mais altos', 'value': 'altos'},
            ],
            value='baixos',
            clearable=False,
            style={'width': '90%'}
        )
    ], width=4), # Aqui estava o erro: o width deve estar DENTRO do parêntese da Col
    
    dbc.Col([
        dcc.Dropdown(
            id='dropdown-anoII',
            options=[
                {'label': '2023', 'value': '2023'},
                {'label': '2024', 'value': '2024'},
                {'label': '2025', 'value': '2025'},
            ],
            value='2023',
            clearable=False,
            style={'width': '90%'}
        )
    ], width=4), 
]), # Fechamento da Row
            dcc.Graph(id='calendar'),
        ])





@app.callback(
    Output('grafico_tec', 'figure'),
    Input('dropdown-tecnologia', 'value')
)
def update_circular_graph(tecnologia_escolhida):
    if not tecnologia_escolhida:
        raise dash.exceptions.PreventUpdate
        
    # Chamamos a tua função de histograma espiral
    fig = create_circular_histogram(tecnologia_escolhida)
    return fig


@app.callback(
    Output('spiral', 'figure'),
    Input('dropdown-tecnologiaII', 'value')
)
def update_circular_graph(tecnologia_escolhida):
    if not tecnologia_escolhida:
        raise dash.exceptions.PreventUpdate
        
    # Chamamos a tua função de histograma espiral
    fig = create_spiral_histogram(tecnologia_escolhida)

    return fig
    

@app.callback(
    Output('area_graph', 'figure'),
    [Input('dropdown-mes', 'value'),
     Input('dropdown-intervalo', 'value'),
     Input('radio-ano', 'value')]
)
def update_circular_graph(mes_escolhido, intervalo_escolhido,ano_escolhido):
    # 1. Prevenção básica
    if not mes_escolhido or not ano_escolhido:
        raise dash.exceptions.PreventUpdate

    # 2. Conversão para inteiro (importante se o teu DF usa int e os inputs são str)
    mes = int(mes_escolhido)
    ano = int(ano_escolhido)

    # 3. Gerar o gráfico passando os dois parâmetros
    # Certifica-te de que a tua função 'create_circular_histogram' aceita (mes, ano)
    fig = area_month_year_interval(mes,ano,intervalo_escolhido)
    
    return fig

@app.callback(
    Output('timeseries_tempo', 'figure'),
    Input('dropdown-meteoI', 'value')
)
def update_weather_timeseries(tecnologia_escolhida):
    if not tecnologia_escolhida:
        raise dash.exceptions.PreventUpdate
        
    # Chamamos a tua função de histograma espiral
    fig = create_weather_timeseries(tecnologia_escolhida)
    return fig




@app.callback(
    Output('meteovsprod', 'figure'),
    [Input('dropdown-meteoII', 'value'),
    Input('dropdown-tecnologiaIII', 'value')]
)
def update_weather_timeseries(meteo_escolhido,tecnologia_escolhida):
    if not tecnologia_escolhida or not meteo_escolhido:
        raise dash.exceptions.PreventUpdate
        
    # Chamamos a tua função de histograma espiral
    fig = create_meteovsprod(meteo_escolhido,tecnologia_escolhida)
    return fig

@app.callback(
    Output('titulo-evolucao', 'children'),
    Input('dropdown-tecnologia', 'value')
)
def update_title(tecnologia_selecionada):
    # Remove o '(kWh)' apenas para o título ficar mais limpo visualmente
    nome_limpo = tecnologia_selecionada.replace(' (kWh)', '')
    
    return f"Evolução Mensal: {nome_limpo}"

@app.callback(
    Output('calendar', 'figure'),
    [Input('dropdown-cluster', 'value'),
    Input('dropdown-anoII', 'value')]
)
def update_cluster_price(cluster,ano):
    if not cluster or not ano:
        raise dash.exceptions.PreventUpdate
        
    ano = int(ano)
    # Chamamos a tua função de histograma espiral
    fig = create_cluster_calendar_visualization(cluster,ano)
    return fig


if __name__ == '__main__':
    app.run(debug=True)