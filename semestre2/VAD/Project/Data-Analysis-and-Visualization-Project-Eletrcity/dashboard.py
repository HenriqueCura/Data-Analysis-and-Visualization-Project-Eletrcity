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
from pag3_precometeocond import create_preco_meteocond_streamgraph
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
        # 1. Título do Dashboard
        html.H3("Energia em Portugal", style={
            'margin': '0 40px 0 0', 
            'fontFamily': 'Arial',
            'whiteSpace': 'nowrap'
        }),
        
        # 2. Contentor das Tabs
        html.Div([
            dcc.Tabs(id="tabs-menu", value='tab-1', children=[
                dcc.Tab(label='Sazonalidade', value='tab-1', 
                        style={'padding': '15px'}, 
                        selected_style={'padding': '15px', 'fontWeight': 'bold'}),
                
                dcc.Tab(label='Meteorologia', value='tab-2', 
                        style={'padding': '15px'}, 
                        selected_style={'padding': '15px', 'fontWeight': 'bold'}),
                
                dcc.Tab(label='Preço', value='tab-3', 
                        style={'padding': '15px'}, 
                        selected_style={'padding': '15px', 'fontWeight': 'bold'}),
            ], style={'height': '50px', 'width': '500px'})
        ]),

        html.Details([
            html.Summary(
                "ℹ", 
                style={
                    'listStyle': 'none',
                    'outline': 'none',
                    'borderRadius': '50%',
                    'width': '30px',
                    'height': '30px',
                    'border': '1px solid #ccc',
                    'backgroundColor': '#f8f9fa',
                    'cursor': 'pointer',
                    'fontSize': '16px',
                    'textAlign': 'center',
                    'lineHeight': '28px',
                    'userSelect': 'none',
                }
            ),
            html.Div([
                html.H5("Energia em Portugal", style={'margin': '0 0 10px 0', 'fontSize': '14px', 'fontWeight': 'bold'}),
                html.P("Desenvolvido para a unidade curricular de VAD.", style={'margin': '0 0 5px 0', 'fontSize': '13px'}),
                html.Hr(style={'margin': '10px 0'}),
                html.B("Autores:", style={'fontSize': '13px'}),
                html.Ul([
                    html.Li("Henrique Cura - MIACD ", style={'fontSize': '12px'}),
                    html.Li("Ricardo Pina - MIACD ", style={'fontSize': '12px'})
                ], style={'margin': '5px 0 0 0', 'paddingLeft': '20px'})
            ], style={
                'position': 'absolute',
                'right': '40px',        # Alinha com a margem direita da barra de topo
                'top': '65px',          # Aparece logo abaixo da barra
                'backgroundColor': 'white',
                'border': '1px solid #ddd',
                'borderRadius': '6px',
                'padding': '15px',
                'boxShadow': '0 4px 12px rgba(0,0,0,0.1)',
                'width': '280px',
                'zIndex': '2000'        # Fica por cima dos gráficos
            })
        ], style={'position': 'relative', 'marginLeft': 'auto'}) # O marginLeft: auto faz a magia de empurrar para a direita
        
    ], style={
        'padding': '10px 40px',
        'backgroundColor': 'white', 
        'borderBottom': '1px solid #ddd',
        'display': 'flex',        
        'alignItems': 'center'    
    }),

    # --- CONTEÚDO ---
    dcc.Loading(
        id="loading-tabs",
        type="cube", 
        fullscreen=True, # True para ocupar o ecrã todo
        children=html.Div(id='tabs-content', style={'padding': '40px'}),
        color="#119DFF",
    )
])



# quando se clica em cada tab
@app.callback(
    Output('tabs-content', 'children'),
    Input('tabs-menu', 'value')   )


def render_content(tab):
    if tab == 'tab-1':
        return html.Div([html.H1('Sazonalidade na produção energética em Portugal', style={'textAlign': 'left'}),
                        html.H4('Evolução da produção energética discriminada por tipo em Portugal'),
                         # title=f"Evolução da produção energética em {dic_month[month]} de {year}"
                         dvc.Vega(id='streamgraph_prods', spec=streamgraph.to_dict(format='vega'),
                                  opt={'actions': False} ),          # Desativa o menu de exportação e ver código), # gráfico streamgraph das produções
dbc.Container([
    dbc.Row([

        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Evolução Mensal por Tecnologia de produção", className="card-title"),
                    dcc.Graph(
                        id='grafico-circular-geral',
                        figure=circularI,
                        config={'displayModeBar': False},
                        style={'height': '500px', 'width': '100%'}
                    )
                ], style={'overflow': 'hidden'})
            ], style={'minHeight': '772px'}, color="secondary", outline=True)
        ], width=6), 


        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Análise Detalhada por Tecnologia", className="card-title"),
                    
                    
                    html.Div([
                        html.Label("Selecione a Tecnologia:"),
                        dcc.Dropdown(
                            id='dropdown-tecnologia',
                            options=[
                                {'label': 'Eólica', 'value': 'Eólica (kWh)'},
                                {'label': 'Fotovoltaica', 'value': 'Fotovoltaica (kWh)'},
                                {'label': 'Hídrica', 'value': 'Hídrica (kWh)'}
                            ],
                            value='Eólica (kWh)',
                            clearable=False,
                            style={'width': '40%'} 
                        ),
                    ], style={'marginBottom': '15px'}),

                    html.H4(id='titulo-evolucao', style={'textAlign': 'left', 'fontSize': '16px'}),
                    
                    dcc.Graph(
                        id='grafico_tec',
                        config={'displayModeBar': False},
                        style={
        'width': '80%',       
        'marginLeft': 'auto', # Margem automática à esquerda
        'marginRight': 'auto' # Margem automática à direita
    }
                    )
                ])
            ], color="secondary", outline=True)
        ], width=6) # Largura 6 para ocupar a outra metade
    ])
], fluid=True, style={'marginTop': '20px'}),
dbc.Container([
    dbc.Row([
       
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Comparação de cada mês para um tipo de produção", className="card-title"),
                    
                    html.Div([
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
                            style={'width': '40%'} 
                        ),
                    ], style={'marginBottom': '20px'}),
                    html.H6('(coloque o cursor por cima de cada barra para descobrir o valor da produção)',style={'fontWeight': 'normal'}),
                    dcc.Graph(
                        id='spiral',
                        config={'displayModeBar': False}
                    ),
                ])
            ], color="secondary", outline=True)
        ], width=6),

        
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Evolução num mês da Produção de Energia em Portugal", className="card-title"),
                    
                    
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
                                             {'label': '2025', 'value': '2025'}],
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
                        ], className="g-2") # 'g-2' para espaçamento horizontal entre os dropdowns
                    ], style={'marginBottom': '20px'}),
                    html.H5(id='aviso-falta', style={'textAlign': 'left', 'fontSize': '16px','italic':'True'}),
                    dcc.Graph(id='area_graph')
                ])
            ], style={'minHeight': '825px'}, color="secondary", outline=True)
        ], width=6)
    ])
], fluid=True, style={'marginTop': '20px'})
]       
)
    elif tab == 'tab-2':
        return html.Div([html.H1('Condições meteorológicas em Portugal e a sua influência na produção energética', style={'textAlign': 'left'}),
                html.H4("Condições meteorológicas em Portugal"),
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
                    style={'width': '20%'} 
                ),
            dcc.Graph(id='timeseries_tempo',style={'width': '100%', 'height': '600px'}),
            html.H4("Correlações entre as diferentes produções e fatores meteorológicos"),
            dcc.Graph(id='heatmap', figure=heatmap ,style={'width': '100%', 'height': '900px'},config={'displayModeBar': False,}),
            html.H4("Comparar fator meteorológico com tipo de produção"),
            dbc.Row([
                    # Mês
                    dbc.Col([
                        dcc.Dropdown(
                    id='dropdown-meteoII',
                    #className="mb-4",
                    options=[
                        {'label': 'Luz diária de sol','value':'sunlight'},
                        {'label': 'Temperatura', 'value': 'temperatura'},
                        {'label': 'Velocidade do vento', 'value': 'vento'},
                        {'label': 'Precipitação diária (em mm)', 'value': 'precipitacao'},
                        {'label': 'Nebulosidade', 'value': 'nebulosidade'}
                    ],
                    value='temperatura',
                    clearable=False,
                    style={'width': '90%'} 
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
                    style={'width': '90%'} 
                ),
                    ], width=2),
            ], style={'marginBottom': '0px','marginTop':'20px'}),
            #html.H4("Fator meteorológico vs produção"),
            dcc.Graph(id='meteovsprod',style={'width': '100%', 'height': '600px'}),
            
            
                         
                         
                         
                         
                         
                         
                         ])
    elif tab == 'tab-3':
        return html.Div([
            html.H1('Preço da eletricidade ', style={'textAlign': 'left'}),
            html.H4("Evolução do preço da eletricidade"),
            dcc.Graph(id='grafico-precos', figure=precos),
            html.H4('Comparação de condição meteorológica selecionado e preço da energia'),
            dcc.Dropdown(
                    id='dropdown-meteoIII',
                    #className="mb-4",
                    options=[
                        {'label': 'Luz diária de sol','value':'sunlight'},
                        {'label': 'Temperatura', 'value': 'temperatura'},
                        {'label': 'Velocidade do vento', 'value': 'vento'},
                        {'label': 'Precipitação diária (em mm)', 'value': 'precipitacao'},
                        {'label': 'Nebulosidade', 'value': 'nebulosidade'}
                    ],
                    value='temperatura',
                    clearable=False,
                    style={'width': '20%'} ),
            dcc.Graph(id='meteovspreco'),
            

            

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
    ], width=2), 
    
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
            style={'width': '60%'}
        )
    ], width=2), 
]),
            dcc.Graph(id='calendar'),
        ])





@app.callback(
    Output('grafico_tec', 'figure'),
    Input('dropdown-tecnologia', 'value')
)
def update_circular_graph(tecnologia_escolhida):
    if not tecnologia_escolhida:
        raise dash.exceptions.PreventUpdate
        
    
    fig = create_circular_histogram(tecnologia_escolhida)
    return fig


@app.callback(
    Output('spiral', 'figure'),
    Input('dropdown-tecnologiaII', 'value')
)
def update_circular_graph(tecnologia_escolhida):
    if not tecnologia_escolhida:
        raise dash.exceptions.PreventUpdate
        
    
    fig = create_spiral_histogram(tecnologia_escolhida)

    return fig
@app.callback(
    Output('aviso-falta', 'children'),
    [Input('dropdown-mes', 'value'),
     Input('dropdown-intervalo', 'value'),
     Input('radio-ano', 'value')]
)
def print_aviso(mes_escolhido, _,ano_escolhido):
    
    if int(mes_escolhido) == 10 and int(ano_escolhido) == 2025:
        return "AVISO: Este mês tem valores em falta. Particular atenção para o dia 14 que tem mais de 95% em falta! "
   

@app.callback(
    Output('area_graph', 'figure'),
    [Input('dropdown-mes', 'value'),
     Input('dropdown-intervalo', 'value'),
     Input('radio-ano', 'value')]
)
def update_circular_graph(mes_escolhido, intervalo_escolhido,ano_escolhido):
   
    if not mes_escolhido or not ano_escolhido:
        raise dash.exceptions.PreventUpdate

    
    mes = int(mes_escolhido)
    ano = int(ano_escolhido)


    fig = area_month_year_interval(mes,ano,intervalo_escolhido)
    
    return fig

@app.callback(
    Output('timeseries_tempo', 'figure'),
    Input('dropdown-meteoI', 'value')
)
def update_weather_timeseries(tecnologia_escolhida):
    if not tecnologia_escolhida:
        raise dash.exceptions.PreventUpdate
        
   
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
        
   
    fig = create_meteovsprod(meteo_escolhido,tecnologia_escolhida)
    return fig

@app.callback(
    Output('titulo-evolucao', 'children'),
    Input('dropdown-tecnologia', 'value')
)
def update_title(tecnologia_selecionada):
   
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

    fig = create_cluster_calendar_visualization(cluster,ano)
    return fig

@app.callback(
    Output('meteovspreco', 'figure'),
    Input('dropdown-meteoIII', 'value')
)
def update_meteovspreco(meteo_escolhido):
    if not meteo_escolhido:
        raise dash.exceptions.PreventUpdate
        
    fig = create_preco_meteocond_streamgraph(meteo_escolhido)
    return fig


if __name__ == '__main__':
    app.run(debug=True)

    
    