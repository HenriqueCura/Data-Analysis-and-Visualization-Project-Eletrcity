import dash
from dash import dcc, html, Input, Output
import dash_vega_components as dvc
from pag1_streamgraph import altair_areaIII
from pag1_circulargraphI import circular_total
from pag1_circulargraphII import create_circular_histogram
import altair as alt
import dash_bootstrap_components as dbc

alt.data_transformers.enable("vegafusion")

app = dash.Dash(__name__,external_stylesheets=[dbc.themes.BOOTSTRAP])


streamgraph = altair_areaIII()
circularI = circular_total()


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
                dcc.Tab(label='Sazonalidade', value='tab-1', style={'padding': '10px'}, selected_style={'padding': '20px'}),
                dcc.Tab(label='Condições meteorológicas', value='tab-2', style={'padding': '10px'}, selected_style={'padding': '20px'}),
                dcc.Tab(label='Preço', value='tab-3', style={'padding': '10px'}, selected_style={'padding': '20px'}),
            ], style={'height': '70px','width':'600px'}) # Definir altura fixa ajuda a alinhar
        ], style={'display': 'inline-block', 'verticalAlign': 'middle'})
        
    ], style={
        'padding': '10px 20px', 
        'backgroundColor': 'white', 
        'borderBottom': '1px solid #ddd',
        'display': 'flex',        # Usa Flexbox para alinhar tudo na horizontal
        'alignItems': 'center'    # Alinha verticalmente ao centro
    }),

    # --- CONTEÚDO ---
    html.Div(id='tabs-content', style={'padding': '40px'})
])

@app.callback(
    Output('tabs-content', 'children'),
    Input('tabs-menu', 'value')
)
def render_content(tab):
    if tab == 'tab-1':
        return html.Div([#html.H4('Produção energética por tipo em Portugal'),
                         dvc.Vega(id='streamgraph_prods', spec=streamgraph.to_dict(format='vega')),
                         dbc.Container([
    dbc.Row([
        # COLUNA ESQUERDA: O Gráfico Circular (Fixo)
        dbc.Col([
            html.H4("Visão Geral: Ciclo de Produção", style={'textAlign': 'center'}),
            dcc.Graph(
                id='grafico-circular-geral',
                figure=circularI # O gráfico que já tens pronto
            )
        ], width=6), # Ocupa metade do ecrã

        # COLUNA DIREITA: O Detalhe por Tecnologia (Dinâmico)
        dbc.Col([
            html.H4("Detalhe por Tecnologia", style={'textAlign': 'center'}),
            
            # Filtro para o gráfico da direita
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
                    clearable=False
                ),
            ], style={'marginBottom': '20px'}),

            # Espaço para o gráfico detalhado
            dcc.Graph(id='grafico_tec')
        ], width=6)
    ])
], fluid=True)
                         
                         ]
                         
                         
                         )
    elif tab == 'tab-2':
        return html.Div([html.H4("Gráfico Solar aqui")])
    return html.Div([html.H4("Tabela de Dados")])


@app.callback(
    Output('grafico_tec', 'figure'),
    Input('dropdown-tecnologia', 'value')
)
def update_circular_graph(tecnologia_escolhida):
    # Chamamos a função aqui, sempre que o dropdown muda
    fig = create_circular_histogram(tecnologia_escolhida)
    return fig


if __name__ == '__main__':
    app.run(debug=True)

    """return html.Div([#html.H4('Produção energética por tipo em Portugal'),
                         dvc.Vega(id='streamgraph_prods', spec=streamgraph.to_dict(format='vega')),
                         dcc.Graph(id='circular_total',figure = circularI)
                         
                         ]"""