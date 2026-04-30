from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import dash_vega_components as dvc
from pag1_streamgraph import altair_areaIII

app = Dash(__name__)


fig = altair_areaIII()
app.layout = html.Div([
    dcc.Graph(figure=fig)
])

"""SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "16rem",
    "padding": "2rem 1rem",
    "background-color": "#f8f9fa",
}

# 2. Estilo do Conteúdo Principal
CONTENT_STYLE = {
    "margin-left": "18rem",        , style=CONTENT_STYLE
    "margin-right": "2rem",
    "padding": "2rem 1rem",      , style=SIDEBAR_STYLE
}"""

# 3. Layout Principal (Menu + Content)
app.layout = html.Div([
    dcc.Location(id="url"), # Monitoriza o URL da página
    
    # Barra Lateral
    html.Div([
        html.H2("Energia", className="display-4"),
        html.Hr(),
        html.P("Menu de Navegação"),
        dcc.Link("Sazonalidade", href="/sazonalidade", style={"display": "block", "margin-bottom": "10px"}),
        dcc.Link("Condições meteorológicas", href="/meteo", style={"display": "block", "margin-bottom": "10px"}),
        dcc.Link("Preço", href="/preco", style={"display": "block"}),
    ]),

    # Área onde o conteúdo vai ser injetado
    html.Div(id="page-content")
])

# 4. Callback para mudar de página
@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname):
    if pathname == "/sazonalidade" or pathname == "/":
        # Aqui chamas a tua função do gráfico
        # fig = create_circular_histogram('Eólica (kWh)')
        return html.Div([
            html.H1("Produção Eólica"),
            dcc.Graph(id="graph-eolica") # fig=fig
        ])
    
    elif pathname == "/meteo":
        return html.Div([
            html.H1("Condições Meteorológicas"),
            html.P("Espaço para o gráfico de condições meteorológicas...")
        ])
    
    elif pathname == "/preco":
        return html.Div([
            html.H1("Preço"),
            html.P("Espaço para o gráfico de preço...")
        ])
    
    # Se a página não for encontrada
    return html.Div([
        html.H1("404: Not found"),
        html.P(f"O caminho {pathname} não foi reconhecido.")
    ])







app.run(debug=True, use_reloader=False)