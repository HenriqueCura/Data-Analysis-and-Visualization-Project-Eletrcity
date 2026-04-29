import dash
from dash import dcc, html, Input, Output

app = dash.Dash(__name__)

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
        return html.Div([html.H4("Gráfico Eólico aqui")])
    elif tab == 'tab-2':
        return html.Div([html.H4("Gráfico Solar aqui")])
    return html.Div([html.H4("Tabela de Dados")])

if __name__ == '__main__':
    app.run(debug=True)