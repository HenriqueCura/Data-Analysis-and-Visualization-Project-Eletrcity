import unicodedata

import numpy as np
import pandas as pd
import plotly.graph_objects as go


# Carrega os datasets usados para cruzar meteorologia diaria com producao horaria.
df_diarios = pd.read_csv("data/dados_diarios.csv")
df_hora = pd.read_csv("data/dados_hora.csv")

# Garante que a coluna Data fica em formato datetime para merges e agrupamentos.
df_diarios["Data"] = pd.to_datetime(df_diarios["Data"])
df_hora["Data"] = pd.to_datetime(df_hora["Data"])


def _normalize(text):
    # Remove acentos e coloca o texto em minusculas para comparar nomes de colunas.
    normalized = unicodedata.normalize("NFKD", str(text))
    return "".join(
        char for char in normalized
        if not unicodedata.combining(char)
    ).casefold()


def _find_column(df, *tokens):
    # Procura a primeira coluna cujo nome contenha todos os tokens pedidos.
    normalized_tokens = [_normalize(token) for token in tokens]

    for col in df.columns:
        normalized_col = _normalize(col)

        if all(token in normalized_col for token in normalized_tokens):
            return col

    return None


def create_correlation_heatmap():
    # Soma os dados horarios por dia para ficar na mesma granularidade dos dados diarios.
    prod_diaria = (
        df_hora
        .groupby("Data", as_index=False)
        .sum(numeric_only=True)
    )

    # Junta meteorologia e producao diaria usando a data como chave comum.
    df_corr = pd.merge(
        df_diarios,
        prod_diaria,
        on="Data",
        how="inner"
    )

    # Identifica a coluna de producao total mesmo que tenha acentos ou pequenas variacoes.
    rede_distribuicao_col = _find_column(df_corr, "rede", "distribuicao")

    # Lista as variaveis que queremos comparar na matriz de correlacao.
    cols_corr = [
        "temp_C_mean",
        "wind_speed_mean",
        "precip_mm_sum",
        "mean_cloud",
        "Sunlight (em minutos)",
        rede_distribuicao_col,
        _find_column(df_corr, "solar"),
        _find_column(df_corr, "fotovoltaica"),
        _find_column(df_corr, "eolica"),
        _find_column(df_corr, "hidrica"),
        _find_column(df_corr, "biomassa")
    ]

    # Remove entradas vazias ou colunas que nao existam no ficheiro de dados.
    cols_corr = [
        col for col in cols_corr
        if col and col in df_corr.columns
    ]

    df_corr = df_corr[cols_corr].copy()

    # Troca nomes tecnicos por nomes mais faceis de ler no grafico.
    rename_columns = {
        "temp_C_mean": "Temperatura média",
        "wind_speed_mean": "Velocidade do vento",
        "precip_mm_sum": "Precipitação",
        "mean_cloud": "Nebulosidade média",
        "Sunlight (em minutos)": "Luz solar"
    }

    if rede_distribuicao_col in df_corr.columns:
        rename_columns[rede_distribuicao_col] = "Total Produção"

    df_corr = df_corr.rename(columns=rename_columns)

    # Calcula o coeficiente de correlacao entre todas as variaveis numericas.
    corr_matrix = df_corr.corr(numeric_only=True)

    # Mostra valores apenas na metade inferior para nao repetir a mesma correlacao duas vezes.
    mask_lower = np.tril(np.ones(corr_matrix.shape, dtype=bool))
    heatmap_values = corr_matrix.where(mask_lower)

    # Prepara os numeros que aparecem escritos dentro das celulas do heatmap.
    heatmap_text = [
        [
            f"{corr_matrix.iloc[i, j]:.2f}" if mask_lower[i, j] else ""
            for j in range(corr_matrix.shape[1])
        ]
        for i in range(corr_matrix.shape[0])
    ]

    corr_colorscale = [
        [0.0, "#245C8A"],
        [0.25, "#8CC6D7"],
        [0.5, "#F7F3EA"],
        [0.75, "#F2A65A"],
        [1.0, "#B94E48"]
    ]

    n_vars = len(corr_matrix.columns)
    cell_padding = 0.38

    def scale_to_cell(values, center, value_min, value_max, invert=False):
        # Converte valores reais para coordenadas pequenas dentro de uma celula da matriz.
        if value_max == value_min:
            return np.full(len(values), center)

        scaled = center - cell_padding + (
            (values - value_min) / (value_max - value_min)
        ) * (cell_padding * 2)

        if invert:
            scaled = center + cell_padding - (
                (values - value_min) / (value_max - value_min)
            ) * (cell_padding * 2)

        return scaled

    fig = go.Figure()

    # Camada principal: heatmap com os valores de correlacao.
    fig.add_trace(go.Heatmap(
        z=heatmap_values.values,
        x=list(range(n_vars)),
        y=list(range(n_vars)),
        text=heatmap_text,
        texttemplate="%{text}",
        customdata=np.dstack([
            np.tile(corr_matrix.columns, (n_vars, 1)),
            np.tile(
                corr_matrix.index.to_numpy().reshape(-1, 1),
                (1, n_vars)
            )
        ]),
        hovertemplate=(
            "%{customdata[1]} vs %{customdata[0]}"
            "<br>Correlação: %{z:.2f}<extra></extra>"
        ),
        colorscale=corr_colorscale,
        zmin=-1,
        zmax=1,
        xgap=2,
        ygap=2,
        colorbar=dict(title="Correlação"),
        hoverongaps=False
    ))

    # Na metade superior, cada celula recebe um mini scatterplot do par de variaveis.
    for i, row_name in enumerate(corr_matrix.index):
        for j, col_name in enumerate(corr_matrix.columns):
            if i < j:
                pair_data = df_corr[[col_name, row_name]].dropna()

                if pair_data.empty:
                    continue

                x_values = pair_data[col_name].to_numpy()
                y_values = pair_data[row_name].to_numpy()

                x_min, x_max = x_values.min(), x_values.max()
                y_min, y_max = y_values.min(), y_values.max()

                x_cell = scale_to_cell(
                    x_values,
                    j,
                    x_min,
                    x_max
                )

                y_cell = scale_to_cell(
                    y_values,
                    i,
                    y_min,
                    y_max,
                    invert=True
                )

                fig.add_trace(go.Scatter(
                    x=x_cell,
                    y=y_cell,
                    mode="markers",
                    marker=dict(
                        color="#184E77",
                        size=3.6,
                        opacity=0.42
                    ),
                    customdata=np.column_stack([x_values, y_values]),
                    hovertemplate=(
                        f"{col_name}: %{{customdata[0]:.2f}}<br>"
                        f"{row_name}: %{{customdata[1]:.2f}}"
                        "<extra></extra>"
                    ),
                    showlegend=False
                ))

                # Acrescenta uma linha de tendencia para facilitar a leitura da relacao.
                if len(pair_data) > 1 and x_max != x_min and y_max != y_min:
                    slope, intercept = np.polyfit(x_values, y_values, 1)

                    line_x_values = np.array([x_min, x_max])
                    line_y_values = slope * line_x_values + intercept

                    line_x_cell = scale_to_cell(
                        line_x_values,
                        j,
                        x_min,
                        x_max
                    )

                    line_y_cell = scale_to_cell(
                        line_y_values,
                        i,
                        y_min,
                        y_max,
                        invert=True
                    )

                    line_y_cell = np.clip(
                        line_y_cell,
                        i - cell_padding,
                        i + cell_padding
                    )

                    fig.add_trace(go.Scatter(
                        x=line_x_cell,
                        y=line_y_cell,
                        mode="lines",
                        line=dict(
                            color="#9B3039",
                            width=1.4
                        ),
                        hoverinfo="skip",
                        showlegend=False
                    ))

    # Define dimensoes, margens e aspeto visual geral.
    fig.update_layout(
        template="plotly_white",
        width=1100,
        height=900,
        title_x=0.5,
        font=dict(size=13),
        margin=dict(l=80, r=80, t=40, b=120),
        plot_bgcolor="white"
    )

    # Coloca os nomes das variaveis no eixo X.
    fig.update_xaxes(
        tickmode="array",
        tickvals=list(range(n_vars)),
        ticktext=corr_matrix.columns,
        tickangle=45,
        range=[-0.5, n_vars - 0.5],
        showgrid=False,
        zeroline=False
    )

    # Inverte o eixo Y para a matriz ficar alinhada como uma tabela.
    fig.update_yaxes(
        tickmode="array",
        tickvals=list(range(n_vars)),
        ticktext=corr_matrix.index,
        range=[n_vars - 0.5, -0.5],
        showgrid=False,
        zeroline=False
    )

    return fig

if __name__ == "__main__":
    # Teste local: cria e abre o grafico quando o ficheiro e executado diretamente.
    fig = create_correlation_heatmap()
    fig.show()  
