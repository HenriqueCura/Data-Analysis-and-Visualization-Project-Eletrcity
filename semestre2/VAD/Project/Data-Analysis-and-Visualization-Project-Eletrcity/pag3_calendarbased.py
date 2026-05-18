import unicodedata
from pathlib import Path

import plotly.io as pio
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


# Abre os graficos no browser quando este ficheiro e executado diretamente.
pio.renderers.default = "browser"

# Constroi caminhos absolutos para os CSV, independentemente da pasta de execucao.
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

# Carrega producao horaria e dados diarios com preco/meteorologia.
df_hora = pd.read_csv(DATA_DIR / "dados_hora.csv")
df_diarios = pd.read_csv(DATA_DIR / "dados_diarios.csv")


def _normalize(text):
    # Remove acentos e uniformiza texto para comparar nomes de colunas e parametros.
    normalized = unicodedata.normalize("NFKD", str(text))
    return "".join(
        char for char in normalized
        if not unicodedata.combining(char)
    ).casefold()


def _find_column(df, *tokens):
    # Procura uma coluna que contenha todos os tokens indicados.
    normalized_tokens = [_normalize(token) for token in tokens]

    for col in df.columns:
        normalized_col = _normalize(col)

        if all(token in normalized_col for token in normalized_tokens):
            return col

    raise ValueError(f"Nenhuma coluna encontrada para: {tokens}")


# Descobre nomes de colunas importantes mesmo que o CSV varie ligeiramente.
date_col = "Data" if "Data" in df_hora.columns else _find_column(df_hora, "data")
datetime_col = "Data/Hora" if "Data/Hora" in df_hora.columns else _find_column(df_hora, "data", "hora")

# Identifica as principais tecnologias renovaveis usadas no perfil horario.
eolica_col = _find_column(df_hora, "eolica")
hidrica_col = _find_column(df_hora, "hidrica")
fotovoltaica_col = _find_column(df_hora, "fotovoltaica")

# Escolhe a coluna de preco medio diario, aceitando nomes em portugues ou ingles.
if "avg_price_eur_mwh" in df_diarios.columns:
    price_col = "avg_price_eur_mwh"
else:
    try:
        price_col = _find_column(df_diarios, "preco")
    except ValueError:
        price_col = _find_column(df_diarios, "price")


# Normaliza as colunas temporais para datetime.
df_hora["Data"] = pd.to_datetime(df_hora[date_col])
df_hora["Data/Hora"] = pd.to_datetime(
    df_hora[datetime_col],
    utc=True,
    errors="coerce"
)

df_diarios["Data"] = pd.to_datetime(df_diarios["Data"])

# Cria a hora decimal para desenhar o perfil medio ao longo do dia.
df_hora["Hora_decimal"] = (
    df_hora["Data/Hora"].dt.hour
    + df_hora["Data/Hora"].dt.minute / 60
)


# Mapa entre nomes apresentados e colunas reais de producao.
PRODUCTION_COLS = {
    "Eólica": eolica_col,
    "Hídrica": hidrica_col,
    "Fotovoltaica": fotovoltaica_col
}


# Textos usados para apresentar os tres grupos de preco.
PRICE_CLUSTER_LABELS = {
    "baixos": "Preços mais baixos",
    "medios": "Preços médios",
    "altos": "Preços mais altos"
}


# Cores dos grupos de preco no calendario.
PRICE_CLUSTER_COLORS = {
    "baixos": "#0F6B8A",
    "medios": "#C77D00",
    "altos": "#8E1B1B"
}


# Cores das linhas de producao no perfil horario.
LINE_COLORS = {
    "Eólica": "#139A8F",
    "Hídrica": "#2F80ED",
    "Fotovoltaica": "#D9A400"
}


# Nomes dos meses usados nas anotacoes do calendario.
MONTH_NAMES = [
    "janeiro", "fevereiro", "março", "abril",
    "maio", "junho", "julho", "agosto",
    "setembro", "outubro", "novembro", "dezembro"
]


# Abreviaturas dos dias da semana mostradas em cada bloco mensal.
WEEKDAYS = ["seg", "ter", "qua", "qui", "sex", "sáb", "dom"]


def _standardize(values):
    # Padroniza os valores para media 0 e desvio padrao 1 antes do k-means.
    means = values.mean(axis=0)
    stds = values.std(axis=0)
    stds[stds == 0] = 1

    return (values - means) / stds


def _kmeans(values, k=3, random_state=42, max_iter=100):
    # Implementacao simples de k-means para separar os dias em 3 grupos de preco.
    if len(values) < k:
        raise ValueError("Não há dados suficientes para criar 3 grupos de preços.")

    # Escolhe centroides iniciais de forma reprodutivel.
    rng = np.random.default_rng(random_state)
    centroids = values[rng.choice(len(values), size=k, replace=False)]

    for _ in range(max_iter):
        # Calcula a distancia de cada dia a cada centroide.
        distances = np.linalg.norm(
            values[:, None, :] - centroids[None, :, :],
            axis=2
        )

        # Atribui cada dia ao cluster mais proximo.
        labels = distances.argmin(axis=1)
        new_centroids = centroids.copy()

        # Recalcula os centroides como a media dos pontos de cada cluster.
        for cluster in range(k):
            cluster_values = values[labels == cluster]

            if len(cluster_values) > 0:
                new_centroids[cluster] = cluster_values.mean(axis=0)

        if np.allclose(centroids, new_centroids):
            break

        centroids = new_centroids

    return labels


def _choose_default_year(daily_df):
    # Escolhe por defeito o ano completo mais recente; se nao houver, usa o ultimo ano disponivel.
    years_by_months = daily_df.assign(
        Ano=daily_df["Data"].dt.year,
        Mes=daily_df["Data"].dt.month
    ).groupby("Ano")["Mes"].nunique()

    complete_years = years_by_months[years_by_months == 12].index

    if len(complete_years) > 0:
        return int(complete_years.max())

    return int(daily_df["Data"].dt.year.max())


def _prepare_price_cluster_data():
    # Soma a producao horaria por dia para cada tecnologia renovavel.
    daily_prod = (
        df_hora
        .groupby("Data", as_index=False)[list(PRODUCTION_COLS.values())]
        .sum()
        .sort_values("Data")
    )

    # Seleciona apenas data e preco medio diario.
    daily_prices = df_diarios[["Data", price_col]].copy()

    # Junta producao diaria com preco diario.
    daily_data = pd.merge(
        daily_prod,
        daily_prices,
        on="Data",
        how="inner"
    )

    daily_data = daily_data.dropna(subset=[price_col]).copy()

    # Usa apenas o preco como variavel de entrada para o clustering.
    price_values = daily_data[[price_col]].to_numpy(dtype=float)

    # Cria 3 clusters: dias de preco baixo, medio e alto.
    labels = _kmeans(
        _standardize(price_values),
        k=3
    )

    daily_data["Price_cluster_raw"] = labels

    # Ordena clusters pela media de preco para dar nomes interpretaveis.
    cluster_order = (
        daily_data
        .groupby("Price_cluster_raw")[price_col]
        .mean()
        .sort_values()
        .index
        .tolist()
    )

    cluster_map = {
        cluster_order[0]: "baixos",
        cluster_order[1]: "medios",
        cluster_order[2]: "altos"
    }

    # Adiciona ao dataframe o nome tecnico e o label legivel de cada cluster.
    daily_data["Price_cluster"] = daily_data["Price_cluster_raw"].map(cluster_map)

    daily_data["Price_cluster_label"] = daily_data["Price_cluster"].map(
        PRICE_CLUSTER_LABELS
    )

    return daily_data


def _calendar_positions(calendar_df):
    # Calcula a posicao de cada dia num calendario anual organizado em 4 linhas x 3 meses.
    first_days = pd.to_datetime({
        "year": calendar_df["Data"].dt.year,
        "month": calendar_df["Data"].dt.month,
        "day": 1
    })

    month_col = (calendar_df["Data"].dt.month - 1) % 3
    month_row = (calendar_df["Data"].dt.month - 1) // 3

    week_of_month = (
        calendar_df["Data"].dt.day
        + first_days.dt.weekday
        - 1
    ) // 7

    month_width = 7.8
    month_height = 8.6
    left_pad = 1.15

    calendar_df = calendar_df.copy()

    # Coordenada horizontal: mes dentro da linha + semana dentro do mes.
    calendar_df["calendar_x"] = (
        left_pad
        + month_col * month_width
        + week_of_month
    )

    # Coordenada vertical: linha do mes + dia da semana.
    calendar_df["calendar_y"] = (
        -month_row * month_height
        - calendar_df["Data"].dt.weekday
    )

    calendar_df["day_label"] = calendar_df["Data"].dt.day.astype(str)

    return calendar_df, month_width, month_height, left_pad


def _add_calendar_annotations(fig, month_width, month_height, left_pad):
    # Escreve os nomes dos meses no calendario.
    for month_idx, month_name in enumerate(MONTH_NAMES, start=1):
        col = (month_idx - 1) % 3
        row = (month_idx - 1) // 3

        fig.add_annotation(
            x=left_pad + col * month_width + 2.45,
            y=-row * month_height + 1.45,
            text=month_name,
            showarrow=False,
            font=dict(size=13, color="#111111"),
            xref="x",
            yref="y",
            xanchor="center"
        )

    # Escreve os dias da semana no lado esquerdo de cada linha de meses.
    for row in range(4):
        for weekday_idx, weekday in enumerate(WEEKDAYS):
            fig.add_annotation(
                x=left_pad - 0.95,
                y=-row * month_height - weekday_idx,
                text=weekday,
                showarrow=False,
                font=dict(size=11, color="#111111"),
                xref="x",
                yref="y",
                xanchor="right"
            )


def _normalize_price_cluster(price_cluster):
    # Permite receber o cluster como numero ou texto e converte para a chave interna.
    if isinstance(price_cluster, int):
        cluster_map = {
            1: "baixos",
            2: "medios",
            3: "altos"
        }

        if price_cluster not in cluster_map:
            raise ValueError(
                "O cluster deve ser 1, 2, 3, 'baixos', 'medios' ou 'altos'."
            )

        return cluster_map[price_cluster]

    price_cluster = _normalize(price_cluster)

    # Aceita varias formas de escrever a mesma escolha.
    aliases = {
        "baixo": "baixos",
        "baixos": "baixos",
        "precos baixos": "baixos",
        "precos mais baixos": "baixos",

        "medio": "medios",
        "medios": "medios",
        "precos medios": "medios",
        "precos mais medios": "medios",

        "alto": "altos",
        "altos": "altos",
        "precos altos": "altos",
        "precos mais altos": "altos"
    }

    if price_cluster not in aliases:
        raise ValueError(
            "O cluster deve ser 'baixos', 'medios' ou 'altos'."
        )

    return aliases[price_cluster]


def create_cluster_calendar_visualization(price_cluster="baixos", year=None):
    # Normaliza a escolha do utilizador para "baixos", "medios" ou "altos".
    price_cluster = _normalize_price_cluster(price_cluster)

    # Prepara os dados diarios ja classificados por grupos de preco.
    daily_data = _prepare_price_cluster_data()

    # Se o utilizador nao indicar ano, escolhe automaticamente um ano adequado.
    if year is None:
        year = _choose_default_year(daily_data)

    # Filtra o calendario para o ano pretendido.
    calendar_df = daily_data[daily_data["Data"].dt.year == year].copy()

    if calendar_df.empty:
        raise ValueError(f"Não existem dados para o ano {year}.")

    calendar_df, month_width, month_height, left_pad = _calendar_positions(calendar_df)

    # Texto da data usado nos tooltips.
    calendar_df["Data_hover"] = calendar_df["Data"].dt.strftime("%Y-%m-%d")

    # Seleciona apenas os dias pertencentes ao grupo de preco escolhido.
    selected_days = calendar_df[
        calendar_df["Price_cluster"] == price_cluster
    ].copy()

    selected_days["Data_hover"] = selected_days["Data"].dt.strftime("%Y-%m-%d")

    if selected_days.empty:
        raise ValueError(
            f"Não existem dias com {PRICE_CLUSTER_LABELS[price_cluster].lower()} no ano {year}."
        )

    selected_label = PRICE_CLUSTER_LABELS[price_cluster]

    # Cria uma figura com dois paineis: calendario e perfil horario medio.
    fig = make_subplots(
        rows=1,
        cols=2,
        column_widths=[0.48, 0.52],
        horizontal_spacing=0.08,
        subplot_titles=(
            f"Calendário - {selected_label} - {year}",
            f"Perfil médio diário de produção - {selected_label}"
        )
    )

    # Desenha todos os dias do ano como fundo neutro do calendario.
    fig.add_trace(go.Scatter(
        x=calendar_df["calendar_x"],
        y=calendar_df["calendar_y"],
        mode="markers+text",
        text=calendar_df["day_label"],
        textfont=dict(color="#9BA4AA", size=9),
        marker=dict(
            symbol="square",
            size=20,
            color="#EFF3F5",
            line=dict(color="#D7DEE2", width=0.7)
        ),
        hovertemplate=(
            "%{customdata[0]}"
            "<br>Preço: %{customdata[1]:.2f} €/MWh"
            "<extra></extra>"
        ),
        customdata=calendar_df[["Data_hover", price_col]].to_numpy(dtype=object),
        showlegend=False
    ), row=1, col=1)

    # Sobrepoe os dias que pertencem ao cluster escolhido, com cor forte.
    fig.add_trace(go.Scatter(
        x=selected_days["calendar_x"],
        y=selected_days["calendar_y"],
        mode="markers+text",
        text=selected_days["day_label"],
        textfont=dict(color="white", size=9),
        marker=dict(
            symbol="square",
            size=20,
            color=PRICE_CLUSTER_COLORS[price_cluster],
            line=dict(color="white", width=0.7)
        ),
        name=selected_label,
        hovertemplate=(
            selected_label
            + "<br>%{customdata[0]}"
            + "<br>Preço: %{customdata[1]:.2f} €/MWh"
            + "<extra></extra>"
        ),
        customdata=selected_days[["Data_hover", price_col]].to_numpy(dtype=object),
        showlegend=False
    ), row=1, col=1)

    # Usa as datas selecionadas para calcular o perfil horario medio de producao.
    selected_dates = set(selected_days["Data"])

    selected_hourly = df_hora[
        df_hora["Data"].isin(selected_dates)
    ].copy()

    # Media da producao por hora decimal nos dias selecionados.
    profile = (
        selected_hourly
        .groupby("Hora_decimal", as_index=False)[list(PRODUCTION_COLS.values())]
        .mean()
        .sort_values("Hora_decimal")
    )

    # Adiciona uma linha para cada tecnologia renovavel no painel direito.
    for label, col in PRODUCTION_COLS.items():
        fig.add_trace(go.Scatter(
            x=profile["Hora_decimal"],
            y=profile[col],
            mode="lines",
            name=label,
            line=dict(
                color=LINE_COLORS[label],
                width=2.2
            ),
            opacity=0.82,
            hovertemplate=(
                f"{label}<br>Hora: %{{x:.2f}}"
                "<br>Produção média: %{y:,.0f} kWh"
                "<extra></extra>"
            )
        ), row=1, col=2)

    # Adiciona nomes dos meses e dias da semana ao calendario.
    _add_calendar_annotations(fig, month_width, month_height, left_pad)

    # Configura layout geral da figura.
    fig.update_layout(
        template="plotly_white",
        width=1450,
        height=760,
        font=dict(size=13),
        margin=dict(l=50, r=60, t=70, b=60)
    )

    # Remove grelhas e labels do painel do calendario, porque as anotacoes ja identificam tudo.
    fig.update_xaxes(
        showgrid=False,
        showticklabels=False,
        zeroline=False,
        range=[-0.8, left_pad + month_width * 3 - 1.2],
        row=1,
        col=1
    )

    fig.update_yaxes(
        showgrid=False,
        showticklabels=False,
        zeroline=False,
        range=[-month_height * 4 + 1.2, 2.8],
        row=1,
        col=1
    )

    # Configura o eixo X do perfil horario.
    fig.update_xaxes(
        title="Hora do dia",
        tickmode="array",
        tickvals=[0, 3, 6, 9, 12, 15, 18, 21, 24],
        ticktext=[
            "00:00", "03:00", "06:00", "09:00",
            "12:00", "15:00", "18:00", "21:00", "24:00"
        ],
        range=[0, 24],
        showgrid=True,
        gridcolor="#E4ECEF",
        row=1,
        col=2
    )

    # Configura o eixo Y do perfil horario.
    fig.update_yaxes(
        title="Produção média (kWh)",
        showgrid=True,
        gridcolor="#E4ECEF",
        row=1,
        col=2
    )

    return fig


if __name__ == "__main__":
    # Teste local: mostra o calendario dos dias com precos altos em 2024.
    fig = create_cluster_calendar_visualization(price_cluster="altos", year=2024)
    fig.show()
