import unicodedata
import plotly.io as pio
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

pio.renderers.default = "browser"

df_hora = pd.read_csv("data/dados_hora.csv")

def _normalize(text):
    normalized = unicodedata.normalize("NFKD", str(text))
    return "".join(char for char in normalized if not unicodedata.combining(char)).casefold()

def _find_column(df, *tokens):
    normalized_tokens = [_normalize(token) for token in tokens]
    for col in df.columns:
        normalized_col = _normalize(col)
        if all(token in normalized_col for token in normalized_tokens):
            return col
    raise ValueError(f"Nenhuma coluna encontrada para: {tokens}")

date_col = "Data" if "Data" in df_hora.columns else _find_column(df_hora, "data")
datetime_col = "Data/Hora" if "Data/Hora" in df_hora.columns else _find_column(df_hora, "data", "hora")
eolica_col = _find_column(df_hora, "eolica")
hidrica_col = _find_column(df_hora, "hidrica")
fotovoltaica_col = _find_column(df_hora, "fotovoltaica")

df_hora["Data"] = pd.to_datetime(df_hora[date_col])
df_hora["Data/Hora"] = pd.to_datetime(df_hora[datetime_col], utc=True, errors="coerce")
df_hora["Hora_decimal"] = (
    df_hora["Data/Hora"].dt.hour
    + df_hora["Data/Hora"].dt.minute / 60
)

PRODUCTION_COLS = {
    "Eólica": eolica_col,
    "Hídrica": hidrica_col,
    "Fotovoltaica": fotovoltaica_col
}

CLUSTER_COLORS = {
    1: "#0F6B8A",
    2: "#C77D00",
    3: "#8E1B1B"
}

LINE_COLORS = {
    "Eólica": "#139A8F",
    "Hídrica": "#2F80ED",
    "Fotovoltaica": "#D9A400"
}

MONTH_NAMES = [
    "janeiro", "fevereiro", "março", "abril",
    "maio", "junho", "julho", "agosto",
    "setembro", "outubro", "novembro", "dezembro"
]

WEEKDAYS = ["seg", "ter", "qua", "qui", "sex", "sáb", "dom"]

def _standardize(values):
    means = values.mean(axis=0)
    stds = values.std(axis=0)
    stds[stds == 0] = 1
    return (values - means) / stds

def _kmeans(values, k=3, random_state=42, max_iter=100):
    rng = np.random.default_rng(random_state)
    centroids = values[rng.choice(len(values), size=k, replace=False)]

    for _ in range(max_iter):
        distances = np.linalg.norm(values[:, None, :] - centroids[None, :, :], axis=2)
        labels = distances.argmin(axis=1)
        new_centroids = centroids.copy()

        for cluster in range(k):
            cluster_values = values[labels == cluster]
            if len(cluster_values) > 0:
                new_centroids[cluster] = cluster_values.mean(axis=0)

        if np.allclose(centroids, new_centroids):
            break

        centroids = new_centroids

    return labels

def _choose_default_year(daily_df):
    years_by_months = daily_df.assign(
        Ano=daily_df["Data"].dt.year,
        Mes=daily_df["Data"].dt.month
    ).groupby("Ano")["Mes"].nunique()

    complete_years = years_by_months[years_by_months == 12].index
    if len(complete_years) > 0:
        return int(complete_years.max())

    return int(daily_df["Data"].dt.year.max())

def _prepare_cluster_data():
    daily_prod = (
        df_hora
        .groupby("Data", as_index=False)[list(PRODUCTION_COLS.values())]
        .sum()
        .sort_values("Data")
    )

    features = daily_prod[list(PRODUCTION_COLS.values())].to_numpy(dtype=float)
    labels = _kmeans(_standardize(features), k=3)
    daily_prod["Cluster_raw"] = labels

    cluster_order = (
        daily_prod
        .groupby("Cluster_raw")[list(PRODUCTION_COLS.values())]
        .mean()
        .sum(axis=1)
        .sort_values()
        .index
        .tolist()
    )
    cluster_map = {cluster: idx + 1 for idx, cluster in enumerate(cluster_order)}
    daily_prod["Cluster"] = daily_prod["Cluster_raw"].map(cluster_map)

    return daily_prod

def _calendar_positions(calendar_df):
    first_days = pd.to_datetime({
        "year": calendar_df["Data"].dt.year,
        "month": calendar_df["Data"].dt.month,
        "day": 1
    })

    month_col = (calendar_df["Data"].dt.month - 1) % 3
    month_row = (calendar_df["Data"].dt.month - 1) // 3
    week_of_month = (
        calendar_df["Data"].dt.day + first_days.dt.weekday - 1
    ) // 7

    month_width = 7.6
    month_height = 8.2

    calendar_df = calendar_df.copy()
    calendar_df["calendar_x"] = month_col * month_width + week_of_month
    calendar_df["calendar_y"] = -month_row * month_height - calendar_df["Data"].dt.weekday
    calendar_df["day_label"] = calendar_df["Data"].dt.day.astype(str)

    return calendar_df, month_width, month_height

def _add_calendar_annotations(fig, year, month_width, month_height):
    for month_idx, month_name in enumerate(MONTH_NAMES, start=1):
        col = (month_idx - 1) % 3
        row = (month_idx - 1) // 3
        fig.add_annotation(
            x=col * month_width + 2.45,
            y=-row * month_height + 0.9,
            text=month_name,
            showarrow=False,
            font=dict(size=13, color="#111111"),
            xref="x",
            yref="y"
        )

    for row in range(4):
        for weekday_idx, weekday in enumerate(WEEKDAYS):
            fig.add_annotation(
                x=-0.75,
                y=-row * month_height - weekday_idx,
                text=weekday,
                showarrow=False,
                font=dict(size=11, color="#111111"),
                xref="x",
                yref="y"
            )

    fig.add_annotation(
        x=month_width + 2.4,
        y=1.85,
        text=str(year),
        showarrow=False,
        font=dict(size=16, color="#111111"),
        xref="x",
        yref="y"
    )

def create_cluster_calendar_visualization(year=None):
    daily_prod = _prepare_cluster_data()
    if year is None:
        year = _choose_default_year(daily_prod)

    calendar_df = daily_prod[daily_prod["Data"].dt.year == year].copy()
    if calendar_df.empty:
        raise ValueError(f"Não existem dados para o ano {year}.")

    calendar_df, month_width, month_height = _calendar_positions(calendar_df)

    fig = make_subplots(
        rows=1,
        cols=2,
        column_widths=[0.48, 0.52],
        horizontal_spacing=0.08,
        subplot_titles=(
            f"Calendário de clusters - {year}",
            "Perfil médio diário de produção"
        )
    )

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
        hovertemplate="%{customdata|%Y-%m-%d}<extra></extra>",
        customdata=calendar_df["Data"],
        showlegend=False
    ), row=1, col=1)

    visible_by_cluster = {}

    for cluster in [1, 2, 3]:
        cluster_days = calendar_df[calendar_df["Cluster"] == cluster]
        visible = cluster == 1
        visible_by_cluster[cluster] = []

        fig.add_trace(go.Scatter(
            x=cluster_days["calendar_x"],
            y=cluster_days["calendar_y"],
            mode="markers+text",
            text=cluster_days["day_label"],
            textfont=dict(color="white", size=9),
            marker=dict(
                symbol="square",
                size=20,
                color=CLUSTER_COLORS[cluster],
                line=dict(color="white", width=0.7)
            ),
            name=f"Cluster {cluster}",
            hovertemplate=(
                "Cluster " + str(cluster) + "<br>"
                "%{customdata|%Y-%m-%d}<extra></extra>"
            ),
            customdata=cluster_days["Data"],
            showlegend=False,
            visible=visible
        ), row=1, col=1)
        visible_by_cluster[cluster].append(len(fig.data) - 1)

    for cluster in [1, 2, 3]:
        cluster_dates = set(calendar_df[calendar_df["Cluster"] == cluster]["Data"])
        cluster_hourly = df_hora[df_hora["Data"].isin(cluster_dates)].copy()
        profile = (
            cluster_hourly
            .groupby("Hora_decimal", as_index=False)[list(PRODUCTION_COLS.values())]
            .mean()
            .sort_values("Hora_decimal")
        )

        for label, col in PRODUCTION_COLS.items():
            fig.add_trace(go.Scatter(
                x=profile["Hora_decimal"],
                y=profile[col],
                mode="lines",
                name=label,
                line=dict(color=LINE_COLORS[label], width=2.2),
                opacity=0.82,
                hovertemplate=(
                    f"{label}<br>Hora: %{{x:.2f}}"
                    "<br>Produção média: %{y:,.0f} kWh<extra></extra>"
                ),
                visible=cluster == 1,
                showlegend=True
            ), row=1, col=2)
            visible_by_cluster[cluster].append(len(fig.data) - 1)

    buttons = []
    for cluster in [1, 2, 3]:
        visible = [False] * len(fig.data)
        visible[0] = True
        for trace_idx in visible_by_cluster[cluster]:
            visible[trace_idx] = True

        buttons.append(dict(
            label=f"Cluster {cluster}",
            method="update",
            args=[
                {"visible": visible},
                {
                    "title": f"Cluster {cluster}: calendário e perfil médio de produção"
                }
            ]
        ))

    _add_calendar_annotations(fig, year, month_width, month_height)

    fig.update_layout(
        title="Cluster calendar based visualization",
        template="plotly_white",
        width=1450,
        height=760,
        title_x=0.5,
        font=dict(size=13),
        margin=dict(l=50, r=60, t=100, b=60),
        updatemenus=[
            dict(
                buttons=buttons,
                direction="down",
                x=1.0,
                y=1.12,
                xanchor="right",
                yanchor="top",
                showactive=True
            )
        ],
        annotations=fig.layout.annotations + (
            dict(
                text="Selecionar cluster:",
                x=0.82,
                y=1.105,
                xref="paper",
                yref="paper",
                showarrow=False,
                align="left"
            ),
        )
    )

    fig.update_xaxes(
        showgrid=False,
        showticklabels=False,
        zeroline=False,
        range=[-1.2, month_width * 3 - 1.4],
        row=1,
        col=1
    )
    fig.update_yaxes(
        showgrid=False,
        showticklabels=False,
        zeroline=False,
        range=[-month_height * 4 + 1.4, 2.4],
        row=1,
        col=1
    )

    fig.update_xaxes(
        title="Hora do dia",
        tickmode="array",
        tickvals=[0, 3, 6, 9, 12, 15, 18, 21, 24],
        ticktext=["00:00", "03:00", "06:00", "09:00", "12:00", "15:00", "18:00", "21:00", "24:00"],
        range=[0, 24],
        showgrid=True,
        gridcolor="#E4ECEF",
        row=1,
        col=2
    )
    fig.update_yaxes(
        title="Produção média (kWh)",
        showgrid=True,
        gridcolor="#E4ECEF",
        row=1,
        col=2
    )

    return fig

if __name__ == "__main__":
    fig = create_cluster_calendar_visualization()
    fig.show()