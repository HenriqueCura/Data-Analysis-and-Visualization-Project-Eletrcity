import numpy as np
import plotly.graph_objects as go
import time

def get_new_graph(n):
    if n:
        # Simulate slow-loading component
        time.sleep(2)
    # Generate a random scatter plot
    n = (n + 1) * 10
    return go.Figure(
        data=go.Scatter(
            y=np.random.randn(n) * 100,
            mode="markers",
            marker=dict(
                size=16,
                color=np.random.randn(n) * 100,
                colorscale="blues",
                showscale=True,
            ),
        ),
        layout=go.Layout(title="This graph takes ages to re-load"),
    )