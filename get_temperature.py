from datetime import date
import pandas as pd
import meteostat as ms



vilaverde_lat =  39.41
vilaverde_lon = -8.07

POINT = ms.Point(vilaverde_lat,vilaverde_lon)
START = date(2020, 1, 1)
END = date(2026, 2, 18)


stations = ms.stations.nearby(POINT, limit=10)

ts = ms.daily(stations, START, END)
df = ms.interpolate(ts, POINT).fetch()

print(df)