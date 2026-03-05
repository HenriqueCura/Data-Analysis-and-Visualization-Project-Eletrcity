
import cdsapi
from pathlib import Path
import cdsapi
import xarray as xr
import pandas as pd
import numpy as np
from pathlib import Path
from zipfile import ZipFile
import xarray as xr
from tempfile import TemporaryDirectory
import cfgrib


FILE = "TEST_2023_01.grib"

# 1) Abrir instantes (t2m, u10, v10)
ds_inst = xr.open_dataset(
    FILE,
    engine="cfgrib",
    backend_kwargs={
        "filter_by_keys": {
            "stepType": "instant"
        }
    }
)

# 2) Abrir acumulados (tp)
ds_acc = xr.open_dataset(
    FILE,
    engine="cfgrib",
    backend_kwargs={
        "filter_by_keys": {
            "stepType": "accum"
        }
    }
)

# 3) Merge seguro
ds = xr.merge([ds_inst, ds_acc], compat="override", join="outer")

# 4) Identificar dimensão temporal
time_dim = "time"

# 5) Média espacial da caixa
def spatial_mean(da):
    dims = [d for d in da.dims if d != time_dim]
    return da.mean(dim=dims)

t2m = spatial_mean(ds["t2m"]).load()      # Kelvin
u10 = spatial_mean(ds["u10"]).load()
v10 = spatial_mean(ds["v10"]).load()
tp  = spatial_mean(ds["tp"]).load()       # m

times = pd.to_datetime(ds[time_dim].values, utc=True)

# 6) DataFrame horário
dfh = pd.DataFrame(index=times)
dfh["temp_C"] = t2m.values - 273.15
dfh["u10"] = u10.values
dfh["v10"] = v10.values
dfh["wind_speed"] = np.sqrt(dfh["u10"]**2 + dfh["v10"]**2)
dfh["precip_mm"] = tp.values * 1000.0  # m -> mm

dfh.to_csv("weather_hourly.csv")
print("✔ hourly CSV")

# 7) Diário
dfd = dfh.resample("1D").agg({
    "temp_C": ["mean", "min", "max"],
    "wind_speed": ["mean", "min", "max"],
    "precip_mm": ["sum"]
})
dfd.columns = ["_".join(col) for col in dfd.columns]
dfd.to_csv("weather_daily_stats.csv")
print("✔ daily CSV")

# 8) Mensal
dfm = dfh.resample("1MS").agg({
    "temp_C": "mean",
    "wind_speed": "mean",
    "precip_mm": "sum"
})
dfm.to_csv("weather_monthly.csv")
print("✔ monthly CSV")