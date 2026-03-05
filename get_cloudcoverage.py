from pathlib import Path
import cdsapi
import xarray as xr
import pandas as pd
import numpy as np



latitude =  39.41
longitude = -8.07
c = cdsapi.Client()




years  = ['2023', '2024', '2025', '2026']
months = [f"{m:02d}" for m in range(1, 12+1)]
days   = [f"{d:02d}" for d in range(1, 32)]
hours  = [f"{h:02d}:00" for h in range(24)]

# Pequena “caixa” à volta do ponto (N, W, S, E)
area = [41.25, -8.5, 37.25, -7.5]
areaII = [39.50, -8.25, 39.25, -8]

outdir = Path("era5_cloud_parts")
outdir.mkdir(parents=True, exist_ok=True)

c = cdsapi.Client()

for y in years:
    for m in months:
        target = outdir / f"era5_tcc_{y}_{m}.nc"
        if target.exists():
            print(f"Já existe: {target}")
            continue
        print(f"Pedido: {y}-{m}")
        c.retrieve(
            'reanalysis-era5-single-levels',
            {
                'product_type': 'reanalysis',
                'variable': 'total_cloud_cover',
                'year': y,
                'month': m,
                'day': days,       # tudo; o servidor ignora dias que não existem
                'time': hours,
                'area': area,      # [N, W, S, E]
                'data_format': 'netcdf'
            },
            str(target)
        )
        print(f"✔ Guardado: {target}")




parts = sorted(Path("era5_cloud_parts").glob("*.nc"))
print(f"Ficheiros encontrados: {len(parts)}")

if len(parts) == 0:
    raise SystemExit("Nenhum ficheiro .nc encontrado!")

# Abrir ficheiros usando o engine mais estável no Windows
ds = xr.open_mfdataset(
    [str(p) for p in parts],
    combine="by_coords",
    engine="h5netcdf",   # <- IMPORTANTE NO WINDOWS
    parallel=False
)

print("Dataset carregado com sucesso!")
print(ds)

# A variável costuma chamar-se 'tcc'
varname = "tcc"
if varname not in ds.variables:
    print("Variáveis disponíveis:", list(ds.variables))
    raise SystemExit("A variável 'tcc' não existe no dataset ERA5!")

tcc = ds[varname]     # dims: time, latitude, longitude

# Média espacial da área (se houver mais do que 1 gridpoint)
dims_espaciais = [d for d in tcc.dims if d != "valid_time"]
if dims_espaciais:
    tcc_point = tcc.mean(dim=dims_espaciais)
else:
    tcc_point = tcc

# Converter para DataFrame
df_hourly = tcc_point.to_dataframe(name="tcc_frac")  # 0..1
df_hourly.index = pd.to_datetime(df_hourly.index, utc=True)

# Passar para percentagem
df_hourly["cloudcover_pct"] = df_hourly["tcc_frac"] * 100

# ------------------ EXPORTAR ------------------

# 1) CSV horário
df_hourly.to_csv("era5_cloud_hourly.csv")
print("✔ Guardado: era5_cloud_hourly.csv")

# 2) Daily mean (média diária)
df_daily_mean = df_hourly.resample("1D").mean(numeric_only=True)
df_daily_mean.to_csv("era5_cloud_daily_mean.csv")
print("✔ Guardado: era5_cloud_daily_mean.csv")

# 3) Daily mean / min / max
df_daily_stats = df_hourly.resample("1D").agg(
    mean_cloud=("cloudcover_pct", "mean"),
    min_cloud=("cloudcover_pct", "min"),
    max_cloud=("cloudcover_pct", "max")
)
df_daily_stats.to_csv("era5_cloud_daily_stats.csv")
print("✔ Guardado: era5_cloud_daily_stats.csv")

# 4) Monthly mean
df_monthly = df_hourly.resample("1MS").agg(
    mean_cloud=("cloudcover_pct", "mean"),
    min_cloud=("cloudcover_pct", "min"),
    max_cloud=("cloudcover_pct", "max"))
df_monthly.to_csv("era5_cloud_monthly.csv")
print("✔ Guardado: era5_cloud_monthly.csv")

print("\n🎉 Todos os CSVs foram gerados com sucesso!")

