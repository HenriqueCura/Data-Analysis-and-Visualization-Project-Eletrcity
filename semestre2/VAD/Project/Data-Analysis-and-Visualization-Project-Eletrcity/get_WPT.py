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
# ==========================================================
# 1) CONFIGURAÇÃO DO DOWNLOAD
# ==========================================================

years  = ['2023', '2024', '2025', '2026']
months = [f"{m:02d}" for m in range(1, 13)]
days   = [f"{d:02d}" for d in range(1, 32)]
hours  = [f"{h:02d}:00" for h in range(24)]

# Área grande o suficiente (apenas 17x5 gridpoints)
area = [41.25, -8.5, 37.25, -7.5]

outdir = Path("era5_multi_parts")
outdir.mkdir(exist_ok=True)

c = cdsapi.Client()

# ==========================================================
# 2) DOWNLOAD GRIBs
# ==========================================================

for y in years:
    for m in months:

        # evitar pedir meses futuros
        if y == '2026' and m == '03':
            break

        target = outdir / f"era5_multi_{y}_{m}.grib"
        if target.exists():
            print(f"↺ Já existe: {target.name}")
            continue

        print(f"⏬ Pedido: {y}-{m}")

        c.retrieve(
            "reanalysis-era5-single-levels",
            {
                "product_type": "reanalysis",
                "variable": [
                    "2m_temperature",
                    "10m_u_component_of_wind",
                    "10m_v_component_of_wind",
                    "total_precipitation"
                ],
                "year": y,
                "month": m,
                "day": days,
                "time": hours,
                "area": area,
                "format": "grib",
                "download_format": "unarchived"
            },
            str(target)
        )

        print(f"✔ Guardado: {target.name}")

# ==========================================================
# 3) PROCESSAMENTO → CSV diário e mensal
# ==========================================================

def open_group(grib_file, stepType):
    """Abre GRIB filtrando o grupo certo."""
    return xr.open_dataset(
        grib_file,
        engine="cfgrib",
        backend_kwargs={"filter_by_keys": {"stepType": stepType}}
    )

def spatial_mean(da, time_dim):
    dims = [d for d in da.dims if d != time_dim]
    return da.mean(dim=dims)

monthly_frames = []

for grib in sorted(outdir.glob("*.grib")):
    print(f"\n📄 A processar: {grib.name}")

    # instant (t2m, u10, v10)
    ds_inst = open_group(grib, "instant")

    # accum (tp)
    ds_acc  = open_group(grib, "accum")

    # merge seguro
    ds = xr.merge([ds_inst, ds_acc], compat="override", join="outer")

    time_dim = "time"

    # média espacial da área
    t2m = spatial_mean(ds["t2m"], time_dim).load()
    u10 = spatial_mean(ds["u10"], time_dim).load()
    v10 = spatial_mean(ds["v10"], time_dim).load()
    tp  = spatial_mean(ds["tp"],  time_dim).load()

    times = pd.to_datetime(ds[time_dim].values, utc=True)

    # construir dataframe horário (interno)
    df = pd.DataFrame(index=times)
    df["temp_C"] = t2m.values - 273.15
    df["wind_speed"] = np.sqrt(u10.values**2 + v10.values**2)
    df["precip_mm"] = tp.values * 1000.0   # m → mm

    monthly_frames.append(df)

# série completa
df_all = pd.concat(monthly_frames).sort_index()

# ==========================================================
# 4) CSV DIÁRIO (mean/min/max + precip sum)
# ==========================================================

df_daily = df_all.resample("1D").agg({
    "temp_C": ["mean", "min", "max"],
    "wind_speed": ["mean", "min", "max"],
    "precip_mm": ["sum"]
})
df_daily.columns = ["_".join(col) for col in df_daily.columns]
df_daily.to_csv("era5_daily_weather_stats.csv")
print("✔ era5_daily_weather_stats.csv criado!")

# ==========================================================
# 5) CSV MENSAL (mean + precip sum)
# ==========================================================

df_monthly = df_all.resample("1MS").agg({
    "temp_C": "mean",
    "wind_speed": "mean",
    "precip_mm": "sum"
})
df_monthly.to_csv("era5_monthly_weather.csv")
print("✔ era5_monthly_weather.csv criado!")

print("\n🎉 Tudo concluído: CSV diário e mensal gerados com sucesso!")
"""
import xarray as xr
import pandas as pd
import numpy as np
from pathlib import Path
from zipfile import ZipFile
from tempfile import TemporaryDirectory

BASE = Path("era5_multi_parts")  # pasta com os meses descarregados
OUT_HOURLY  = "era5_hourly_weather.csv"
OUT_DAILY   = "era5_daily_weather_stats.csv"
OUT_MONTHLY = "era5_monthly_weather.csv"

def is_zip(path: Path) -> bool:
    with open(path, "rb") as f:
        return f.read(2) == b"PK"

def detect_engine(nc_path: Path) -> str:
    with open(nc_path, "rb") as f:
        magic = f.read(4)
    if magic.startswith(b"\x89HD"):  # NetCDF4/HDF5
        return "h5netcdf"
    if magic.startswith(b"CDF"):     # NetCDF clássico
        return "netcdf4"             # (alternativa: engine='scipy')
    raise ValueError(f"{nc_path.name} não parece NetCDF válido (magic={magic!r})")

def open_nc_auto(nc_path: Path) -> xr.Dataset:
    engine = detect_engine(nc_path)
    # cache=False ajuda a libertar rapidamente o handle ao fechar
    ds = xr.open_dataset(nc_path, engine=engine, cache=False)
    return ds

def normalize_vars(ds: xr.Dataset) -> xr.Dataset:
    Garante nomes curtinhos: t2m, u10, v10, tp, se vierem com nomes longos.
    rename_map = {}
    if "2m_temperature" in ds.data_vars and "t2m" not in ds.data_vars:
        rename_map["2m_temperature"] = "t2m"
    if "10m_u_component_of_wind" in ds.data_vars and "u10" not in ds.data_vars:
        rename_map["10m_u_component_of_wind"] = "u10"
    if "10m_v_component_of_wind" in ds.data_vars and "v10" not in ds.data_vars:
        rename_map["10m_v_component_of_wind"] = "v10"
    if "total_precipitation" in ds.data_vars and "tp" not in ds.data_vars:
        rename_map["total_precipitation"] = "tp"
    if rename_map:
        ds = ds.rename(rename_map)
    return ds

def spatial_mean(var: xr.DataArray, time_dim: str) -> xr.DataArray:
    dims = [d for d in var.dims if d != time_dim]
    return var.mean(dim=dims) if dims else var

monthly_frames = []
raw_files = sorted(BASE.glob("*.nc"))

for f in raw_files:
    print(f"\n>> A processar: {f.name}")

    # Cada ficheiro (na verdade, ZIP) é tratado numa temp dir única
    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        if is_zip(f):
            with ZipFile(f, "r") as z:
                names = z.namelist()
                nc_names = [n for n in names if n.lower().endswith(".nc")]
                if not nc_names:
                    print(f"   ⚠ ZIP sem .nc: {f.name} (pode conter error.html/json do CDS). Ignorado.")
                    continue
                # Extrair todos os .nc para a temp dir
                for n in nc_names:
                    z.extract(n, tmpdir)

            # Localizar instant/accum (ou detetar por variáveis)
            ds_instant = ds_accum = None
            for nc_path in tmpdir.glob("**/*.nc"):
                try:
                    ds_tmp = open_nc_auto(nc_path)
                except Exception as e:
                    print(f"   ⚠ Falha ao abrir {nc_path.name}: {e}")
                    continue
                # Normalizar nomes e tempo
                ds_tmp = normalize_vars(ds_tmp)
                time_dim = "valid_time" if "valid_time" in ds_tmp.coords else "time"

                # Heurística: 'accum' contem tp; 'instant' contem t2m/u10/v10
                has_tp = "tp" in ds_tmp.data_vars or "total_precipitation" in ds_tmp.data_vars
                has_inst = any(v in ds_tmp.data_vars for v in ("t2m","u10","v10","2m_temperature","10m_u_component_of_wind","10m_v_component_of_wind"))

                if has_tp and ds_accum is None:
                    ds_accum = ds_tmp
                elif has_inst and ds_instant is None:
                    ds_instant = ds_tmp
                else:
                    # Se cair aqui, pode ser duplicado; fecha para libertar handle
                    ds_tmp.close()

            if ds_instant is None and ds_accum is None:
                print("   ✘ Não encontrei variáveis esperadas (t2m/u10/v10/tp). Ignorado.")
                continue

            # Merge (alinha por coords)
            pieces = [d for d in (ds_instant, ds_accum) if d is not None]
            ds_month = xr.merge(pieces, compat="override", join="outer")

        else:
            # Caso raro: NetCDF "puro"
            ds_month = open_nc_auto(f)
            ds_month = normalize_vars(ds_month)
            time_dim = "valid_time" if "valid_time" in ds_month.coords else "time"

        # Normalizações finais
        time_dim = "valid_time" if "valid_time" in ds_month.coords else "time"
        if "expver" in ds_month:
            ds_month = ds_month.drop_vars("expver")

        # Garantir que todas as variáveis existem
        missing = [v for v in ("t2m","u10","v10","tp") if v not in ds_month.data_vars]
        if missing:
            print(f"   ⚠ Variáveis em falta neste mês ({f.name}): {missing}. Vou continuar com as disponíveis se possível.")
            # Se faltar alguma essencial, ignora mês
            if not all(v in ds_month.data_vars for v in ("t2m","u10","v10","tp")):
                # fecha e segue
                ds_month.close()
                continue

        # Média espacial
        t2m_pt = spatial_mean(ds_month["t2m"], time_dim)
        u10_pt = spatial_mean(ds_month["u10"], time_dim)
        v10_pt = spatial_mean(ds_month["v10"], time_dim)
        tp_pt  = spatial_mean(ds_month["tp"],  time_dim)

        # Materializar em memória e FECHAR ficheiros (libertar locks)
        t2m_vals = t2m_pt.load().values
        u10_vals = u10_pt.load().values
        v10_vals = v10_pt.load().values
        tp_vals  = tp_pt.load().values
        times = pd.to_datetime(ds_month[time_dim].values, utc=True)

        # Agora é seguro fechar o dataset (liberta handles)
        ds_month.close()
        if 'ds_instant' in locals() and ds_instant is not None:
            try: ds_instant.close()
            except: pass
        if 'ds_accum' in locals() and ds_accum is not None:
            try: ds_accum.close()
            except: pass

        # Construir DF do mês em memória
        dfm = pd.DataFrame(index=times)
        dfm["temp_C"]     = t2m_vals - 273.15
        dfm["u10"]        = u10_vals
        dfm["v10"]        = v10_vals
        dfm["wind_speed"] = np.sqrt(dfm["u10"]**2 + dfm["v10"]**2)
        dfm["precip_mm"]  = tp_vals * 1000.0  # m -> mm (acumulado hora)

        monthly_frames.append(dfm)

# Concatenar todos os meses -> série completa
if not monthly_frames:
    raise SystemExit("Nenhum mês válido foi processado. Verifique permissões e conteúdo dos ZIPs.")

df_hourly = pd.concat(monthly_frames).sort_index()
df_hourly.to_csv(OUT_HOURLY)
print(f"\n✔ Guardado: {OUT_HOURLY}")

# Diário: média/min/max; precip = soma diária
df_daily = df_hourly.resample("1D").agg({
    "temp_C":     ["mean","min","max"],
    "wind_speed": ["mean","min","max"],
    "precip_mm":  ["sum"]
})
df_daily.columns = ["_".join(col) for col in df_daily.columns]
df_daily.to_csv(OUT_DAILY)
print(f"✔ Guardado: {OUT_DAILY}")

# Mensal: média; precip = soma mensal
df_monthly = df_hourly.resample("1MS").agg({
    "temp_C":     "mean",
    "wind_speed": "mean",
    "precip_mm":  "sum"
})
df_monthly.to_csv(OUT_MONTHLY)
print(f"✔ Guardado: {OUT_MONTHLY}")

print("\n🎉 CSVs gerados com sucesso (hourly, daily stats, monthly).")


BASE = Path("era5_multi_parts")      # <-- muda aqui se necessário
files = sorted(BASE.glob("*.nc"))

print(f"\nEncontrados {len(files)} ficheiros.\n")

def detect_engine(nc_path: Path):
    #Descobre o engine correto para abrir o NetCDF.
    with open(nc_path, "rb") as f:
        magic = f.read(4)
    if magic.startswith(b"\x89HD"):  # HDF5 / NetCDF4
        return "h5netcdf"
    if magic.startswith(b"CDF"):
        return "netcdf4"
    return None  # Não é NetCDF válido

for f in files:
    print("="*80)
    print(f"📄 Ficheiro: {f.name}")

    # 1) Ver assinatura (ZIP ou NetCDF)
    with open(f, "rb") as fp:
        magic = fp.read(4)

    print("Magic bytes:", magic)

    # 2) Se for ZIP → listar conteúdo
    if magic.startswith(b"PK"):
        print("➡ Este ficheiro é um ZIP (mesmo tendo extensão .nc).")
        with ZipFile(f, "r") as z:
            names = z.namelist()
            print("Conteúdo do ZIP:")
            for name in names:
                print("  -", name)

        # 3) Abrir cada .nc dentro do ZIP sem deixar locks
        print("\n🔍 A inspecionar o conteúdo interno dos .nc ...\n")
        with TemporaryDirectory() as tmp:
            tmpdir = Path(tmp)
            with ZipFile(f, "r") as z:
                for name in z.namelist():
                    if name.endswith(".nc"):
                        z.extract(name, tmpdir)
                        nc_path = tmpdir / name

                        # escolher engine com base na assinatura interna
                        engine = detect_engine(nc_path)
                        if engine is None:
                            print(f"  ⚠ {name}: NÃO é NetCDF válido.")
                            continue

                        try:
                            ds = xr.open_dataset(nc_path, engine=engine)
                        except Exception as e:
                            print(f"  ⚠ Erro ao abrir {name}: {e}")
                            continue

                        print(f"  ✔ {name}:")
                        print("     Variáveis:", list(ds.data_vars))
                        print("     Coords    :", list(ds.coords))
                        ds.close()

    # 4) Caso seja NetCDF direto
    elif magic.startswith(b"\x89HD") or magic.startswith(b"CDF"):
        print("➡ Este ficheiro é NetCDF.")
        engine = detect_engine(f)
        try:
            ds = xr.open_dataset(f, engine=engine)
            print("Variáveis:", list(ds.data_vars))
            print("Coords   :", list(ds.coords))
            ds.close()
        except Exception as e:
            print("⚠ Erro ao abrir NetCDF:", e)

    else:
        print("⚠ Este ficheiro NÃO é NetCDF nem ZIP — parece corrompido.")

print("\n✓ Inspeção completa.")
"""


