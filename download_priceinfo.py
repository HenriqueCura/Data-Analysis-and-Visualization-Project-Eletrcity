import os
import requests
import pandas as pd
from datetime import datetime, timedelta

# Criar pasta local
os.makedirs("omie_raw", exist_ok=True)

# Intervalo de datas (últimos 4 anos)
end_date = datetime.today()
start_date = datetime(2023,1,1,0,0)

# Função para testar download do ficheiro de um dia
def download_omie_day(date):
    date_str = date.strftime("%Y-%m-%d")
    
    # Exemplo de URL frequente usado pelo OMIE para mercado diário (day-ahead)
    url = f"https://www.omie.es/sites/default/files/2023-04/Resultados_Mercado_Diario_{date_str}.csv"
    url
    
    local_file = f"omie_raw/{date_str}.csv"

    response = requests.get(url)

    if response.status_code == 200 and len(response.content) > 100:
        with open(local_file, "wb") as f:
            f.write(response.content)
        print("✔ Descarregado:", date_str)
        return True
    else:
        print("✘ Não encontrado:", date_str)
        return False


# Loop pelas datas
current_date = start_date
while current_date <= end_date:
    download_omie_day(current_date)
    current_date += timedelta(days=1)

# Agregar todos os ficheiros encontrados
dfs = []
for file in os.listdir("omie_raw"):
    if file.endswith(".csv"):
        df = pd.read_csv(os.path.join("omie_raw", file), sep=";", encoding="latin1")
        df["date"] = file.replace(".csv", "")
        dfs.append(df)

# Concatenar num DataFrame único
if dfs:
    final_df = pd.concat(dfs, ignore_index=True)
    final_df.to_csv("omie_4anos_portugal.csv", index=False)
    print("📁 Ficheiro final criado: omie_4anos_portugal.csv")
else:
    print("⚠ Nenhum ficheiro válido foi descarregado.")