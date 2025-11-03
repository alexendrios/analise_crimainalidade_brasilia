import pandas as pd
from io import BytesIO
import requests

# URL da planilha
url = "https://dados.df.gov.br/dataset/3435c8b7-5d61-4541-bf1c-38bdf9e34fd8/resource/cddb5da1-8ba4-444d-987b-a62174871025/download/tabelasseriehistorica-roubo-a-transeunte.xlsx"

response = requests.get(url)
df = pd.read_excel(BytesIO(response.content), header=2)

# Remove colunas completamente vazias
df = df.dropna(axis=1, how="all")

# Lista todas as colunas para inspeção
print(df.columns.tolist())

print(df)