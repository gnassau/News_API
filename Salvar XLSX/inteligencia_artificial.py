from newsapi import NewsApiClient
from dotenv import load_dotenv
import json
import pandas as pd
import psycopg2
import os
load_dotenv()

# Carregar a chave da API do arquivo .env
api_key = os.getenv("API_KEY")
newsapi = NewsApiClient(api_key=api_key)

# Obter as principais notícias
everything = newsapi.get_everything(
    q='inteligência artificial'
    , language='pt'
    , sort_by='relevancy')

# Lista para armazenar os dados processados
dados_processados = []

# Loop para processar os artigos
for article in everything['articles']:
    dados_processados.append({
        'title': article['title'],
        'description': article['description'],
        'url': article['url'],
        'publishedAt': article['publishedAt'],
        'source': article['source']['name'],
        'content': article['content']
    })

# Criar um DataFrame com os dados processados
df = pd.DataFrame(dados_processados)

# Salvar no formato Excel (.xlsx)
df.to_excel("Inteligencia_artificial.xlsx", index=False)
print("Arquivo Excel criado com sucesso!")