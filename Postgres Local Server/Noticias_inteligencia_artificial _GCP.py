from newsapi import NewsApiClient
from dotenv import load_dotenv
import json
import pandas as pd
import psycopg2
import os
load_dotenv()

# Carregar a chave da API do arquivo .env
API_KEY = os.getenv("API_KEY")
DATABASE_URL = os.getenv("GCP_URL")

# Conectar ao banco de dados PostgreSQL
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# Função para criar a tabela no banco de dados (caso não exista)
def criar_tabela():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS noticias_ai (
        id SERIAL PRIMARY KEY,
        titulo TEXT,
        descricao TEXT,
        url TEXT,
        fonte TEXT,
        data_publicacao TIMESTAMP,
        conteudo_completo TEXT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("Tabela 'noticias' criada com sucesso (se não existia).")

# Obter as principais notícias
def obter_noticias():
    newsapi = NewsApiClient(api_key=API_KEY)
    everything = newsapi.get_everything(
        q='inteligência artificial'
        , language='pt'
        , sort_by='relevancy')
    return everything


def salvar_noticias(everything):
    # Lista para armazenar todas as notícias a serem inseridas
    noticias_para_inserir = []
    
    # Loop para processar os artigos
    for article in everything['articles']:
        titulo = article['title']
        descricao = article['description']
        url = article['url']
        fonte = article['source']['name']
        data_publicacao = article['publishedAt']
        conteudo_completo = article.get('content', 'Conteúdo não disponível')
        
        # Adicionar os dados na lista de tuplas
        noticias_para_inserir.append((
            titulo, descricao, url, fonte, data_publicacao, conteudo_completo
        ))
    
    # Inserir todos os dados no banco de dados de uma vez
    inserir_noticias_batch(noticias_para_inserir)

def inserir_noticias_batch(noticias_para_inserir):
    # Inserir os dados no banco de dados de uma vez só usando executemany
    cursor.executemany("""
        INSERT INTO noticias_ai (titulo, descricao, url, fonte, data_publicacao, conteudo_completo)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, noticias_para_inserir)
    conn.commit()
    print(f"{len(noticias_para_inserir)} notícias inseridas com sucesso.")

if __name__ == "__main__":
    # Criar a tabela no banco de dados
    criar_tabela()

    # Obter as notícias
    noticias = obter_noticias()

    # Salvar as notícias no banco de dados
    salvar_noticias(noticias)

    # Fechar a conexão com o banco de dados
    cursor.close()
    conn.close()

print("Notícias inseridas com sucesso no banco de dados!")



# Loops de páginas (desconsiderar)
# def obter_noticias():
#     newsapi = NewsApiClient(api_key=API_KEY)
#     all_articles = []
#     page = 1  # Começando pela primeira página
#     while True:
#         # Consultar a API para a página atual
#         everything = newsapi.get_everything(
#             q='inteligência artificial',
#             language='pt',
#             sort_by='relevancy',
#             page=page
#         )
#         # Adiciona os artigos da página à lista
#         all_articles.extend(everything['articles'])
#         # Se não houver artigos, interrompa o loop
#         if not everything['articles']:
#             break
#         page += 1  # Caso contrário, vamos para a próxima página
#     return all_articles