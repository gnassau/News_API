from airflow.decorators import DAG
from airflow.operators.python import PythonOperator
import requests
from datetime import datetime

from newsapi import NewsApiClient
from dotenv import load_dotenv
import json
import pandas as pd
import psycopg2
import os
load_dotenv()


# Função que consome a API
def consumir_api():
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
        # Loop para processar os artigos
        for article in everything['articles']:
            titulo = article['title']
            descricao = article['description']
            url = article['url']
            fonte = article['source']['name']
            data_publicacao = article['publishedAt']
            conteudo_completo = article.get('content', 'Conteúdo não disponível')

            # Inserir os dados no banco de dados
            inserir_noticia(titulo, descricao, url, fonte, data_publicacao, conteudo_completo)

    # Função para inserir os dados no banco de dados
    def inserir_noticia(titulo, descricao, url, fonte, data_publicacao, conteudo_completo):
        cursor.execute("""
            INSERT INTO noticias_ai (titulo, descricao, url, fonte, data_publicacao, conteudo_completo)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (titulo, descricao, url, fonte, data_publicacao, conteudo_completo))
        conn.commit()

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

# Definir o DAG
dag = DAG(
    'consumir_api_diariamente',  # Nome da DAG
    description='DAG para consumir uma API diariamente e salvar os dados no banco de dados',  # Descrição da DAG
    schedule='0 8 * * *',  # Executa todo dia às 08:00 (UTC)
    start_date=datetime(2023, 1, 1, 8, 0),  # Data e hora de início da execução
    catchup=False,  # Impede que o Airflow execute a DAG retroativamente
)

task_teste = PythonOperator(
    task_id='consumindo_apis',  # ID da tarefa
    python_callable=consumir_api,  # Função a ser chamada
    dag=dag,  # DAG associada à tarefa
)
