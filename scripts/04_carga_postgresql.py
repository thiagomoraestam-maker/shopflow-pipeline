# -*- coding: utf-8 -*-
import boto3
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
from io import StringIO

# Carrega credenciais
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

# Conecta no S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Função para ler Gold do S3
def ler_gold_s3(nome_arquivo):
    obj = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=f"gold/{nome_arquivo}")
    df = pd.read_csv(obj["Body"])
    print(f"✓ {nome_arquivo} carregado — {df.shape[0]} linhas")
    return df

# Carrega os 4 arquivos Gold
df_vendas_mes = ler_gold_s3("gold_vendas_por_mes.csv")
df_vendas_estado = ler_gold_s3("gold_vendas_por_estado.csv")
df_categorias = ler_gold_s3("gold_categorias.csv")
df_satisfacao = ler_gold_s3("gold_satisfacao.csv")

print("\n✅ Todos os arquivos Gold carregados!")

# Conecta no PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database=os.getenv("PG_DATABASE"),
    user="postgres",
    password=os.getenv("PG_PASSWORD")
)

cursor = conn.cursor()
print("✅ Conectado ao PostgreSQL!")

# Cria as tabelas
cursor.execute("""
    CREATE TABLE IF NOT EXISTS vendas_por_mes (
        ano_mes VARCHAR(10),
        total_receita NUMERIC(12,2),
        qtd_pedidos INTEGER,
        ticket_medio NUMERIC(10,2)
    );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS vendas_por_estado (
        customer_state VARCHAR(2),
        total_receita NUMERIC(12,2),
        qtd_pedidos INTEGER,
        ticket_medio NUMERIC(10,2)
    );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS categorias (
        category_english VARCHAR(100),
        total_receita NUMERIC(12,2),
        qtd_itens_vendidos INTEGER
    );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS satisfacao (
        review_score INTEGER,
        qtd_avaliacoes INTEGER,
        media_dias_entrega NUMERIC(5,1)
    );
""")

conn.commit()
print("✅ Tabelas criadas no PostgreSQL!")

# Insere dados em vendas_por_mes
for _, row in df_vendas_mes.iterrows():
    cursor.execute("""
        INSERT INTO vendas_por_mes (ano_mes, total_receita, qtd_pedidos, ticket_medio)
        VALUES (%s, %s, %s, %s)
    """, (row["ano_mes"], row["total_receita"], row["qtd_pedidos"], row["ticket_medio"]))

print("✅ vendas_por_mes inserido!")

# Insere dados em vendas_por_estado
for _, row in df_vendas_estado.iterrows():
    cursor.execute("""
        INSERT INTO vendas_por_estado (customer_state, total_receita, qtd_pedidos, ticket_medio)
        VALUES (%s, %s, %s, %s)
    """, (row["customer_state"], row["total_receita"], row["qtd_pedidos"], row["ticket_medio"]))

print("✅ vendas_por_estado inserido!")

# Insere dados em categorias
for _, row in df_categorias.iterrows():
    cursor.execute("""
        INSERT INTO categorias (category_english, total_receita, qtd_itens_vendidos)
        VALUES (%s, %s, %s)
    """, (row["category_english"], row["total_receita"], row["qtd_itens_vendidos"]))

print("✅ categorias inserido!")

# Insere dados em satisfacao
for _, row in df_satisfacao.iterrows():
    cursor.execute("""
        INSERT INTO satisfacao (review_score, qtd_avaliacoes, media_dias_entrega)
        VALUES (%s, %s, %s)
    """, (int(row["review_score"]), int(row["qtd_avaliacoes"]), float(row["media_dias_entrega"])))

print("✅ satisfacao inserido!")

# Confirma todas as inserções
conn.commit()

# Fecha a conexão
cursor.close()
conn.close()

print("\n🎉 Pipeline completo! Dados carregados no PostgreSQL!")