# Databricks notebook source
import boto3
import pandas as pd
import os

# Credenciais AWS
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "sua_access_key_aqui")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "sua_secret_key_aqui")
AWS_BUCKET_NAME = os.environ.get("AWS_BUCKET_NAME", "shopflow-pipeline-thiago")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")

# Conecta no S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

print("Conectado ao S3 com sucesso!")

# Testa listando os arquivos da pasta bronze
response = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix="bronze/")
for obj in response["Contents"]:
    print(obj["Key"])

# Visualizando os pedidos
print("=== PEDIDOS ===")
display(df_orders)

# COMMAND ----------

# Função para ler CSV do S3
def ler_csv_s3(nome_arquivo):
    obj = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=f"bronze/{nome_arquivo}")
    df = pd.read_csv(obj["Body"])
    print(f"✓ {nome_arquivo} carregado — {df.shape[0]} linhas, {df.shape[1]} colunas")
    return df

# Lê as 3 tabelas principais
df_orders = ler_csv_s3("olist_orders_dataset.csv")
df_customers = ler_csv_s3("olist_customers_dataset.csv")
df_items = ler_csv_s3("olist_order_items_dataset.csv")

# Visualizando os pedidos
print("=== PEDIDOS ===")
display(df_orders)

# COMMAND ----------

# ==============================
# SILVER - Tratamento df_orders
# ==============================

# Colunas de data para converter
colunas_data = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

# Converte as colunas de texto para data
for coluna in colunas_data:
    df_orders[coluna] = pd.to_datetime(df_orders[coluna])

# Remove linhas onde order_id ou customer_id são nulos
df_orders_silver = df_orders.dropna(subset=["order_id", "customer_id"])

print(f"Orders Silver: {df_orders_silver.shape[0]} linhas")
print(f"Tipos das colunas:")
print(df_orders_silver.dtypes)

# COMMAND ----------

# Explorando o df_orders antes de tratar
print("=== SHAPE (linhas x colunas) ===")
print(df_orders.shape)

print("\n=== PRIMEIRAS 5 LINHAS ===")
print(df_orders.head())

print("\n=== TIPOS DE CADA COLUNA ===")
print(df_orders.dtypes)

print("\n=== QUANTIDADE DE NULOS POR COLUNA ===")
print(df_orders.isnull().sum())

print("\n=== ESTATÍSTICAS GERAIS ===")
print(df_orders.describe())

# COMMAND ----------

# Explorando o df_customers antes de tratar
print("=== SHAPE (linhas x colunas) ===")
print(df_customers.shape)

print("\n=== PRIMEIRAS 5 LINHAS ===")
print(df_customers.head())

print("\n=== TIPOS DE CADA COLUNA ===")
print(df_customers.dtypes)

print("\n=== QUANTIDADE DE NULOS POR COLUNA ===")
print(df_customers.isnull().sum())

# COMMAND ----------

# Explorando o df_items antes de tratar
print("=== SHAPE (linhas x colunas) ===")
print(df_items.shape)

print("\n=== PRIMEIRAS 5 LINHAS ===")
print(df_items.head())

print("\n=== TIPOS DE CADA COLUNA ===")
print(df_items.dtypes)

print("\n=== QUANTIDADE DE NULOS POR COLUNA ===")
print(df_items.isnull().sum())

# COMMAND ----------

# SILVER - df_orders
df_orders_silver = df_orders.drop_duplicates()

print(f"Orders Bronze: {df_orders.shape[0]} linhas")
print(f"Orders Silver: {df_orders_silver.shape[0]} linhas")
print(f"Duplicatas removidas: {df_orders.shape[0] - df_orders_silver.shape[0]}")

# COMMAND ----------

# SILVER - df_customers
df_customers_silver = df_customers.copy()

# Converte CEP de número para texto
df_customers_silver["customer_zip_code_prefix"] = df_customers_silver["customer_zip_code_prefix"].astype(str).str.zfill(5)

# Remove duplicatas
df_customers_silver = df_customers_silver.drop_duplicates()

print(f"Customers Bronze: {df_customers.shape[0]} linhas")
print(f"Customers Silver: {df_customers_silver.shape[0]} linhas")
print(f"\nTipos após tratamento:")
print(df_customers_silver.dtypes)
print(f"\nExemplo de CEP:")
print(df_customers_silver["customer_zip_code_prefix"].head())

# COMMAND ----------

# SILVER - df_items
df_items_silver = df_items.copy()

# Converte shipping_limit_date de texto para data
df_items_silver["shipping_limit_date"] = pd.to_datetime(df_items_silver["shipping_limit_date"])

# Remove duplicatas
df_items_silver = df_items_silver.drop_duplicates()

print(f"Items Bronze: {df_items.shape[0]} linhas")
print(f"Items Silver: {df_items_silver.shape[0]} linhas")
print(f"\nTipos após tratamento:")
print(df_items_silver.dtypes)

# COMMAND ----------

import io

# Função para salvar DataFrame no S3
def salvar_silver_s3(df, nome_arquivo):
    print(f"Salvando {nome_arquivo} no S3...")
    
    # Converte o DataFrame para CSV na memória
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Envia para o S3
    s3.put_object(
        Bucket=AWS_BUCKET_NAME,
        Key=f"silver/{nome_arquivo}",
        Body=csv_buffer.getvalue()
    )
    
    print(f"✓ {nome_arquivo} salvo em silver/ com sucesso!")

# Salva as 3 tabelas do Grupo 1
salvar_silver_s3(df_orders_silver, "orders_silver.csv")
salvar_silver_s3(df_customers_silver, "customers_silver.csv")
salvar_silver_s3(df_items_silver, "items_silver.csv")

print("\n✅ Grupo 1 salvo na camada Silver no S3!")

# COMMAND ----------

# Carrega o Grupo 2
df_payments = ler_csv_s3("olist_order_payments_dataset.csv")
df_reviews = ler_csv_s3("olist_order_reviews_dataset.csv")
df_sellers = ler_csv_s3("olist_sellers_dataset.csv")

# COMMAND ----------

# Explorando df_payments
print("=== PAYMENTS ===")
print(f"\nSHAPE: {df_payments.shape}")
print(f"\nTIPOS:")
print(df_payments.dtypes)
print(f"\nNULOS:")
print(df_payments.isnull().sum())
print(f"\nPRIMEIRAS 3 LINHAS:")
print(df_payments.head(3).to_string())

print("\n")

# Explorando df_reviews
print("=== REVIEWS ===")
print(f"\nSHAPE: {df_reviews.shape}")
print(f"\nTIPOS:")
print(df_reviews.dtypes)
print(f"\nNULOS:")
print(df_reviews.isnull().sum())
print(f"\nPRIMEIRAS 3 LINHAS:")
print(df_reviews.head(3).to_string())

print("\n")

# Explorando df_sellers
print("=== SELLERS ===")

# COMMAND ----------

# Explorando df_sellers
print("=== SELLERS ===")
print(f"\nSHAPE: {df_sellers.shape}")
print(f"\nTIPOS:")
print(df_sellers.dtypes)
print(f"\nNULOS:")
print(df_sellers.isnull().sum())
print(f"\nPRIMEIRAS 3 LINHAS:")
print(df_sellers.head(3).to_string())

# COMMAND ----------

# SILVER - df_payments
df_payments_silver = df_payments.drop_duplicates()

print(f"Payments Bronze: {df_payments.shape[0]} linhas")
print(f"Payments Silver: {df_payments_silver.shape[0]} linhas")
print(f"Duplicatas removidas: {df_payments.shape[0] - df_payments_silver.shape[0]}")

# COMMAND ----------

# SILVER - df_reviews
df_reviews_silver = df_reviews.copy()

# Converte datas de texto para data real
df_reviews_silver["review_creation_date"] = pd.to_datetime(df_reviews_silver["review_creation_date"])
df_reviews_silver["review_answer_timestamp"] = pd.to_datetime(df_reviews_silver["review_answer_timestamp"])

# Preenche nulos dos comentários
df_reviews_silver["review_comment_title"] = df_reviews_silver["review_comment_title"].fillna("Sem comentário")
df_reviews_silver["review_comment_message"] = df_reviews_silver["review_comment_message"].fillna("Sem comentário")

# Remove duplicatas
df_reviews_silver = df_reviews_silver.drop_duplicates()

print(f"Reviews Bronze: {df_reviews.shape[0]} linhas")
print(f"Reviews Silver: {df_reviews_silver.shape[0]} linhas")
print(f"\nNulos após tratamento:")
print(df_reviews_silver.isnull().sum())
print(f"\nTipos após tratamento:")
print(df_reviews_silver.dtypes)

# COMMAND ----------

# SILVER - df_sellers
df_sellers_silver = df_sellers.copy()

# Converte CEP de número para texto
df_sellers_silver["seller_zip_code_prefix"] = df_sellers_silver["seller_zip_code_prefix"].astype(str).str.zfill(5)

# Padroniza cidade para minúsculo
df_sellers_silver["seller_city"] = df_sellers_silver["seller_city"].str.lower().str.strip()

# Remove duplicatas
df_sellers_silver = df_sellers_silver.drop_duplicates()

print(f"Sellers Bronze: {df_sellers.shape[0]} linhas")
print(f"Sellers Silver: {df_sellers_silver.shape[0]} linhas")
print(f"\nTipos após tratamento:")
print(df_sellers_silver.dtypes)
print(f"\nExemplo de CEP e cidade:")
print(df_sellers_silver[["seller_zip_code_prefix", "seller_city"]].head())

# COMMAND ----------

# Salva o Grupo 2 na camada Silver no S3
salvar_silver_s3(df_payments_silver, "payments_silver.csv")
salvar_silver_s3(df_reviews_silver, "reviews_silver.csv")
salvar_silver_s3(df_sellers_silver, "sellers_silver.csv")

print("\n✅ Grupo 2 salvo na camada Silver no S3!")

# COMMAND ----------

# Carrega o Grupo 3
df_products = ler_csv_s3("olist_products_dataset.csv")
df_category = ler_csv_s3("product_category_name_translation.csv")
df_geo = ler_csv_s3("olist_geolocation_dataset.csv")

# COMMAND ----------

# Explorando Grupo 3
for nome, df in [("PRODUCTS", df_products), ("CATEGORY", df_category), ("GEOLOCATION", df_geo)]:
    print(f"\n{'='*50}")
    print(f"TABELA: {nome}")
    print(f"{'='*50}")
    print(f"\nSHAPE: {df.shape}")
    print(f"\nTIPOS:")
    print(df.dtypes)
    print(f"\nNULOS:")
    print(df.isnull().sum())
    print(f"\nPRIMEIRAS 3 LINHAS:")
    print(df.head(3).to_string())

# COMMAND ----------

# SILVER - df_products
df_products_silver = df_products.copy()

# Preenche nulos da categoria
df_products_silver["product_category_name"] = df_products_silver["product_category_name"].fillna("sem_categoria")

# Colunas numéricas para preencher com mediana
colunas_numericas = [
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
]

# Preenche cada coluna com a mediana
for coluna in colunas_numericas:
    mediana = df_products_silver[coluna].median()
    df_products_silver[coluna] = df_products_silver[coluna].fillna(mediana)

# Remove duplicatas
df_products_silver = df_products_silver.drop_duplicates()

print(f"Products Bronze: {df_products.shape[0]} linhas")
print(f"Products Silver: {df_products_silver.shape[0]} linhas")
print(f"\nNulos após tratamento:")
print(df_products_silver.isnull().sum())

# COMMAND ----------

# SILVER - df_category
df_category_silver = df_category.copy()

# Renomeia as colunas
df_category_silver.columns = ["category_portuguese", "category_english"]

# Remove duplicatas
df_category_silver = df_category_silver.drop_duplicates()

print(f"Category Bronze: {df_category.shape[0]} linhas")
print(f"Category Silver: {df_category_silver.shape[0]} linhas")
print(f"\nColunas renomeadas:")
print(df_category_silver.head(3).to_string())

# COMMAND ----------

# SILVER - df_geo
df_geo_silver = df_geo.copy()

# Converte CEP para texto
df_geo_silver["geolocation_zip_code_prefix"] = df_geo_silver["geolocation_zip_code_prefix"].astype(str).str.zfill(5)

# Remove duplicatas mantendo só um registro por CEP
df_geo_silver = df_geo_silver.drop_duplicates(subset=["geolocation_zip_code_prefix"])

print(f"Geolocation Bronze: {df_geo.shape[0]} linhas")
print(f"Geolocation Silver: {df_geo_silver.shape[0]} linhas")
print(f"Duplicatas removidas: {df_geo.shape[0] - df_geo_silver.shape[0]}")
print(f"\nTipos após tratamento:")
print(df_geo_silver.dtypes)

# COMMAND ----------

# Salva o Grupo 3 na camada Silver no S3
salvar_silver_s3(df_products_silver, "products_silver.csv")
salvar_silver_s3(df_category_silver, "category_silver.csv")
salvar_silver_s3(df_geo_silver, "geo_silver.csv")

print("\n✅ Grupo 3 salvo na camada Silver no S3!")