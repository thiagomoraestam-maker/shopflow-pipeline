# Databricks notebook source
import boto3
import pandas as pd
import os
import io

# Credenciais AWS
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

# Função para ler CSV do S3
def ler_silver_s3(nome_arquivo):
    obj = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=f"silver/{nome_arquivo}")
    df = pd.read_csv(obj["Body"])
    print(f"✓ {nome_arquivo} carregado — {df.shape[0]} linhas")
    return df

print("Conectado ao S3 com sucesso!")

# COMMAND ----------

# Carrega as tabelas Silver do S3
df_orders = ler_silver_s3("orders_silver.csv")
df_customers = ler_silver_s3("customers_silver.csv")
df_items = ler_silver_s3("items_silver.csv")
df_payments = ler_silver_s3("payments_silver.csv")
df_reviews = ler_silver_s3("reviews_silver.csv")
df_products = ler_silver_s3("products_silver.csv")
df_category = ler_silver_s3("category_silver.csv")

print("\n✅ Todas as tabelas Silver carregadas!")

# COMMAND ----------

# ============================
# GOLD - Pergunta 1
# Vendas ao longo do tempo
# ============================

# Converte data para datetime
df_orders["order_purchase_timestamp"] = pd.to_datetime(df_orders["order_purchase_timestamp"])

# Extrai ano e mês
df_orders["ano_mes"] = df_orders["order_purchase_timestamp"].dt.to_period("M").astype(str)

# Join orders + payments
df_vendas = df_orders.merge(df_payments, on="order_id", how="left")

# Agrega por mês
gold_vendas_mes = df_vendas.groupby("ano_mes").agg(
    total_receita=("payment_value", "sum"),
    qtd_pedidos=("order_id", "count"),
).reset_index()

# Calcula ticket médio
gold_vendas_mes["ticket_medio"] = (gold_vendas_mes["total_receita"] / gold_vendas_mes["qtd_pedidos"]).round(2)
gold_vendas_mes["total_receita"] = gold_vendas_mes["total_receita"].round(2)

# Ordena por data
gold_vendas_mes = gold_vendas_mes.sort_values("ano_mes")

print("✅ Gold - Vendas por mês:")
print(gold_vendas_mes.head(10).to_string())

# COMMAND ----------

# Função para salvar Gold no S3
def salvar_gold_s3(df, nome_arquivo):
    print(f"Salvando {nome_arquivo} no S3...")
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(
        Bucket=AWS_BUCKET_NAME,
        Key=f"gold/{nome_arquivo}",
        Body=csv_buffer.getvalue()
    )
    print(f"✓ {nome_arquivo} salvo em gold/ com sucesso!")

# Salva Pergunta 1
salvar_gold_s3(gold_vendas_mes, "gold_vendas_por_mes.csv")

# COMMAND ----------

# ============================
# GOLD - Pergunta 2
# Vendas por estado
# ============================

# Join orders + customers + payments
df_estados = df_orders.merge(df_customers, on="customer_id", how="left")
df_estados = df_estados.merge(df_payments, on="order_id", how="left")

# Agrega por estado
gold_vendas_estado = df_estados.groupby("customer_state").agg(
    total_receita=("payment_value", "sum"),
    qtd_pedidos=("order_id", "count")
).reset_index()

# Calcula ticket médio
gold_vendas_estado["ticket_medio"] = (gold_vendas_estado["total_receita"] / gold_vendas_estado["qtd_pedidos"]).round(2)
gold_vendas_estado["total_receita"] = gold_vendas_estado["total_receita"].round(2)

# Ordena por receita
gold_vendas_estado = gold_vendas_estado.sort_values("total_receita", ascending=False)

print("✅ Gold - Vendas por estado:")
print(gold_vendas_estado.head(10).to_string())

# Salva no S3
salvar_gold_s3(gold_vendas_estado, "gold_vendas_por_estado.csv")

# COMMAND ----------

# ============================
# GOLD - Pergunta 3
# Categorias mais vendidas
# ============================

# Join items + products + category
df_categorias = df_items.merge(df_products, on="product_id", how="left")
df_categorias = df_categorias.merge(df_category, left_on="product_category_name", right_on="category_portuguese", how="left")

# Agrega por categoria em inglês
gold_categorias = df_categorias.groupby("category_english").agg(
    total_receita=("price", "sum"),
    qtd_itens_vendidos=("order_id", "count")
).reset_index()

# Arredonda receita
gold_categorias["total_receita"] = gold_categorias["total_receita"].round(2)

# Ordena por receita
gold_categorias = gold_categorias.sort_values("total_receita", ascending=False)

print("✅ Gold - Top 10 categorias:")
print(gold_categorias.head(10).to_string())

# Salva no S3
salvar_gold_s3(gold_categorias, "gold_categorias.csv")

# COMMAND ----------

# ============================
# GOLD - Pergunta 4
# Satisfação dos clientes
# ============================

# Converte datas
df_orders["order_delivered_customer_date"] = pd.to_datetime(df_orders["order_delivered_customer_date"])
df_orders["order_purchase_timestamp"] = pd.to_datetime(df_orders["order_purchase_timestamp"])

# Join reviews + orders
df_satisfacao = df_reviews.merge(df_orders, on="order_id", how="left")

# Calcula tempo de entrega em dias
df_satisfacao["dias_entrega"] = (
    df_satisfacao["order_delivered_customer_date"] - 
    df_satisfacao["order_purchase_timestamp"]
).dt.days

# Remove pedidos sem data de entrega
df_satisfacao = df_satisfacao.dropna(subset=["dias_entrega"])

# Agrega por nota
gold_satisfacao = df_satisfacao.groupby("review_score").agg(
    qtd_avaliacoes=("review_id", "count"),
    media_dias_entrega=("dias_entrega", "mean")
).reset_index()

# Arredonda
gold_satisfacao["media_dias_entrega"] = gold_satisfacao["media_dias_entrega"].round(1)

print("✅ Gold - Satisfação dos clientes:")
print(gold_satisfacao.to_string())

# Salva no S3
salvar_gold_s3(gold_satisfacao, "gold_satisfacao.csv")