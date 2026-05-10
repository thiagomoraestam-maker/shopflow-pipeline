# -*- coding: utf-8 -*-
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from dotenv import load_dotenv

# Carrega credenciais
load_dotenv()

# Conecta no PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database=os.getenv("PG_DATABASE"),
    user="postgres",
    password=os.getenv("PG_PASSWORD")
)

print("✅ Conectado ao PostgreSQL!")

# Lê as tabelas
df_vendas_mes = pd.read_sql("SELECT * FROM vendas_por_mes ORDER BY ano_mes", conn)
df_vendas_estado = pd.read_sql("SELECT * FROM vendas_por_estado ORDER BY total_receita DESC LIMIT 10", conn)
df_categorias = pd.read_sql("SELECT * FROM categorias ORDER BY total_receita DESC LIMIT 10", conn)
df_satisfacao = pd.read_sql("SELECT * FROM satisfacao ORDER BY review_score", conn)

print("✅ Dados carregados do PostgreSQL!")

# Configuração visual
sns.set_theme(style="whitegrid")
fig, axes = plt.subplots(2, 2, figsize=(18, 12))
fig.suptitle("ShopFlow - Dashboard de Análise de Dados", fontsize=16, fontweight="bold")

# Gráfico 1 - Vendas por mês
axes[0, 0].plot(df_vendas_mes["ano_mes"], df_vendas_mes["total_receita"], marker="o", color="#2196F3")
axes[0, 0].set_title("Evolução de Vendas por Mês")
axes[0, 0].set_xlabel("Mês")
axes[0, 0].set_ylabel("Receita (R$)")
axes[0, 0].tick_params(axis="x", rotation=45)

# Gráfico 2 - Top 10 estados
axes[0, 1].barh(df_vendas_estado["customer_state"], df_vendas_estado["total_receita"], color="#4CAF50")
axes[0, 1].set_title("Top 10 Estados por Receita")
axes[0, 1].set_xlabel("Receita (R$)")
axes[0, 1].set_ylabel("Estado")

# Gráfico 3 - Top 10 categorias
axes[1, 0].barh(df_categorias["category_english"], df_categorias["total_receita"], color="#FF9800")
axes[1, 0].set_title("Top 10 Categorias por Receita")
axes[1, 0].set_xlabel("Receita (R$)")
axes[1, 0].set_ylabel("Categoria")

# Gráfico 4 - Satisfação
axes[1, 1].bar(df_satisfacao["review_score"], df_satisfacao["media_dias_entrega"], color="#F44336")
axes[1, 1].set_title("Média de Dias de Entrega por Nota")
axes[1, 1].set_xlabel("Nota (1-5)")
axes[1, 1].set_ylabel("Média de Dias")

# Salva o dashboard
plt.tight_layout()
os.makedirs("docs", exist_ok=True)
plt.savefig("docs/dashboard_shopflow.png", dpi=150, bbox_inches="tight")
plt.show()

print("✅ Dashboard salvo em docs/dashboard_shopflow.png!")

# Fecha conexão
conn.close()