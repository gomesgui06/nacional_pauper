# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Libs

# COMMAND ----------

from pyspark.sql import SparkSession
import pandas as pd  
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import Row
from pyspark.sql import SparkSession
import re

spark = SparkSession.builder.appName("nacionalPauper").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura dos dados

# COMMAND ----------

df_facebook = pd.read_csv("seu/caminho/scrapper.csv")
df_geral = pd.read_csv("seu/caminho/lista_jogadores.csv")

# df_facebook = gsheet.get_dataset(spreadsheet_id = '1IuOcyr0P5wGclH3RoZzl6qyjbKmD3NcgCj9s57O-T8A', range = 'Scrapper decklist!A:N')
# df_geral = gsheet.get_dataset(spreadsheet_id = '1IuOcyr0P5wGclH3RoZzl6qyjbKmD3NcgCj9s57O-T8A', range = 'Lista de jogadores!A:H')

df_facebook.display()
df_geral.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Funções

# COMMAND ----------

def add_comma_between_letter_and_digit(text):
    return re.sub(r'([A-Za-z])(\d)', r'\1 \2', text)

add_comma_udf = udf(add_comma_between_letter_and_digit, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ### ETL

# COMMAND ----------

regex_main_deck = r"Main Deck:(.*?)(?=Sideboard|$)"
regex_sideboard = r"Sideboard:(.*?)(?=--|$)"
regex_player = r"\b(SP|RJ|MG|MT|PR|SC|RS)\w*.*?\d"

df_facebook = (df_facebook.withColumn("main_deck_data", F.regexp_extract(F.col('content'), regex_main_deck, 1))
       .withColumn("sideboard_data", F.regexp_extract(F.col('content'), regex_sideboard, 1))
       .withColumn("player_name_data", F.regexp_extract(F.col('content'), regex_player, 0))
       )

df_facebook = (df_facebook.withColumn("main_deck_adjusted", add_comma_udf(F.col('main_deck_data')))
        .withColumn("sideboard_adjusted", add_comma_udf(F.col('sideboard_data'))))


# df_facebook.display()

# COMMAND ----------

lista_nomes = df_geral.select('nome').collect()
lista_decklists = df_facebook.select('player_name_data').collect()

resultado_nome = []
resultado_decklist_chave =[]

for decklist in lista_decklists:
  for nome in lista_nomes:
    match = re.search(nome.nome, decklist.player_name_data)
    if match:
      nome_jogador = match.group(0).strip()
      resultado_nome.append(nome_jogador)
      resultado_decklist_chave.append(decklist.player_name_data)

rdd = spark.sparkContext.parallelize([Row(nome_jogador=value1, chave=value2) for value1, value2 in zip(resultado_nome, resultado_decklist_chave)])
df_chave = spark.createDataFrame(rdd)

# df_chave.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join

# COMMAND ----------

final_df = (df_geral.join(df_chave, df_geral["nome"]==df_chave['nome_jogador'], 'left')
                    .join(df_facebook, df_facebook["player_name_data"] == df_chave['chave'], 'left')
                    )
final_df.display()

# COMMAND ----------

columns = ['nome', 'data', 'loja', 'cidade', 'estado', 'deck', 'post_url', 'main_deck_adjusted', 'sideboard_adjusted']
final_df.select([F.lower(column).alias(column.lower()) for column in final_df.columns]).orderBy(F.col('nome').asc()).display()
