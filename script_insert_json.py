import pandas as pd
import psycopg2
import json

# Ler dados do Excel
df = pd.read_json("C:/Users/annea/Downloads/desafio1/arquivo_json/json_data.json")

# Conectar-se ao banco de dados
conn = psycopg2.connect(
    host="localhost",
    database="bootcamp",
    user="root",
    password="root"
)

cur = conn.cursor()

with open('C:/Users/annea/Downloads/desafio1/arquivo_json/json_data.json', encoding= 'utf-8') as f:
    arquivo = json.load(f)

# Cria a tabela e schema

cur.execute('''CREATE SCHEMA dados_json;''')
cur.execute("CREATE TABLE IF NOT EXISTS dados_json.startup(id serial primary key, arquivo JSONB)")

# Inserir os dados na tabela
for d in arquivo:
    cur.execute("INSERT INTO dados_json.startup (arquivo) VALUES (%s)", [json.dumps(list(d.values()))])


conn.commit()
conn.close()



