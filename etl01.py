from pyodbc import drivers
import pandas as pd 
from sqlalchemy import create_engine

dados = pd.read_excel('caminho/do/arquivo.xlsx')
dados['vendas'] = dados['vendas'].fillna(0) #caso no excel tenha um NaN 
dados['vendas'] = dados['vendas'].astype(int) #se for float vira um inteiro


#conecta em um banco de dados
server = ''      # Nome do servidor
database = ''    # Nome do banco de dados
driver = ''      # Driver de conexão
database_con = f"mssql+pyodbc://{user}:{senha}@{server}/{database}?driver={driver}" # String de conexão

engine = create_engine(database_con, fast_executemany = True)

con = engine.connect()


dados.to_sql('VENDAS', con = con, index = False, if_exists = "append")

