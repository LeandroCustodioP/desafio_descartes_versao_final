import pandas as pd
import os

# Obtém o caminho completo do arquivo CSV
caminho = os.path.join("..", "data_lake", "trusted")


# Importando todos os dataframes
df_orders = pd.read_parquet(os.path.join(caminho, "orders.parquet"))
df_organization = pd.read_parquet(os.path.join(caminho, "organization.parquet"))
df_route = pd.read_parquet(os.path.join(caminho, "route.parquet"))
df_stop = pd.read_parquet(os.path.join(caminho, "stop.parquet"))

# fazendo merge de todos os dataframes
result = pd.merge(df_orders, df_stop, how='left', left_on='STOP_ID', right_on='ID')
result = pd.merge(result, df_route, how='left', left_on='ROUTE_ID', right_on='ID')
result = pd.merge(result, df_organization, how='left', left_on='ORGANIZATION_ID', right_on='ID')
result = result.loc[:, ~result.columns.duplicated()]

# Criando df_relatório1
df_relatorio1 = result[['DESCRIPTION', 'ORDER_NUMBER', 'ORGANIZATION_ID', 'ROUTE_ID', 'STOP_ID']].copy()

# Criando df_status
df_status = result[['ORDER_NUMBER', 'CREATION_DATE','STATUS']].copy()

# Criando dataframe pedidos1
pedidos1 = df_relatorio1.groupby('DESCRIPTION')['ORDER_NUMBER'].count().reset_index()

# Criando dataframe pedidos_status
pedidos_status = df_status.groupby('STATUS')['ORDER_NUMBER'].count().reset_index()


# Exportando arquivo formado pela junção de todos os outros no formato parquet para o data_lake
caminho = os.path.join("..","data_lake", "business")
if not os.path.exists(caminho):
    os.makedirs(caminho)
    
result.to_parquet(os.path.join(caminho, "result.parquet"))
df_relatorio1.to_parquet(os.path.join(caminho, "df_relatorio1.parquet"))
pedidos1.to_parquet(os.path.join(caminho, "pedidos1.parquet"))
df_status.to_parquet(os.path.join(caminho, "df_status.parquet"))
pedidos_status.to_parquet(os.path.join(caminho, "pedidos_status.parquet"))
