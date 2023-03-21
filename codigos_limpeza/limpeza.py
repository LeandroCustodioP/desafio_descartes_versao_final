import pandas as pd
import numpy as np
import os

from funcoes_aux import rename_columns, converter_para_datetime, convert_to_int, convert_to_float, import_csv_to_dataframe

if __name__== "__main__":
    # importando arquivo arquivos csv
    df1 = import_csv_to_dataframe('orders.csv')
    df2 = import_csv_to_dataframe('organization.csv')
    df3 = import_csv_to_dataframe('route.csv')
    df4 = import_csv_to_dataframe('stop.csv')

    # Renomeando colunas
    nomes_novos1 = ['ID', 'STOP_ID', 'ORDER_NUMBER', 'CREATION_DATE']
    df1 = rename_columns(df1, nomes_novos1)

    nomes_novos2 = ['ID', 'PARENT_ORGANIZATION_ID', 'AKEY', 'DESCRIPTION']
    df2 = rename_columns(df2, nomes_novos2)

    nomes_novos3 = ['ID', 'AKEY', 'ORGANIZATION_ID', 'STATUS', 'PLANNED_DEPARTURE', 'ACTUAL_DEPARTURE',
                    'PLANNED_ARRIVAL', 'ACTUAL_ARRIVAL', 'ROUTE_DATE']
    df3 = rename_columns(df3, nomes_novos3)

    nomes_novos4 = ['ID','ROUTE_ID','PLANNED_ARRIVAL','ACTUAL_ARRIVAL','PLANNED_DEPARTURE',
                    'ACTUAL_DEPARTURE', 'ACTUAL_DISTANCE']
    df4 = rename_columns(df4, nomes_novos4)

    # Mudando caracteres estranhos
    mudar_caracteres = {'SÃ?O LUÃ?S':'SAO LUIS', 'ITAJAÃ?':'ITAJAI', 'BETIM/ TIMÃ“TEO':'BETIM/TIMOTEO',
            'BRASÃ?LIA':'BRASILIA', 'ARAUCÃ?RIA':'ARAUCARIA', 'BELÃ‰M':'BELEM', 'JEQUIÃ‰':'JEQUIAO',
            'JUÃ?Z DE FORA':'JUIZ DE FORA'}
    
    df2.replace(mudar_caracteres, inplace=True)

    # remover valores NA para converter para inteiro
    df2.drop(0, inplace=True)



    # Convertendo colunas para o tipo data
    colunas_para_data1 = ['CREATION_DATE']
    df1 = converter_para_datetime(df1,colunas_para_data1)

    colunas_para_data2 = ['PLANNED_DEPARTURE', 'ACTUAL_DEPARTURE','PLANNED_ARRIVAL',
                          'ACTUAL_ARRIVAL', 'ROUTE_DATE']
    df3 = converter_para_datetime(df3, colunas_para_data2)

    colunas_para_data3 = ['PLANNED_DEPARTURE', 'ACTUAL_DEPARTURE','PLANNED_ARRIVAL',
                          'ACTUAL_ARRIVAL']
    df4 = converter_para_datetime(df4, colunas_para_data3)

    # convertendo para inteiro
    colunas_para_int = ['PARENT_ORGANIZATION_ID']
    df2 = convert_to_int(df2, colunas_para_int)



    # Exportando arquivo orders no formato parquet para o data_lake
    caminho = os.path.join("..","data_lake", "trusted")
    if not os.path.exists(caminho):
        os.makedirs(caminho)
        
    df1.to_parquet(os.path.join(caminho, "orders.parquet"))
    df2.to_parquet(os.path.join(caminho, "organization.parquet"))
    df3.to_parquet(os.path.join(caminho, "route.parquet"))
    df4.to_parquet(os.path.join(caminho, "stop.parquet"))

    
    

