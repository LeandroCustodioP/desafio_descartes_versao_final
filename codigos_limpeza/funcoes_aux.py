import os
import pandas as pd
import numpy as np

from datetime import datetime, timedelta

# Pré processamento
def import_csv_to_dataframe(filename):
    '''
    Importa um arquivo CSV para um Dataframe do Pandas

    Args:
    file_path (str): O caminho completo do arquivo CSV.
    
    Returns:
    Um DataFrame do pandas com os dados do arquivo CSV.
    '''
    
    # Obtém o caminho completo do arquivo CSV
    file_path = os.path.join(os.path.dirname(__file__), "..", "data_lake", "raw", filename)

    # Lê o arquivo CSV em um DataFrame do pandas
    df = pd.read_csv(file_path, sep = ',', encoding = 'windows-1252')
    return df

def import_parquet_to_dataframe(filename):
    '''
    Importa um arquivo parquet para um Dataframe do Pandas

    Args:
    file_path (str): O caminho completo do arquivo CSV.
    
    Returns:
    Um DataFrame do pandas com os dados do arquivo parquet.
    '''
    
    # Obtém o caminho completo do arquivo CSV
    file_path = os.path.join(os.path.dirname(__file__), ".", "data_lake", "trusted", filename)

    # Lê o arquivo CSV em um DataFrame do pandas
    df = pd.read_parquet(file_path) #sep = ',', encoding = 'windows-1252')
    return df


# Função para renomear colunas
def rename_columns(df, new_col_names):
    """
    Substitui os nomes das colunas do dataframe pelos nomes inseridos pelo usuário.

    Args:
    - df: dataframe pandas
    - new_col_names: lista com os novos nomes das colunas

    Returns:
    - dataframe pandas com as colunas renomeadas
    """
    # Verifica se o número de novos nomes de colunas é igual ao número de colunas do dataframe
    if len(new_col_names) != len(df.columns):
        print('O número de novos nomes de colunas não corresponde ao número de colunas do dataframe.')
        return None
    # Renomeia as colunas
    df.columns = new_col_names
    return df


# Função para converter colunas para data
def converter_para_datetime(df, colunas):
    """
    Converte as colunas especificadas do dataframe para o tipo datetime.

    Args:
        df (pandas.DataFrame): O dataframe que será modificado.
        cols (list): Uma lista com os nomes das colunas a serem convertidas.

    Returns:
        pandas.DataFrame: O dataframe com as colunas convertidas para datetime.
    """
    for coluna in colunas:
        if df[coluna].dtype == 'object':
            df[coluna] = pd.to_datetime(df[coluna], infer_datetime_format=True)
    return df

#Função para converter colunas para inteiro
def convert_to_int(df, cols):
    """
    Converte as colunas especificadas do dataframe para o tipo inteiro.

    Args:
        df (pandas.DataFrame): O dataframe que será modificado.
        cols (list): Uma lista com os nomes das colunas a serem convertidas.

    Returns:
        pandas.DataFrame: O dataframe com as colunas convertidas para inteiro.
    """
    for col in cols:
        # Utiliza a função astype() para converter a coluna para o tipo inteiro.
        df[col] = df[col].astype('Int64')
    return df

#Função para converter colunas para float
def convert_to_float(df, cols):
    """
    Converte as colunas especificadas do dataframe para o tipo float.

    Args:
        df (pandas.DataFrame): O dataframe que será modificado.
        cols (list): Uma lista com os nomes das colunas a serem convertidas.

    Returns:
        pandas.DataFrame: O dataframe com as colunas convertidas para float.
    """
    for col in cols:
        # Utiliza a função astype() para converter a coluna para o tipo float.
        df[col] = df[col].astype(float)
    return df

