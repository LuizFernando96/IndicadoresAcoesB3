import pandas as pd
import os

#Diretório onde os arquivos CSV estão localizados
diretorio_arquivos_csv = 'E:\\Luiz\\ETL\\JamesVirtual\\Arquivos_CSV'

#Diretório onde o arquivo combinado será salvo
diretorio_saida = 'E:\\Luiz\\ETL\\JamesVirtual\\ArquivosMaster'

#Verifica se o diretório de saída existe e, se não existir, cria-o
if not os.path.exists(diretorio_saida):
    os.makedirs(diretorio_saida)

#Lista para armazenar os DataFrames de cada arquivo CSV
dataframes = []

#Nomes dos arquivos CSV a serem combinados
arquivos_csv = ['2019.csv', '2020.csv', '2021.csv', '2022.csv', '2023.csv']

#Lê cada arquivo CSV e o adiciona à lista de DataFrames
for arquivo in arquivos_csv:
    caminho_arquivo = os.path.join(diretorio_arquivos_csv, arquivo)
    df = pd.read_csv(caminho_arquivo)
    dataframes.append(df)

#Combina os DataFrames em um único DataFrame
df_combinado = pd.concat(dataframes, ignore_index=True)

# Salva o DataFrame combinado como um arquivo CSV no diretório de saída
nome_arquivo_saida = os.path.join(diretorio_saida, '10105.csv')
df_combinado.to_csv(nome_arquivo_saida, index=False)

