import requests
import zipfile
import os
import pandas as pd

#Criando a pasta que guardará os arquivos
diretorio_destino = 'E:\\Luiz\\ETL\\JamesVirtual\\Arquivos'
if not os.path.exists(diretorio_destino):
    os.makedirs(diretorio_destino)
print("Pasta criada")
#Criando loop para que todos os arquivos sejam baixados de forma automática
Ano = 2018
while Ano <= 2022:
    print("Iniciando procedimento no ano "+str(Ano)+"...")  
    #URL do arquivo ZIP para baixar baixar
    url = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A" + str(Ano) + ".ZIP"

    #Nome do arquivo ZIP local
    nome_arquivo = os.path.join(diretorio_destino, str(Ano) + ".zip")

    #Download do arquivo ZIP
    print("Baixando arquivo do ano " + str(Ano) +"...")
    response = requests.get(url)

    #Verificando se o download deu certo
    if response.status_code == 200:
        #Salve o arquivo ZIP
        with open(nome_arquivo, 'wb') as file:
            file.write(response.content)
            print("Arquivo do ano " + str(Ano) +" baixado")
        #Extraindo arquivo
        print("Extraindo arquivo do ano "+str(Ano)+"...")
        with zipfile.ZipFile(nome_arquivo, 'r') as zip_ref:
            zip_ref.extractall(diretorio_destino)
            print("Arquivo do ano "+str(Ano)+" extraído")
    
    #Selecionando o arquivo TXT
    f_bovespa = 'E:\\Luiz\\ETL\\JamesVirtual\\Arquivos\\' + 'COTAHIST_A' + str(Ano) + '.txt'
    print("Iniciando tratamento do arquivo do ano "+str(Ano)+"...")
    #Definindo os tamnhaos 
    tamanho_campos=[2,8,2,12,3,12,10,3,4,13,13,13,13,13,13,13,5,18,18,13,1,8,7,13,12,3]

    #Lendo como um "Fixed Width File" ou "Arquivo com Largura Fixa" e ignorando a primeira linha
    dados_acoes = pd.read_fwf(f_bovespa, widths=tamanho_campos, header=0)

    #Nomeando as colunas
    dados_acoes.columns = ["tipo_registro","data_pregao","cod_bdi","cod_negociacao","tipo_mercado","nome_empresa","especificacao_papel","prazo_dias_merc_termo","moeda_referencia","preco_abertura","preco_maximo","preco_minimo","preco_medio","preco_ultimo_negocio","preco_melhor_oferta_compra","preco_melhor_oferta_venda","numero_negocios","quantidade_papeis_negociados","volume_total_negociado","preco_exercicio","indicador_correcao_precos","data_vencimento","fator_cotacao","preco_exercicio_pontos","codigo_isin","num_distribuicao_papel"]

    #Eliminando a ultima linha do arquivo
    linha=len(dados_acoes["data_pregao"])
    dados_acoes=dados_acoes.drop(linha-1)

    #Ajustando valores com vírgula
    listaVirgula=["preco_abertura","preco_maximo","preco_minimo","preco_medio","preco_ultimo_negocio","preco_melhor_oferta_compra","preco_melhor_oferta_venda","volume_total_negociado","preco_exercicio","preco_exercicio_pontos"]
   
    for coluna in listaVirgula:
       dados_acoes[coluna]=[i/100. for i in dados_acoes[coluna]]
       
    #Alterando o formato da data
    dados_acoes['data_pregao'] = pd.to_datetime(dados_acoes.data_pregao)
    dados_acoes['data_pregao'] = dados_acoes['data_pregao'].dt.strftime('%d/%m/%Y')
    print("Arquivo do ano"+str(Ano)+" tratado")
    print("Filtrando arquivo...")
    #Filtrando as ações
    acoes_interesse = ["B3SA3", "CRFB3", "RECV3", "B3SA3", "MRVE3", "BRKM5", "ITUB4", "ABEV3", "UGPA3", "ELET3"]

    df = dados_acoes[dados_acoes['cod_negociacao'].isin(acoes_interesse)]

    nome_arquivo = str(Ano) + '.csv'

    #Criando o caminho completo para o arquivo no diretório de destino
    caminho_destino = os.path.join(diretorio_destino, nome_arquivo)
    print("Transformando arquivo " + nome_arquivo +" de TXT para CSV...")
    #Criando o arquivo CSV no diretório de destino
    with open(caminho_destino, "w") as fileO:
        fileO.write(df.to_csv(index=False))    
        print("Arquivo " + nome_arquivo +" transformado de TXT para CSV\n")
    Ano += 1
print("Iniciando criação do CSV único...")

#Diretório onde os arquivos CSV estão localizados
diretorio_arquivos = 'E:\\Luiz\\ETL\\JamesVirtual\\Arquivos'

#Diretório onde o arquivo combinado será salvo
diretorio_saida = 'E:\\Luiz\\ETL\\JamesVirtual\\Arquivos'

#Lista para armazenar os DataFrames de cada arquivo CSV
dataframes = []

#Nomes dos arquivos CSV a serem combinados
arquivos_csv = ['2018.csv', '2019.csv', '2020.csv', '2021.csv', '2022.csv']

#Lê cada arquivo CSV e o adiciona à lista de DataFrames
for arquivo in arquivos_csv:
    caminho_arquivo = os.path.join(diretorio_arquivos, arquivo)
    df = pd.read_csv(caminho_arquivo)
    dataframes.append(df)

#Combina os DataFrames em um único DataFrame
df_combinado = pd.concat(dataframes, ignore_index=True)

# Salva o DataFrame combinado como um arquivo CSV no diretório de saída
nome_arquivo_saida = os.path.join(diretorio_saida, '10100.csv')
df_combinado.to_csv(nome_arquivo_saida, index=False)

print("CSV único criado\nFIM")

