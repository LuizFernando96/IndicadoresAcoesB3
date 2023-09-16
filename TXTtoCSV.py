import pandas as pd
import os

#Criando a pasta que guardará os arquivos CSV's
diretorio_destino = 'E:\\Luiz\\ETL\\JamesVirtual\\Arquivos_CSV'
if not os.path.exists(diretorio_destino):
    os.makedirs(diretorio_destino)

Ano = 2019
while Ano <= 2023:
    #Seleciona o arquivo TXT (Selecionar de acordo com o local onde o arquivo se encontra)
    f_bovespa = 'E:\\Luiz\\ETL\\JamesVirtual\\Arquivos_TXT\\' + 'COTAHIST_A' + str(Ano) + '.txt'

    #Define o tamanho dos campos de acordo com o layout do arquivo da B3  
    tamanho_campos=[2,8,2,12,3,12,10,3,4,13,13,13,13,13,13,13,5,18,18,13,1,8,7,13,12,3]

    #Lê como um "Fixed Width File" ou "Arquivo com Largura Fixa" e ignora a primeira linha com o 'header=0'
    dados_acoes = pd.read_fwf(f_bovespa, widths=tamanho_campos, header=0)

    #Nomea as colunas de acordo com o layout do arquivo da B3
    dados_acoes.columns = ["tipo_registro","data_pregao","cod_bdi","cod_negociacao","tipo_mercado","nome_empresa","especificacao_papel","prazo_dias_merc_termo","moeda_referencia","preco_abertura","preco_maximo","preco_minimo","preco_medio","preco_ultimo_negocio","preco_melhor_oferta_compra","preco_melhor_oferta_venda","numero_negocios","quantidade_papeis_negociados","volume_total_negociado","preco_exercicio","indicador_correcao_precos","data_vencimento","fator_cotacao","preco_exercicio_pontos","codigo_isin","num_distribuicao_papel"]

    #Elimina a ultima linha do arquivo (o Trailer segundo o layout do arquivo da b3)
    linha=len(dados_acoes["data_pregao"])
    dados_acoes=dados_acoes.drop(linha-1)

    #Ajusta valores com vírgula (dividir os valores dessas colunas por 100)
    listaVirgula=["preco_abertura","preco_maximo","preco_minimo","preco_medio","preco_ultimo_negocio","preco_melhor_oferta_compra","preco_melhor_oferta_venda","volume_total_negociado","preco_exercicio","preco_exercicio_pontos"]
   
    for coluna in listaVirgula:
       dados_acoes[coluna]=[i/100. for i in dados_acoes[coluna]]
       
    #Altera o formato da data de 'AAAAMMDD' para 'DDMMAAAA'
    dados_acoes['data_pregao'] = pd.to_datetime(dados_acoes.data_pregao)
    dados_acoes['data_pregao'] = dados_acoes['data_pregao'].dt.strftime('%d/%m/%Y')

    #Filtra as ações de interesse
    acoes_interesse = ["B3SA3", "CRFB3", "RECV3", "B3SA3", "MRVE3", "BRKM5", "ITUB4", "ABEV3", "UGPA3", "ELET3"]

    df = dados_acoes[dados_acoes['cod_negociacao'].isin(acoes_interesse)]

    nome_arquivo_csv = str(Ano) + '.csv'

    #Crie o caminho completo para o arquivo no diretório de destino
    caminho_destino = os.path.join(diretorio_destino, nome_arquivo_csv)

    #Crie o arquivo CSV no diretório de destino
    with open(caminho_destino, "w") as fileO:
        fileO.write(df.to_csv(index=False))
    Ano += 1
    