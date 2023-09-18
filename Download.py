import requests
import zipfile
import os

#Criando a pasta que guardará os arquivos TXT
diretorio_destino = 'E:\\Luiz\\ETL\\JamesVirtual\\Arquivos_TXT'
if not os.path.exists(diretorio_destino):
    os.makedirs(diretorio_destino)

#Criando loop para que todos os arquivos sejam baixados de forma automática
Ano = 2018
while Ano <= 2022:
    
    #URL do arquivo ZIP que você deseja baixar
    url = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A" + str(Ano) + ".ZIP"

    #Nome do arquivo ZIP local (com base no nome do arquivo na URL)
    nome_arquivo_zip = os.path.join(diretorio_destino, str(Ano) + ".zip")

    #Faça o download do arquivo ZIP
    response = requests.get(url)

    #Verifique se o download foi bem-sucedido (código de status HTTP 200)
    if response.status_code == 200:
        #Salve o arquivo ZIP
        with open(nome_arquivo_zip, 'wb') as file:
            file.write(response.content)

        #Extraindo arquivo
        with zipfile.ZipFile(nome_arquivo_zip, 'r') as zip_ref:
            zip_ref.extractall(diretorio_destino)
    Ano += 1
