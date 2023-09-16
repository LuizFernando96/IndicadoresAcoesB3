import subprocess

#Execute o arquivo Download.py
download_process = subprocess.Popen(['python', 'Download.py'])

#Aguarde o término do processo de download
download_process.wait()

#Verifique se o processo de download foi bem-sucedido (código de retorno 0)
if download_process.returncode == 0:
    print("Download concluído.")
    
    #Execute o arquivo TXTtoCSV.py
    txt_to_csv_process = subprocess.Popen(['python', 'TXTtoCSV.py'])

    #Aguarde o término do processo de conversão
    txt_to_csv_process.wait()

    #Verifique se o processo de conversão foi bem-sucedido (código de retorno 0)
    if txt_to_csv_process.returncode == 0:
        print("TXT para CSV concluído.")
        
        #Execute o arquivo BigCSV.py
        big_csv_process = subprocess.Popen(['python', 'BigCSV.py'])

        #Aguarde o término do processo de criação do arquivo grande
        big_csv_process.wait()

        #Verifique se o processo de criação do arquivo grande foi bem-sucedido (código de retorno 0)
        if big_csv_process.returncode == 0:
            print("Arquivo grande concluído.")
        else:
            print("Erro durante o processo de criação do arquivo grande.")
    else:
        print("Erro durante o processo de conversão de TXT para CSV.")
else:
    print("Erro durante o processo de download.")
