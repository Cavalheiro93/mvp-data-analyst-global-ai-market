# Essa classe foi criada usando POO e Clean Code para facilitar o uso e reutilização.
# O professor pode executar normalmente sem necessidade de importar arquivos externos.
import pandas as pd
import time

class LimpezaDados:
    def __init__(self, caminho_arquivo: str):
        self.caminho_arquivo = caminho_arquivo
        self.df = None

    def carregar_dados(self):
        
        print("📥 Lendo o arquivo...")
        self.df = pd.read_csv(self.caminho_arquivo)
        print("✅ Arquivo carregado!")
        time.sleep(0.5)

    def tratar_valores_nulos(self):
        print("🧹 Tratando valores nulos...")
        self.df.fillna("Não Informado", inplace=True)
        time.sleep(0.5)

    def remover_duplicatas(self):
        print("🧽 Removendo duplicatas...")
        self.df.drop_duplicates(inplace=True)
        time.sleep(0.5)

    def executar_pipeline(self):
        self.carregar_dados()
        self.tratar_valores_nulos()
        self.remover_duplicatas()
        print("🏁 Fim do processo! DataFrame disponível.")
        return self.df
