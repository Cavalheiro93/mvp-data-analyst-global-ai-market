# Essa classe foi criada usando POO e Clean Code para facilitar o uso e reutilização.
# O professor pode executar normalmente sem necessidade de importar arquivos externos.
import pandas as pd
import time

class LimpezaDados:
    def __init__(self, caminho_arquivo: str):
        self.caminho_arquivo = caminho_arquivo
        self.df = None

    def carregar_dados(self):
        """carregar_dados _summary_

        _extended_summary_
        """
        print("📥 Lendo o arquivo...")
        self.df = pd.read_csv(self.caminho_arquivo, sep=',', encoding='latin1')
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

    def conversao_para_data(self):
        print("🏗️ Convertendo tipos...")
        self.df['posting_date'] = pd.to_datetime(self.df['posting_date'])
        self.df['application_deadline'] = pd.to_datetime(self.df['application_deadline'])
        time.sleep(0.5)

    def executar_pipeline(self):
        """
        Executa o pipeline completo de limpeza dos dados.

        Returns:
            pd.DataFrame: DataFrame tratado e pronto para análise.
        """
        self.carregar_dados()
        self.tratar_valores_nulos()
        self.remover_duplicatas()
        print("🏁 Fim do processo! DataFrame disponível.")
        return self.df

