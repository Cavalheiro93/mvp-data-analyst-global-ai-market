# Essa classe foi criada usando POO e Clean Code para facilitar o uso e reutilização.
# O professor pode executar normalmente sem necessidade de importar arquivos externos.
import pandas as pd
import time

class LimpezaDados:
    def __init__(self, caminho_arquivo: str):
        self.caminho_arquivo = caminho_arquivo
        self.df = None

    def carregar_dados(self):
        self.df = pd.read_csv(self.caminho_arquivo, sep=',', encoding='latin1')
        print("🔹 Arquivo carregado!")
        time.sleep(0.5)

    def _tratar_valores_nulos(self):
        self.df.fillna("Não Informado", inplace=True)
        print("🔹 Valores nulos tratados")        
        time.sleep(0.5)

    def _remover_duplicatas(self):
        self.df.drop_duplicates(inplace=True)
        print("🔹 Duplicatas removidas")
        time.sleep(0.5)

    def _conversao_para_data(self):
        self.df['posting_date'] = pd.to_datetime(self.df['posting_date'])
        self.df['application_deadline'] = pd.to_datetime(self.df['application_deadline'])
        print("🔹 Datas convertidas para datetime")
        time.sleep(0.5)
    
    def _remote_ratio_categorizado(self):
        self.df['remote_ratio'] = self.df['remote_ratio'].map({
            0: '1-On-site',
            50: '2-Hybrid',
            100: '3-Remote'
            }).astype('category')
        print("🔹 Coluna 'experience_level' categorizado")
        time.sleep(0.5)       

    def _experience_level_categorizado(self):
        self.df['experience_level'] = self.df['experience_level'].map({
            'EN': '1- Entry-level',
            'MI': '2-Mid-level',
            'SE': '3-Senior',
            'EX': '4-Executive'
            }).astype('category')
        print("🔹 Coluna 'experience_level' categorizado")
        time.sleep(0.5)  

    def _employment_type_categorizado(self):
        # Substitui os valores e já converte para tipo categórico
        self.df['employment_type'] = self.df['employment_type'].map({
            'FT': 'Full-time',
            'PT': 'Part-time',
            'CT': 'Contract',
            'FL': 'Freelance'
        }).astype('category')    
        print("🔹 Coluna 'employment_type' categorizado")
        time.sleep(0.5)        

    def _company_size_categorizado(self):
        # Substitui os valores e já converte para tipo categórico
        self.df['company_size'] = self.df['company_size'].map({
            'S': '1-Small [<50]',
            'M': '2-Medium [50-250]',
            'L': '3-Large [>250]'
        }).astype('category')
        print("🔹 Coluna 'company_size' categorizado")
        time.sleep(0.5)                         

    def executar_pipeline(self):
        """
        Executa o pipeline completo de limpeza dos dados.

        Returns:
            pd.DataFrame: DataFrame tratado e pronto para análise.
        """
        self.carregar_dados()
        self._tratar_valores_nulos()
        self._remover_duplicatas()
        self._conversao_para_data()
        self._remote_ratio_categorizado()
        self._experience_level_categorizado()
        self._employment_type_categorizado()
        self._company_size_categorizado()
        print("✅ Fim do processo! DataFrame disponível. ✅")
        print("Segunda Tentativa")
        return self.df

