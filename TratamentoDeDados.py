from abc import ABC, abstractmethod
import pandas as pd
import time


class TratamentoBase(ABC):
    """
    Classe base abstrata para tratamentos de dados.
    Todas as subclasses devem implementar o método 'executar'.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df

    @abstractmethod
    def executar(self) -> pd.DataFrame:
        pass


class LimpezaInicialDados(TratamentoBase):
    """
    Realiza a limpeza inicial dos dados: valores nulos, duplicatas, conversão de datas e categorização de colunas.
    """

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
        print("🔹 Coluna 'remote_ratio' categorizado")
        time.sleep(0.5)

    def _experience_level_categorizado(self):
        self.df['experience_level'] = self.df['experience_level'].map({
            'EN': '1-Entry-level',
            'MI': '2-Mid-level',
            'SE': '3-Senior',
            'EX': '4-Executive'
        }).astype('category')
        print("🔹 Coluna 'experience_level' categorizado")
        time.sleep(0.5)

    def _employment_type_categorizado(self):
        self.df['employment_type'] = self.df['employment_type'].map({
            'FT': 'Full-time',
            'PT': 'Part-time',
            'CT': 'Contract',
            'FL': 'Freelance'
        }).astype('category')
        print("🔹 Coluna 'employment_type' categorizado")
        time.sleep(0.5)

    def _company_size_categorizado(self):
        self.df['company_size'] = self.df['company_size'].map({
            'S': '1-Small [<50]',
            'M': '2-Medium [50-250]',
            'L': '3-Large [>250]'
        }).astype('category')
        print("🔹 Coluna 'company_size' categorizado")
        time.sleep(0.5)

    def executar(self) -> pd.DataFrame:
        """
        Executa o pipeline completo de limpeza inicial.
        """
        print("📥 Iniciando limpeza inicial...")
        self._tratar_valores_nulos()
        self._remover_duplicatas()
        self._conversao_para_data()
        self._remote_ratio_categorizado()
        self._experience_level_categorizado()
        self._employment_type_categorizado()
        self._company_size_categorizado()
        print("✅ Fim da limpeza inicial. DataFrame disponível.")
        return self.df
