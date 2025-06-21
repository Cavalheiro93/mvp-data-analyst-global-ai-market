from abc import ABC, abstractmethod
import pandas as pd
import time
import seaborn as sns
import matplotlib.pyplot as plt
import ipywidgets as widgets
from IPython.display import display


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

    def _startmonth_new_column(self):
        # Criar a coluna Start_Month com o primeiro dia do mês
        self.df['Start_Month'] = self.df['posting_date'].dt.to_period('M').dt.to_timestamp()

        # Se quiser só como string 'YYYY-MM'
        self.df['Start_Month'] = self.df['posting_date'].dt.strftime('%Y-%m')     

        print("🔹 Coluna 'Start_Month' criada")   

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
        self._startmonth_new_column()
        self._remote_ratio_categorizado()
        self._experience_level_categorizado()
        self._employment_type_categorizado()
        self._company_size_categorizado()
        print("✅ Fim da limpeza inicial. DataFrame disponível.")
        return self.df


class LimpezaFinalDados(TratamentoBase):
    """
    Realiza a limpeza final dos dados: Remove as colunas que não serão utilizadas.
    """

    def __init__(self, df: pd.DataFrame, colunas_remover: list[str]):
        super().__init__(df)
        self.colunas_remover = colunas_remover

    def _drop_colunas(self):
        self.df.drop(columns=self.colunas_remover, inplace=True)
        print(f"🔹 Colunas removidas: {', '.join(self.colunas_remover)}")
        time.sleep(0.5)

    def executar(self) -> pd.DataFrame:
        print("📥 Iniciando limpeza final...")
        self._drop_colunas()
        print("✅ Fim da limpeza final. DataFrame disponível.")
        return self.df


class GeradorSkillsPorCategoria:
    """
    Gera um novo DataFrame com a contagem de habilidades por categoria especificada.
    """

    def __init__(self, df: pd.DataFrame, coluna_categoria: str):
        self.df = df.copy()
        self.coluna_categoria = coluna_categoria
        self.df_resultado = None

    def gerar(self) -> pd.DataFrame:
        """
        Trata a coluna de skills e gera o DataFrame com contagem por categoria.

        Returns:
            pd.DataFrame: DataFrame com colunas ['required_skill', categoria, 'qtd']
        """
        self.df['required_skills_list'] = self.df['required_skills'].astype(str).str.split(',')

        df_skills = self.df[[self.coluna_categoria, 'required_skills_list']].explode('required_skills_list').copy()
        df_skills.rename(columns={'required_skills_list': 'required_skill'}, inplace=True)
        df_skills['required_skill'] = df_skills['required_skill'].str.strip().str.title()

        self.df_resultado = (
            df_skills
            .groupby(['required_skill', self.coluna_categoria])
            .size()
            .reset_index(name='qtd')
        )

        print(f"✅ DataFrame de skills por '{self.coluna_categoria}' gerado com sucesso.")
        return self.df_resultado

    def plotar_top_habilidades_interativo(self, top_n: int = 10, categoria_especifica: str = None):
        """
        Exibe um gráfico (interativo ou direto) das top N habilidades por categoria.

        Args:
            top_n (int): Número de habilidades a exibir.
            categoria_especifica (str, opcional): Categoria específica para plotar diretamente.
        """
        if self.df_resultado is None:
            print("⚠️ Você precisa chamar o método 'gerar()' antes de visualizar o gráfico.")
            return

        def plotar(categoria_selecionada):
            df_filtrado = self.df_resultado[self.df_resultado[self.coluna_categoria] == categoria_selecionada]
            df_top = df_filtrado.nlargest(top_n, 'qtd')

            plt.figure(figsize=(10, 6))
            sns.barplot(data=df_top, x='qtd', y='required_skill', palette='viridis')
            plt.title(f'Top {top_n} Habilidades - {categoria_selecionada}')
            plt.xlabel('Número de Vagas')
            plt.ylabel('Habilidade')
            plt.grid(axis='x', linestyle='--', alpha=0.5)

            # Adiciona os rótulos ao final de cada barra
            for i, valor in enumerate(df_top['qtd']):
                plt.text(
                    x=valor + 1,
                    y=i,
                    s=str(valor),
                    va='center'
                )

            plt.tight_layout()
            plt.show()

        # Se o usuário forneceu uma categoria específica, plota direto
        if categoria_especifica:
            if categoria_especifica not in self.df_resultado[self.coluna_categoria].unique():
                print(f"❌ Categoria '{categoria_especifica}' não encontrada na coluna '{self.coluna_categoria}'.")
                return
            plotar(categoria_especifica)

        else:
            # Caso contrário, cria um dropdown interativo
            categorias = sorted(self.df_resultado[self.coluna_categoria].unique())

            seletor = widgets.Dropdown(
                options=categorias,
                description=f'{self.coluna_categoria}:',
                style={'description_width': 'initial'},
                layout=widgets.Layout(width='50%')
            )

            widgets.interact(plotar, categoria_selecionada=seletor)

