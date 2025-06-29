from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
import re
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


class PadronizacaoDadosBrasil(TratamentoBase):
    """
    Classe para padronizar os dados do Brasil de acordo com o schema do DataFrame global.
    Inclui ajuste de job_title, experience_level, employment_type, company_location,
    industry e required_skills.
    """

    def _ajustar_experience_level(self):
        """
        Ajusta a coluna 'xp_level' com base em valores explícitos e inferência pelo job_title.
        """
        dict_experience_from_title = {
            'SE': ['Sênior', 'Senior', 'Lead', 'Dados III', 'Especialista', 'SR', 'specialist'],   
            'MI': ['Pleno', 'Mid', 'Intermediate', 'Dados II', 'PL'],     
            'EN': ['Junior', 'Júnior', 'Estágio', 'Intern', 'Dados I', 'Jr'],
            'EX': ['Diretor', 'Executive', 'Head', 'VP', 'Chief']
        }
        dict_experience_from_xp_level = {
            'Assistente': 'EN',
            'Júnior': 'EN',
            'Estágio': 'EN',
            'Pleno-sênior': 'MI',
            'Diretor': 'EX',
            'Executivo': 'EX',
            'Não aplicável': 'Não aplicável'
        }

        def ajustar_xp_direct(xp_level):
            if xp_level != 'Não aplicável':
                return dict_experience_from_xp_level.get(xp_level, xp_level)
            return xp_level

        self.df['xp_level'] = self.df['xp_level'].apply(ajustar_xp_direct)

        def corrigir_xp_na(row):
            if row['xp_level'] == 'Não aplicável':
                title = row['job_title'].lower()
                for level, keywords in dict_experience_from_title.items():
                    for kw in keywords:
                        if kw.lower() in title:
                            return level
            return row['xp_level']

        self.df['xp_level'] = self.df.apply(corrigir_xp_na, axis=1)
        print("🔹 Coluna 'xp_level' ajustada com base nos valores explícitos e no job_title.")  

    def _remover_colunas_irrelevantes(self):
        """
        Remove colunas que não existem ou não são relevantes no padrão global.
        """
        colunas_para_remover = ['job_title', 'time_posted', 'num_applicants']
        self.df.drop(columns=colunas_para_remover, inplace=True, errors='ignore')
        print(f"🔹 Colunas removidas: {', '.join(colunas_para_remover)}")

    def _renomear_colunas(self):
        """
        Renomeia colunas para padronizar com o schema do DataFrame global.
        """
        colunas_renomear = {
            'keyword': 'job_title',
            'xp_level': 'experience_level',
            'job_type': 'employment_type',
            'location': 'company_location',
            'work_model': 'remote_ratio',
            'job_sectors': 'industry',
            'scrape_date': 'posting_date'
        }
        self.df.rename(columns=colunas_renomear, inplace=True)
        print("🔹 Colunas renomeadas para padrão global.")

    def _padronizar_job_title(self):
        """
        Padroniza a coluna 'job_title' com base em mapeamento de keywords para o padrão global.
        """
        map_keyword_to_jobtitle = {
            'Analista de Dados': 'Data Analyst',
            'Cientista de Dados': 'Data Scientist',
            'Engenheiro de Dados': 'Data Engineer',
            'Data Analyst': 'Data Analyst',
            'Data Scientist': 'Data Scientist',
            'Data Engineer': 'Data Engineer',
            'Analista de BI': 'Business Intelligence Analyst',
            'Machine Learning Engineer': 'Machine Learning Engineer',
            'Engenheiro de Machine Learning': 'Machine Learning Engineer',
            'Engenheiro de IA': 'AI Engineer',
            'AI Engineer': 'AI Engineer',
            'Business Intelligence Analyst': 'Business Intelligence Analyst'
        }

        self.df['job_title'] = self.df['job_title'].map(map_keyword_to_jobtitle)
        print("🔹 Coluna 'job_title' padronizada para o padrão global.")

    def _padronizar_employment_type(self):
        """
        Padroniza a coluna 'employment_type' com base em mapeamento de jobtype para o padrão global.
        """        
        map_jobtype_to_employment = {
        'Tempo integral': 'FT',
        'Contrato': 'CT',
        'Temporário': 'CT',
        'Meio período': 'PT',
        'Estágio': 'PT',
        'Voluntário': 'FL',
        'Outro': 'FL'
    }

        self.df['employment_type'] = self.df['employment_type'].map(map_jobtype_to_employment) 
        print("🔹 Coluna 'employment_type' padronizada para o padrão global.")       

    def _padronizar_company_only_brazil(self):
        """
        Padroniza a coluna 'company_location' para o padrão de 'Brasil' apenas.
        """
        self.df['company_location'] = 'Brasil'
        print("🔹 Coluna 'company_location' padronizada para 'Brasil' apenas.")

    def _ajustar_remote_ratio(self):
        """
        Ajusta a coluna 'remote_ratio' com base no modelo de trabalho para os valores padrões globais.
        """
        map_jobtype_to_remote = {
            'Presencial': 0,
            'Híbrido': 50,
            'Remoto': 100
        }

        self.df['remote_ratio'] = self.df['remote_ratio'].map(map_jobtype_to_remote)
        print("🔹 Coluna 'remote_ratio' ajustada para valores padrão global (0, 50, 100).") 

    def _ajustar_industry(self):
        """
        Ajusta a coluna 'industry' mapeando para categorias globais com base em palavras-chave.
        """
        dict_industry_keywords = {
            'Automotive': ['autos', 'veículo', 'carro', 'automotiva', 'automotores'],
            'Media': ['mídia', 'comunicação', 'publicidade', 'Entretenimento'],
            'Education': ['escola', 'educação', 'universidade', 'ensino', 'Treinamento', 'Gestão educacional', 'Pesquisa', 'Pesquisas'],
            'Consulting': ['consultoria', 'assessoria'],
            'Healthcare': ['saúde', 'hospital', 'clínica'],
            'Gaming': ['jogo', 'game', 'gaming'],
            'Government': ['governo', 'prefeitura', 'estado', 'Administração pública', 'ordem pública'],
            'Telecommunications': ['telecom', 'telefonia', 'comunicação'],
            'Manufacturing': ['fábrica', 'indústria', 'produção', 'fabricação'],
            'Energy': ['energia', 'petroleo', 'gás'],
            'Technology': ['tecnologia', 'ti', 'software', 'Atividades dos serviços de tecnologia da informação', 'Plataformas de'],
            'Real Estate': ['imobiliária', 'construção', 'incorporadora'],
            'Finance': ['banco', 'financeira', 'seguro', 'Atividades de serviços financeiros', 'Mercados de capital'],
            'Transportation': ['transporte', 'logística', 'frota'],
            'Retail': ['varejo', 'loja', 'comércio', 'Bens de consumo', 'Alimentos e bebidas'],
            'Services': ['Serviços', 'Serviço'],
            'Human Resource': ['recursos humanos', 'Recrutamento e seleção de pessoal'],
            'Agriculture': ['Agricultura', 'Criação de bovinos']
        }

        def map_industry(br_industry):
            br_industry_lower = str(br_industry).lower()
            for global_industry, keywords in dict_industry_keywords.items():
                for kw in keywords:
                    if kw.lower() in br_industry_lower:
                        return global_industry
            return 'Others'  # se não encontrar, classifica como "Outros"

        self.df['industry'] = self.df['industry'].apply(map_industry)
        print("🔹 Coluna 'industry' ajustada e mapeada para categorias globais.")

    def _extrair_required_skills(self):
        """
        Extrai e padroniza as habilidades mencionadas na coluna 'job_description',
        salvando em 'required_skills' no formato do DataFrame global.
        """
        dict_skills = {
            'Aws': ['Aws'],
            'Azure': ['Azure'],
            'Computer Vision': ['Computer Vision', 'Visão Computacional'],
            'Data Visualization': ['Data Visualization', 'Dashboard', 'Visualização de Dados'],
            'Deep Learning': ['Deep Learning', 'Aprendizagem profunda'],
            'Docker': ['Docker'],
            'Gcp': ['Gcp', 'Google Cloud', 'Google Cloud Plataform'],
            'Git': ['Git', 'Github'],
            'Hadoop': ['Hadoop'],
            'Java': ['Java'],
            'Kubernetes': ['Kubernetes'],
            'Linux': ['Linux'],
            'Mathematics': ['Mathematics', 'Matematica', 'Matemática', 'reas correlatas'],
            'Mlops': ['Mlops'],
            'Nlp': ['Nlp'],
            'Python': ['Python', 'Rython', 'ython'],
            'Pytorch': ['Pytorch'],
            'R': ['Linguagem R'],
            'Scala': ['Scala'],
            'Spark': ['Spark', 'Pyspark'],
            'Sql': ['Sql'],
            'Statistics': ['Statistics', 'Estatistica', 'Estatística', 'reas correlatas'],
            'Tableau': ['Tableau'],
            'Tensorflow': ['Tensorflow']
        }

        def encontrar_skills(texto):
            skills_encontradas = []
            texto_lower = str(texto).lower()
            for skill_padrao, variacoes in dict_skills.items():
                for variacao in variacoes:
                    if skill_padrao == 'R':
                        if re.search(r'\b[rR]\b', texto):
                            skills_encontradas.append(skill_padrao)
                            break
                    else:
                        if variacao.lower() in texto_lower:
                            skills_encontradas.append(skill_padrao)
                            break
            return ', '.join(sorted(set(skills_encontradas)))

        self.df['required_skills'] = self.df['job_description'].apply(encontrar_skills)
        print("🔹 Coluna 'required_skills' extraída e padronizada a partir do job_description.")

    def _calcular_job_description_length(self):
        """
        Calcula o comprimento (número de caracteres) de cada descrição de vaga,
        e substitui a coluna 'job_description' pelo seu comprimento.
        """
        self.df['job_description_length'] = self.df['job_description'].apply(lambda x: len(str(x)))
        self.df.drop(columns=['job_description'], inplace=True)
        print("🔹 Coluna 'job_description' atualizada com o comprimento dos textos.")

    def _ajustar_posting_date(self):
        self.df['posting_date'] = pd.to_datetime(self.df['posting_date'], dayfirst=True, errors='coerce')
        self.df['posting_date'] = self.df['posting_date'].dt.strftime('%Y-%m-%d')


    def _atribuir_valores_vazios(self):

        # DataFrame já criado ou carregado em df
        self.df['salary_usd'] = np.nan
        self.df['salary_currency'] = "N/A"
        self.df['company_size'] = "N/A"
        self.df['employee_residence'] = "N/A"
        self.df['education_required'] = "N/A"
        self.df['years_experience'] = np.nan
        self.df['application_deadline'] = np.nan
        self.df['benefits_score'] = np.nan
        self.df['posting_date'] = np.nan
        print("🔹 Ajuste de valores vazios.")

    def _ajustar_dtypes(self):
        """
        Ajusta os tipos de dados das colunas do DataFrame do Brasil para que
        fiquem compatíveis com o schema do DataFrame global.
        """
        tipos_alvo = {
            'job_id': 'object',
            'job_title': 'object',
            'salary_usd': 'Int64',  # pandas usa 'Int64' para int com suporte a NaN
            'salary_currency': 'object',
            'experience_level': 'object',
            'employment_type': 'object',
            'company_location': 'object',
            'company_size': 'object',
            'employee_residence': 'object',
            'remote_ratio': 'Int64',
            'required_skills': 'object',
            'education_required': 'object',
            'years_experience': 'Int64',
            'industry': 'object',
            'posting_date': 'object',
            'application_deadline': 'object',
            'job_description_length': 'Int64',
            'benefits_score': 'float64',
            'company_name': 'object'
        }

        for coluna, tipo in tipos_alvo.items():
            if coluna in self.df.columns:
                self.df[coluna] = self.df[coluna].astype(tipo, errors='ignore')

        print(f"🔹 Tipos de Colunas padronizadas.")


    def executar(self) -> pd.DataFrame:
        """
        Executa o pipeline completo de padronização dos dados do Brasil.
        """
        print("📥 Iniciando padronização dos dados do Brasil...")
        self._ajustar_experience_level()
        self._remover_colunas_irrelevantes()
        self._renomear_colunas()
        self._padronizar_job_title()
        self._padronizar_employment_type()
        self._padronizar_company_only_brazil()
        self._ajustar_remote_ratio()
        self._ajustar_industry()
        self._extrair_required_skills()
        self._calcular_job_description_length()
        self._atribuir_valores_vazios()
        self._ajustar_posting_date()
        self._ajustar_dtypes()
        print("✅ Fim da padronização. DataFrame disponível.")
        return self.df


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
        # self._tratar_valores_nulos()
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

