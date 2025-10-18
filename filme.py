from datetime import datetime
class GerenciadorFilmes:
    @staticmethod
    def pegar_campos_para_csv():
        campos = [
            'titulo_brasil',
            'titulo_original',
            'ano',
            'direcao',
            'elenco',
            'categoria',
            'tempo_minutos',
            'nacionalidade',
            'idioma',
            'resumo',
            'quando_cadastrou',
            'quem_cadastrou'
        ]
        return campos

    @staticmethod
    def criar_exemplo():
        """Monta um filme de exemplo pra testar o sistema"""
        exemplo = Filme()
        exemplo.titulo_brasil = "Cidade de Deus"
        exemplo.titulo_original = "City of God"
        exemplo.ano = 2002
        exemplo.direcao = "Fernando Meirelles, Kátia Lund"
        exemplo.elenco = "Alexandre Rodrigues, Leandro Firmino, Phellipe Haagensen"
        exemplo.categoria = "Drama, Crime"
        exemplo.tempo_minutos = 130
        exemplo.nacionalidade = "Brasil"
        exemplo.idioma = "Português"
        exemplo.resumo = "Dois jovens seguem caminhos diferentes na violenta Cidade de Deus."
        exemplo.quando_cadastrou = "2024-03-10 14:20:00"
        exemplo.quem_cadastrou = "moderador01"
        return exemplo

class Filme:
    """
  Representa um filme no catálogo do Letterboxd.

    """
    def __init__(self):
        """Prepara um novo filme com todos os campos vazios"""
        self.id = None
        self.titulo_brasil = ""
        self.titulo_original = ""
        self.ano = None
        self.direcao = ""
        self.elenco = ""
        self.categoria = ""
        self.tempo_minutos = 0
        self.nacionalidade = ""
        self.idioma = ""
        self.resumo = ""
        self.quando_cadastrou = None
        self.quem_cadastrou = ""

    def __str__(self):
        """Mostra o filme de forma simples"""
        titulo = self.titulo_brasil or self.titulo_original or "Filme sem título"
        return f"{titulo} ({self.ano})" if self.ano else titulo

    def mostrar_info_completa(self):
        """Monta uma ficha completa do filme"""
        info = []
        info.append(f"TÍTULO: {self.titulo_brasil}")

        if self.titulo_original and self.titulo_original != self.titulo_brasil:
            info.append(f"TÍTULO ORIGINAL: {self.titulo_original}")

        if self.ano:
            info.append(f"ANO: {self.ano}")

        if self.direcao:
            info.append(f"DIREÇÃO: {self.direcao}")

        if self.tempo_minutos > 0:
            horas = self.tempo_minutos // 60
            minutos = self.tempo_minutos % 60
            if horas > 0:
                info.append(f"DURAÇÃO: {horas}h {minutos}min")
            else:
                info.append(f"DURAÇÃO: {minutos}min")

        if self.categoria:
            info.append(f"GÊNERO: {self.categoria}")

        if self.nacionalidade:
            info.append(f"PAÍS: {self.nacionalidade}")

        if self.idioma:
            info.append(f"IDIOMA: {self.idioma}")

        if self.resumo:
            info.append(f"SINOPSE: {self.resumo}")

        return "\n".join(info)

    def converter_para_dicionario(self):
        """Transforma os dados do filme em um dicionário"""
        dados = {
            'titulo_brasil': self.titulo_brasil,
            'titulo_original': self.titulo_original,
            'ano': self.ano,
            'direcao': self.direcao,
            'elenco': self.elenco,
            'categoria': self.categoria,
            'tempo_minutos': self.tempo_minutos,
            'nacionalidade': self.nacionalidade,
            'idioma': self.idioma,
            'resumo': self.resumo,
            'quando_cadastrou': self.quando_cadastrou,
            'quem_cadastrou': self.quem_cadastrou
        }
        return dados

    @classmethod
    def criar_apartir_dict(cls, dados):
        """Cria um filme novo a partir de um dicionário de dados"""
        novo_filme = cls()

        if 'id' in dados:
            novo_filme.id = dados['id']

        if 'titulo_brasil' in dados:
            novo_filme.titulo_brasil = dados['titulo_brasil']

        if 'titulo_original' in dados:
            novo_filme.titulo_original = dados['titulo_original']

        if 'ano' in dados:
            try:
                novo_filme.ano = int(dados['ano'])
            except (ValueError, TypeError):
                novo_filme.ano = None

        if 'direcao' in dados:
            novo_filme.direcao = dados['direcao']

        if 'elenco' in dados:
            novo_filme.elenco = dados['elenco']

        if 'categoria' in dados:
            novo_filme.categoria = dados['categoria']

        if 'tempo_minutos' in dados:
            try:
                novo_filme.tempo_minutos = int(dados['tempo_minutos'])
            except (ValueError, TypeError):
                novo_filme.tempo_minutos = 0

        if 'nacionalidade' in dados:
            novo_filme.nacionalidade = dados['nacionalidade']

        if 'idioma' in dados:
            novo_filme.idioma = dados['idioma']

        if 'resumo' in dados:
            novo_filme.resumo = dados['resumo']

        if 'quando_cadastrou' in dados:
            novo_filme.quando_cadastrou = dados['quando_cadastrou']
        else:
            novo_filme.quando_cadastrou = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if 'quem_cadastrou' in dados:
            novo_filme.quem_cadastrou = dados['quem_cadastrou']

        return novo_filme

    def validar_informacoes(self):
        """Confere se os dados do filme estão corretos"""
        problemas = []

        if not self.titulo_brasil.strip():
            problemas.append("❌ Precisa ter um título em português")

        if not self.ano:
            problemas.append("❌ Ano de lançamento é obrigatório")
        else:
            if self.ano < 1880:
                problemas.append("❌ Ano muito antigo (antes de 1880)")
            if self.ano > datetime.now().year + 2:
                problemas.append("❌ Ano no futuro muito distante")

        if not self.direcao.strip():
            problemas.append("❌ Nome do diretor é obrigatório")

        if not self.categoria.strip():
            problemas.append("❌ Gênero do filme é obrigatório")

        if self.tempo_minutos <= 0:
            problemas.append("❌ Duração precisa ser maior que zero")
        elif self.tempo_minutos > 600:
            problemas.append("❌ Duração muito longa (mais de 10 horas)")

        if not self.nacionalidade.strip():
            problemas.append("❌ País de origem é obrigatório")

        return problemas

    def tem_ator_famoso(self, lista_atores_famosos=None):
        """Verifica se tem atores conhecidos no elenco"""
        if not lista_atores_famosos:
            lista_atores_famosos = []

        if not self.elenco:
            return False

        elenco_limpo = [ator.strip().lower() for ator in self.elenco.split(',')]
        famosos_limpos = [ator.lower() for ator in lista_atores_famosos]

        for ator in elenco_limpo:
            if ator in famosos_limpos:
                return True

        return False

    def eh_recente(self):
        """Diz se o filme é considerado recente (últimos 5 anos)"""
        if not self.ano:
            return False

        ano_atual = datetime.now().year
        return (ano_atual - self.ano) <= 5

    def calcular_idade(self):
        """Calcula quantos anos tem o filme"""
        if not self.ano:
            return 0

        ano_atual = datetime.now().year
        return ano_atual - self.ano


if __name__ == "__main__":
    print("=" * 50)
    print("TESTANDO A CLASSE FILME")
    print("=" * 50)

    # Criando um filme de teste
    filme_teste = GerenciadorFilmes.criar_exemplo()

    print("\n FILME CRIADO:")
    print(filme_teste)

    print("\n FICHA COMPLETA:")
    print(filme_teste.mostrar_info_completa())

    print("\n VALIDAÇÃO:")
    erros = filme_teste.validar_informacoes()
    if erros:
        for erro in erros:
            print(erro)
    else:
        print("✓ Todos os dados estão corretos!")

    print(f"\n IDADE DO FILME: {filme_teste.calcular_idade()} anos")

    print(f"\n TEM ATOR FAMOSO: {filme_teste.tem_ator_famoso(['Alexandre Rodrigues', 'Seu Jorge'])}")

    print(f"\n É RECENTE: {filme_teste.eh_recente()}")

    print("\n CONVERTENDO PARA DICIONÁRIO:")
    dados = filme_teste.converter_para_dicionario()
    for chave, valor in dados.items():
        print(f"  {chave}: {valor}")
