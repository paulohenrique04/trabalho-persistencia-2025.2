import random
from db.database import DeltaDatabase
from faker import Faker
from filme import Filme
from datetime import datetime

def popular_banco_dados(quantidade=1000):
    # Configura Faker para português brasileiro
    fake = Faker("pt_BR")

    # Inicializa o banco de dados
    db = DeltaDatabase("data/filmes")

    # Gêneros de filmes
    generos = [
        "Ação", "Aventura", "Comédia", "Drama", "Ficção Científica",
        "Terror", "Romance", "Suspense", "Documentário", "Animação",
        "Fantasia", "Musical", "Crime", "Mistério", "Guerra"
    ]

    # Nacionalidades
    nacionalidades = [
        "Brasil", "Estados Unidos", "França", "Reino Unido", "Itália",
        "Alemanha", "Espanha", "Japão", "Coreia do Sul", "México",
        "Argentina", "Canadá", "Austrália", "Índia", "China"
    ]

    # Idiomas
    idiomas = [
        "Português", "Inglês", "Francês", "Espanhol", "Italiano",
        "Alemão", "Japonês", "Coreano", "Mandarim", "Hindi"
    ]

    print(f"Iniciando população do banco com {quantidade} filmes...")

    for i in range(quantidade):
        # Gera dados fictícios para um filme
        titulo_brasil = f"{fake.word().title()} {fake.word().title()}"
        titulo_original = titulo_brasil if random.choice([True, False]) else f"{fake.word().title()} {fake.word().title()}"

        filme_data = {
            "titulo_brasil": titulo_brasil,
            "titulo_original": titulo_original,
            "ano": random.randint(1980, 2024),
            "direcao": fake.name(),
            "elenco": ", ".join([fake.name() for _ in range(random.randint(1, 5))]),
            "categoria": random.choice(generos),
            "tempo_minutos": random.randint(60, 240),
            "nacionalidade": random.choice(nacionalidades),
            "idioma": random.choice(idiomas),
            "resumo": fake.sentence(nb_words=20),
            "quando_cadastrou": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "quem_cadastrou": fake.user_name()
        }

        # Criar objeto Filme para validação
        obj_filme = Filme.criar_apartir_dict(filme_data)
        problemas = obj_filme.validar_informacoes()

        if problemas:
            print(f"Filme {i+1} inválido: {problemas}. Pulando...")
            continue

        # Converter para dicionário para inserção
        dados_para_inserir = obj_filme.converter_para_dicionario()

        # Insere no banco de dados
        db.insert(dados_para_inserir)

        # Progresso a cada 100 inserções
        if (i + 1) % 100 == 0:
            print(f"Inseridos {i + 1} filmes...")

    print(f"População concluída! Total de {quantidade} filmes inseridos.")

    # Mostra estatísticas
    total = db.count()
    print(f"Total de filmes no banco: {total}")

if __name__ == "__main__":
    popular_banco_dados(1000)
