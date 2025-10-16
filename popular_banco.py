import random
from database import DeltaDatabase
from faker import Faker

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
    
    # Classificações etárias
    classificacoes = ["L", "10", "12", "14", "16", "18"]
    
    print(f"Iniciando população do banco com {quantidade} filmes...")
    
    for i in range(quantidade):
        # Gera dados fictícios para um filme
        filme = {
            "nome": f"{fake.word().title()} {fake.word().title()}",
            "ano_lancamento": random.randint(1980, 2024),
            "genero": random.choice(generos),
            "diretor": fake.name(),
            "roteirista": fake.name(),
            "duracao": random.randint(60, 240),  # minutos
            "classificacao": random.choice(classificacoes),
            "orcamento": round(random.uniform(100000, 200000000), 2),
        }
        
        # Insere no banco de dados
        db.insert(filme)

        # Progresso a cada 100 inserções
        if (i + 1) % 100 == 0:
            print(f"Inseridos {i + 1} filmes...")

    print(f"População concluída! Total de {quantidade} filmes inseridos.")

    # Mostra estatísticas
    total = db.count()
    print(f"Total de filmes no banco: {total}")

if __name__ == "__main__":
    popular_banco_dados(1000)