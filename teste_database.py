from db.database import DeltaDatabase
import pandas as pd
import os

# Caminho da tabela
table_path = "data/filmes"

# Se já existir de testes anteriores, remove para recomeçar do zero
if os.path.exists(table_path):
    import shutil
    shutil.rmtree(table_path)
if os.path.exists(f"{table_path}.seq"):
    os.remove(f"{table_path}.seq")

# Cria uma instância do "banco"
db = DeltaDatabase(table_path)

print("✅ Banco Delta criado com sucesso!")

# 1️⃣ Inserindo registros
print("\n=== INSERINDO REGISTROS ===")
db.insert({"nome": "Matrix", "ano_lancamento": 1999, "genero": "Ficção", "diretor": "Wachowski"})
db.insert({"nome": "Interestelar", "ano_lancamento": 2014, "genero": "Ficção", "diretor": "Nolan"})
db.insert({"nome": "Oppenheimer", "ano_lancamento": 2023, "genero": "Drama", "diretor": "Nolan"})

# 2️⃣ Lendo os dados
print("\n=== LENDO TABELA ===")
db.read(table_path)

# 3️⃣ Atualizando um registro
print("\n=== ATUALIZANDO REGISTRO COM ID 2 ===")
db.update(2, {"nome": "Interestelar (versão estendida)", "ano_lancamento": 2015})
db.read(table_path)

# 4️⃣ Deletando um registro
print("\n=== DELETANDO REGISTRO COM ID 1 ===")
db.delete(1)
db.read(table_path)

# 5️⃣ Contando registros
print("\n=== CONTANDO REGISTROS ===")
print(f"Total de registros: {db.count()}")