from deltalake import DeltaTable, write_deltalake
import pandas as pd
import os

class DeltaDatabase:
    """
    Classe para representar um banco de dados simples usando Delta Lake
    """

    def __init__(self, table_path: str):
        """
        Inicializa o banco de dados

        Args:
            table_path (str): Caminho para o diretório onde os dados serão armazenados
        """

        # Primeiro guardamos o caminho da tabela que queremos armazenar os dados, por exemplo: data/filmes
        self.path = table_path
        self.table_path = table_path
        # Como é um requisito usar o arquivo .seq para os IDs da aplicação, faremos com que ele tenha o mesmo nome
        # do diretório da tabela, mas com a extensão .seq. Por exemplo: data/filmes.seq
        self.seq_path = f"{self.path}.seq"

        # Verificamos se o diretório já existe, se não existir, criamos o diretório e inicializamos a tabela.
        if not os.path.exists(table_path):
            os.makedirs(table_path, exist_ok=True)
            # Criar um DataFrame vazio com todas as colunas que vamos usar
            df = pd.DataFrame(columns=[
                "id", 
                "nome", 
                "ano_lancamento", 
                "genero", 
                "diretor", 
                "roteirista", 
                "duracao", 
                "classificacao", 
                "orcamento"
            ]).astype({
                "id": "int64",
                "nome": "string",
                "ano_lancamento": "int64", 
                "genero": "string",
                "diretor": "string",
                "roteirista": "string",
                "duracao": "int64",
                "classificacao": "string",
                "orcamento": "float64"
            })
            write_deltalake(table_path, df)
        
        # verificamos se ainda não existe um arquivo .seq para lidar com a tabela que 
        # queremos adicionar, se não existir, criamos o arquivo e iniciamos com o valor 0
        if not os.path.exists(self.seq_path):
            with open(self.seq_path, "w") as file:
                file.write("0")
    
    def get_next_id(self) -> int:
        with open(self.seq_path, "r") as file:
            current_id = int(file.read().strip())
        next_id = current_id + 1

        with open(self.seq_path, "w") as file:
            file.write(str(next_id))

        return next_id
    
    def insert(self, data: dict):
        current_id = self.get_next_id()
        data["id"] = current_id
        
        # Garantir que todas as colunas estejam presentes no DataFrame
        # Criar um dicionário com todas as colunas esperadas
        default_data = {
            "id": current_id,
            "nome": "",
            "ano_lancamento": 0,
            "genero": "",
            "diretor": "",
            "roteirista": "",
            "duracao": 0,
            "classificacao": "",
            "orcamento": 0.0
        }
        
        # Atualizar com os dados fornecidos
        default_data.update(data)
        
        df = pd.DataFrame([default_data])
        write_deltalake(self.path, df, mode="append")
    
    def read(self, path: str):
        df = DeltaTable(path).to_pandas()
        print(df)
    
    def update(self, update_id: int, new_data: dict):
        df = DeltaTable(self.table_path).to_pandas()

        if update_id in df["id"].values:
            for col, value in new_data.items():
                if col in df.columns:
                    df.loc[df["id"] == update_id, col] = value
                else:
                    raise ValueError(f"Coluna '{col}' não existe na tabela.")
            write_deltalake(self.table_path, df, mode="overwrite")
        else:
            raise ValueError(f"ID '{update_id}' não encontrado na tabela.")
    
    def delete(self, delete_id: int):
        df = DeltaTable(self.table_path).to_pandas()

        if delete_id in df["id"].values:
            df = df[df["id"] != delete_id]
            df = df.reset_index(drop=True)
            write_deltalake(self.table_path, df, mode="overwrite", schema_mode="overwrite")
            return True
        else:
            raise ValueError(f"ID '{delete_id}' não encontrado na tabela.")
    
    def count(self) -> int:
        dt = DeltaTable(self.path)
        return len(dt.to_pandas())
    
    def vacuum(self):
        # TODO
        return