from deltalake import DeltaTable, write_deltalake
import pandas as pd
import os
from pathlib import Path

class DeltaDatabase:
    """
    Classe para representar um banco de dados simples usando Delta Lake
    """

    def __init__(self, table_path: str):
        self.path = Path(table_path)
        self.seq_file = self.path / ".seq"
        self.path.mkdir(parents=True, exist_ok=True)
        if not self.seq_file.exists():
            self.write_seq_file("0")

    def read_seq_file(self) -> int:
        return int(self.seq_file.read_text().strip() or 0)
    
    # Sobrecreve o arquivo .seq com o novo valor
    def write_seq_file(self, value: int):
        self.seq_file.write_text(str(int(value)))
    
    def get_next_id(self) -> int:
        current_id = self.read_seq_file() + 1
        self.write_seq_file(current_id)
        return current_id
    
    def insert(self, data: dict):
        current_id = self.get_next_id()
        data["id"] = current_id
        df = pd.DataFrame([data])

        if not any(self.path.iterdir()):
            write_deltalake(str(self.path), df)
        else:
            write_deltalake(str(self.path), df, mode="append")
        
        return current_id
    
    def get_by_id(self, record_id: int) -> dict | None:
        dt = DeltaTable(self.path)
        df = dt.to_pandas()
        record = df[df["id"] == record_id]
        if not record.empty:
            return record.iloc[0].to_dict()
    
    def read(self, path: str):
        df = DeltaTable(path).to_pandas()
        print(df)
    
    def update(self, update_id: int, new_data: dict):
        df = DeltaTable(self.path).to_pandas()

        if update_id in df["id"].values:
            for col, value in new_data.items():
                if col in df.columns:
                    df.loc[df["id"] == update_id, col] = value
                else:
                    raise ValueError(f"Coluna '{col}' não existe na tabela.")
            write_deltalake(self.path, df, mode="overwrite")
        else:
            raise ValueError(f"ID '{update_id}' não encontrado na tabela.")
    
    def delete(self, delete_id: int):
        df = DeltaTable(self.path).to_pandas()

        if delete_id in df["id"].values:
            df = df[df["id"] != delete_id]
            df = df.reset_index(drop=True)
            
            if df.empty:
                for file in self.path.iterdir():
                    if file.is_file():
                        os.remove(file)
                print("Todos os registros foram deletados. Tabela removida.")
        else:
            raise ValueError(f"ID '{delete_id}' não encontrado na tabela.")
    
    def count(self) -> int:
        if not any(self.path.iterdir()):
            return 0
        
        try:
            dt = DeltaTable(self.path)
            return len(dt.to_pandas())
        except Exception as e:
            print(f"Erro ao contar registros: {e}")
            return 0
    
    def vacuum(self):
        if not any(self.path.iterdir()):
            print("Tabela não existe ou está vazia.")
            return
        
        try:
            dt = DeltaTable(self.path)
            dt.vacuum(retention_hours=0)
        except Exception as e:
            print(f"Erro ao executar vacuum: {e}")

if __name__ == "__main__":
    db = DeltaDatabase("./data/usuarios")

    # CREATE
    user_id = db.insert({"nome": "Paulo", "idade": 25})
    print(f"Usuário inserido com ID: {user_id}")

    # READ
    print("Usuário encontrado:", db.get_by_id(user_id))

    # UPDATE
    db.update(user_id, {"idade": 26})
    print("Atualizado:", db.get_by_id(user_id))

    # COUNT
    print("Total de registros:", db.count())

    # DELETE
    db.delete(user_id)
    print("Após deleção, total:", db.count())

    # VACUUM
    db.vacuum()