from deltalake import DeltaTable, write_deltalake
import pandas as pd
import os

# df = pd.DataFrame(columns=["id", "nome", "ano_lancamento", "genero", "diretor", "roteirista", "duracao", "classificacao", "orcamento"])


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
        # Como é um requisito usar o arquivo .seq para os IDs da aplicação, faremos com que ele tenha o mesmo nome
        # do diretório da tabela, mas com a extensão .seq. Por exemplo: data/filmes.seq
        self.seq_path = f"{self.path}.seq"

        # Verificamos se o diretório já existe, se não existir, criamos o diretório e inicializamos a tabela.
        # Vamos inicializar a tabela com um DataFrame com a coluna de id para deixar algo mais genérico e para que consigamos expandir 
        # o código com facilidade para entregas futuras
        if not os.path.exists(table_path):
            os.makedirs(table_path, exist_ok=True)
            df = pd.DataFrame(columns=["id"])
            write_deltalake(table_path, df)
        
        # verificamos se ainda não existe um arquivo .seq para lidar com a tabela que 
        # queremos adicionar, se não existir, criamos o arquivo e iniciamos com o valor 0
        if not os.path.exists(self.seq_path):
            with open(self.seq_path, "w") as file:
                file.write("0")
    
    def create():
        # TODO
        return
    
    def read():
        # TODO
        return
    
    def update():
        # TODO
        return
    
    def delete():
        # TODO
        return
    
    def count():
        # TODO
        return
    
    def vacuum():
        # TODO
        return


    
    

        