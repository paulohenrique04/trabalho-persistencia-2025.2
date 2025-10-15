from fastapi import FastAPI, HTTPException,Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing  import  Optional, Dict, Any
import pandas as pd
import zipfile
import io
import hashlib
from db.database import DeltaDatabase

app = FastAPI(title="API de Filmes", version="1.0.0")

# Inicializa o banco de dados Delta
db = DeltaDatabase("data/filmes")

# Modelo Pydantic para validação dos dados de filme
class FilmeCreate(BaseModel):
    nome: str
    ano_lancamento: int
    genero: str
    diretor: str
    roteirista: str
    duracao: int
    classificacao: str
    orcamento: float

    class FilmeUpdate(BaseModel):
     nome: Optional[str] = None
    ano_lancamento: Optional[int] = None
    genero: Optional[str] = None
    diretor: Optional[str] = None
    roteirista: Optional[str] = None
    duracao: Optional[int] = None
    classificacao: Optional[str] = None
    orcamento: Optional[float] = None

    class HashRequest(BaseModel):
        dado: str
        funcao_hash: str  # "md5", "sha1", "sha256"
        class  PaginacaoRequest(BaseModel):
          pagina: int 
          tamanho_pagina: int 
          # F1: Inserir entidade
@app.post("/filmes/", response_model=Dict[str, Any])
async def criar_filme(filme: FilmeCreate):
    """F1: Inserir um novo filme no banco de dados"""
    try:
        filme_dict = filme.dict()
        db.insert(filme_dict)
        return {"mensagem": "Filme inserido com sucesso", "dados": filme_dict}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao inserir filme: {str(e)}")
        
        # F2: Listar com paginação
@app.post("/filmes/paginacao/")
async def listar_filmes_paginados(paginacao: PaginacaoRequest):
    """F2: Retornar filmes com paginação"""
    try:
        # Carrega todos os dados
        dt = DeltaTable(db.path)
        df = dt.to_pandas()
        
        # Calcula índices para paginação
        inicio = (paginacao.pagina - 1) * paginacao.tamanho_pagina
        fim = inicio + paginacao.tamanho_pagina
        
        # Aplica paginação
        df_paginado = df.iloc[inicio:fim]
        
        return {
            "pagina": paginacao.pagina,
            "tamanho_pagina": paginacao.tamanho_pagina,
            "total_filmes": len(df),
            "filmes": df_paginado.to_dict('records')
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar filmes: {str(e)}")
        
       # F3: CRUD Completo

# GET - Listar todos os filmes
@app.get("/filmes/")
async def listar_filmes():
    """Listar todos os filmes"""
    try:
        dt = DeltaTable(db.path)
        df = dt.to_pandas()
        return {"filmes": df.to_dict('records')}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar filmes: {str(e)}")

# GET - Buscar filme por ID
@app.get("/filmes/{filme_id}")
async def buscar_filme(filme_id: int):
    """Buscar filme por ID"""
    try:
        dt = DeltaTable(db.path)
        df = dt.to_pandas()
        filme = df[df["id"] == filme_id]
        
        if filme.empty:
            raise HTTPException(status_code=404, detail="Filme não encontrado")
        
        return filme.iloc[0].to_dict()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao buscar filme: {str(e)}")

# PUT - Atualizar filme
@app.put("/filmes/{filme_id}")
async def atualizar_filme(filme_id: int, filme: FilmeUpdate):
    """Atualizar filme por ID"""
    try:
        # Filtra apenas os campos que foram fornecidos
        dados_atualizacao = {k: v for k, v in filme.dict().items() if v is not None}
        
        if not dados_atualizacao:
            raise HTTPException(status_code=400, detail="Nenhum dado fornecido para atualização")
        
        db.update(filme_id, dados_atualizacao)
        return {"mensagem": f"Filme {filme_id} atualizado com sucesso"}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao atualizar filme: {str(e)}")

# DELETE - Remover filme
@app.delete("/filmes/{filme_id}")
async def remover_filme(filme_id: int):
    """Remover filme por ID"""
    try:
        db.delete(filme_id)
        return {"mensagem": f"Filme {filme_id} removido com sucesso"}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao remover filme: {str(e)}")

# F4: Contagem de entidades
@app.get("/filmes/contagem/")
async def contar_filmes():
    """F4: Retornar a quantidade total de filmes"""
    try:
        quantidade = db.count()
        return {"total_filmes": quantidade}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao contar filmes: {str(e)}")

# F5: Exportar dados em CSV compactado
@app.get("/filmes/exportar/")
async def exportar_filmes():
    """F5: Exportar todos os filmes como CSV compactado via streaming"""
    try:
        dt = DeltaTable(db.path)
        df = dt.to_pandas()
        
        # Cria um buffer em memória para o ZIP
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Converte DataFrame para CSV em memória
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            
            # Adiciona o CSV ao arquivo ZIP
            zip_file.writestr("filmes.csv", csv_buffer.getvalue())
        
        zip_buffer.seek(0)
        
        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={"Content-Disposition": "attachment; filename=filmes.zip"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao exportar filmes: {str(e)}")

# F6: Calcular hash
@app.post("/hash/")
async def calcular_hash(hash_request: HashRequest):
    """F6: Calcular hash de um dado usando MD5, SHA1 ou SHA256"""
    try:
        dado = hash_request.dado.encode('utf-8')
        funcao = hash_request.funcao_hash.lower()
        
        if funcao == "md5":
            hash_resultado = hashlib.md5(dado).hexdigest()
        elif funcao == "sha1":
            hash_resultado = hashlib.sha1(dado).hexdigest()
        elif funcao == "sha256":
            hash_resultado = hashlib.sha256(dado).hexdigest()
        else:
            raise HTTPException(
                status_code=400, 
                detail="Função hash não suportada. Use: md5, sha1 ou sha256"
            )
        
        return {
            "dado": hash_request.dado,
            "funcao_hash": funcao,
            "hash": hash_resultado
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao calcular hash: {str(e)}")

@app.get("/")
async def root():
    return {"mensagem": "API de Filmes - Delta Database", "version": "1.0.0"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)