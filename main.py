from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any
import pandas as pd
import zipfile
import io
import hashlib
from deltalake import DeltaTable
from db.database import DeltaDatabase
from filme import Filme

app = FastAPI(title="API de Filmes", version="1.0.0")

# Inicializa o banco de dados Delta
db = DeltaDatabase("data/filmes")

# Modelo Pydantic para validação dos dados de filme
class FilmeCreate(BaseModel):
    titulo_brasil: str
    titulo_original: Optional[str] = None
    ano: int
    direcao: str
    elenco: Optional[str] = None
    categoria: str
    tempo_minutos: int
    nacionalidade: str
    idioma: Optional[str] = None
    resumo: Optional[str] = None
    quando_cadastrou: Optional[str] = None
    quem_cadastrou: Optional[str] = None

class FilmeUpdate(BaseModel):
    titulo_brasil: Optional[str] = None
    titulo_original: Optional[str] = None
    ano: Optional[int] = None
    direcao: Optional[str] = None
    elenco: Optional[str] = None
    categoria: Optional[str] = None
    tempo_minutos: Optional[int] = None
    nacionalidade: Optional[str] = None
    idioma: Optional[str] = None
    resumo: Optional[str] = None
    quando_cadastrou: Optional[str] = None
    quem_cadastrou: Optional[str] = None

class HashRequest(BaseModel):
    dado: str
    funcao_hash: str  # "md5", "sha1", "sha256"

class PaginacaoRequest(BaseModel):
    pagina: int 
    tamanho_pagina: int

# F1: Inserir entidade
@app.post("/filmes/", response_model=Dict[str, Any])
async def criar_filme(filme: FilmeCreate):
    """F1: Inserir um novo filme no banco de dados"""
    try:
        filme_dict = filme.dict()
        # Criar objeto Filme para validação
        obj_filme = Filme.criar_apartir_dict(filme_dict)
        # Validar dados
        problemas = obj_filme.validar_informacoes()
        if problemas:
            raise HTTPException(status_code=400, detail=f"Dados inválidos: {'; '.join(problemas)}")
        # Converter para dicionário para inserção
        dados_para_inserir = obj_filme.converter_para_dicionario()
        filme_id = db.insert(dados_para_inserir)
        return {"mensagem": "Filme inserido com sucesso", "id": filme_id, "dados": dados_para_inserir}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao inserir filme: {str(e)}")
        
# F2: Listar com paginação
@app.post("/filmes/paginacao/")
async def listar_filmes_paginados(paginacao: PaginacaoRequest):
    """F2: Retornar filmes com paginação"""
    try:
        # Carrega todos os dados
        dt = DeltaTable(str(db.path))
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
        dt = DeltaTable(str(db.path))
        df = dt.to_pandas()
        return {"filmes": df.to_dict('records')}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar filmes: {str(e)}")

# GET - Buscar filme por ID
@app.get("/filmes/{filme_id}")
async def buscar_filme(filme_id: int):
    """Buscar filme por ID"""
    try:
        filme = db.get_by_id(filme_id)
        if filme is None:
            raise HTTPException(status_code=404, detail="Filme não encontrado")
        return filme
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

        # Buscar filme existente para validação completa
        filme_existente = db.get_by_id(filme_id)
        if filme_existente is None:
            raise HTTPException(status_code=404, detail="Filme não encontrado")

        # Combinar dados existentes com atualizações
        dados_completos = {**filme_existente, **dados_atualizacao}

        # Criar objeto Filme para validação
        obj_filme = Filme.criar_apartir_dict(dados_completos)
        problemas = obj_filme.validar_informacoes()
        if problemas:
            raise HTTPException(status_code=400, detail=f"Dados inválidos: {'; '.join(problemas)}")

        # Converter para dicionário para atualização
        dados_para_atualizar = obj_filme.converter_para_dicionario()
        db.update(filme_id, dados_para_atualizar)
        return {"mensagem": f"Filme {filme_id} atualizado com sucesso"}
    except HTTPException:
        raise
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
        dt = DeltaTable(str(db.path))
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
    uvicorn.run(app, host="127.0.0.1", port=8000)
