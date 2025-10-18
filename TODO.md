# TODO: Integrar Classe Filme com API REST

## 1. Atualizar Modelos Pydantic em main.py
- [x] Modificar FilmeCreate para usar campos da classe Filme: titulo_brasil, titulo_original, ano, direcao, elenco, categoria, tempo_minutos, nacionalidade, idioma, resumo, quando_cadastrou, quem_cadastrou
- [x] Modificar FilmeUpdate para os mesmos campos opcionais

## 2. Modificar Endpoints em main.py
- [x] No endpoint POST /filmes/: Usar Filme.criar_apartir_dict para criar objeto Filme, validar com validar_informacoes(), converter para dict com converter_para_dicionario()
- [x] No endpoint PUT /filmes/{filme_id}: Usar Filme para validação e conversão
- [x] Adicionar validação nos endpoints de criação e atualização

## 3. Atualizar popular_banco.py
- [x] Modificar geração de dados para usar campos da classe Filme
- [x] Usar GerenciadorFilmes.pegar_campos_para_csv() se necessário
- [x] Gerar dados fictícios compatíveis com os novos campos

## 4. Testar Integração
- [x] Executar testes para verificar se a API funciona com a nova entidade
- [x] Verificar inserção, listagem, atualização e validação
