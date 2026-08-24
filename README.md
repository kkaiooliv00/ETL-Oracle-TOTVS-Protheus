# Projeto ETL: Oracle TOTVS Protheus -> PostgreSQL e Integrações

Este documento contém a documentação completa da ferramenta de ETL. O objetivo é servir como guia definitivo para que novos desenvolvedores e analistas de dados possam entender a arquitetura, dar manutenção e **criar novos processos de extração de forma autônoma e modular**.

## 1. Visão Geral

O projeto é um pipeline de dados (ETL) construído em Python. Possui duas frentes principais de extração:
1. **Oracle TOTVS Protheus -> PostgreSQL (Supabase)**: Extração nativa de tabelas do ERP, com suporte a schemas variados, limpeza de dados sujos e carga otimizada no Postgres.
2. **PostgreSQL (Leitura) -> PostgreSQL (Supabase)**: Sincronização entre instâncias PostgreSQL (ex: banco de leitura BI para o Supabase), ideal para relatórios consolidados e transformações mais densas (Ex: processo de Sales).

**Principais Tecnologias Usadas:**
- **Python 3+**: Linguagem e orquestrador principal.
- **oracledb**: Driver Thin para conexão com o Oracle (não exige instalação do Oracle Instant Client).
- **psycopg3 (`psycopg[binary]`)**: Para comunicação de alta performance com PostgreSQL, especialmente usando o comando `COPY`.
- **SQLAlchemy**: Para gerenciamento de pool de conexões (QueuePool) e execução de instruções DDL/Upserts no PostgreSQL.
- **pandas**: Usado na etapa de transformação para normalização, limpeza e manipulação em memória.
- **PyYAML / python-dotenv**: Para leitura de arquivos de configuração declarativos.

---

## 2. Arquitetura e Comportamento

O funcionamento interno difere ligeiramente dependendo do tipo de job.

### Motor Oracle (`oracle_etl.py`)
1. **Extração**: Dados extraídos em lotes (*batches*) de 20.000 registros por padrão. O tamanho é configurável por ambiente para equilibrar velocidade e memória. Cada tabela usa uma query SQL customizada definida no YAML.
2. **Transformação**: O Pandas faz a limpeza de caracteres invisíveis/nulos comuns no TOTVS (como `\xa0` e `\x00`), compõe a **Chave de Negócio (`business_key`)** e garante a tipagem correta.
3. **Carga**:
   - Os dados vão para uma **Tabela de Staging Temporária** no Postgres em frações de segundo usando `COPY FROM STDIN`.
   - É aplicada uma **Deduplicação** no banco (via `ROW_NUMBER()`) para manter a versão mais recente caso haja sobreposição de chaves na extração incremental.
   - Os dados são mesclados usando `INSERT ... ON CONFLICT DO UPDATE` (Upsert). Há suporte nativo a *Schema Drift* (novas colunas no Oracle são criadas automaticamente no Postgres).

### Motor PostgreSQL (`postgres_to_supabase.py`)
1. **Extração**: Consulta via `psycopg` com cursores de servidor para exportação paginada da origem. As queries são grandes e ficam isoladas em arquivos `.sql`.
2. **Carga**: Possui lógica similar de staging e upsert dinâmico (com Schema Drift), mas roda de PostgreSQL para PostgreSQL, possivelmente usando conexões específicas e chaves de negócios dedicadas.

---

## 3. Estrutura de Arquivos e Módulos

O projeto foi construído de forma **modular**, permitindo que diferentes áreas de negócio (Finanças, RH, Comercial) tenham seus pipelines separados.

- **`oracle_etl.py`**: O motor principal para conexões com o Protheus (Oracle).
- **`postgres_to_supabase.py`**: O motor para integrações Postgres-to-Postgres.
- **Arquivos YAML de Configuração**:
  - `oracle_jobs.yml`: Configurações principais/padrão do Protheus.
  - `oracle_jobs_finance.yml`: Pipeline dedicado apenas à rotina financeira (ex: inadimplência).
  - `oracle_jobs_cleanup.yml`: Processos de exclusão e manutenção de registros mortos.
  - `postgres_jobs.yml`: Mapeamentos do motor Postgres-to-Postgres.
- **Queries Externas (`*.sql`)**: Como `postgres_sales_query.sql`. Usadas quando a query é grande demais para caber confortavelmente em um YAML.
- **`verify_tables.py`**: Utilitário que verifica se as tabelas listadas no YAML realmente existem no Oracle (lê contra um arquivo de texto de catálogo).

---

## 4. Variáveis de Ambiente e Configuração

Todo o controle de segredos e roteamento de arquivos ocorre no arquivo `.env`. Copie o `.env.example` para iniciar.

**Credenciais Oracle**: `ORA_HOST`, `ORA_PORT`, `ORA_SERVICE`, `ORA_USER`, `ORA_PASSWORD`
**Destino Supabase (Padrão)**: `DATABASE_URL`

**Variáveis Essenciais de Roteamento (Jobs)**:
- `ETL_JOBS_FILE`: Define qual YAML o `oracle_etl.py` vai executar. (Padrão: `oracle_jobs.yml`).
- `POSTGRES_JOBS_FILE`: Define qual YAML o `postgres_to_supabase.py` vai executar. (Padrão: `postgres_jobs.yml`).

**Variáveis Opcionais de Volume**:
- `ORACLE_FETCH_SIZE`: registros lidos por lote do Oracle (padrão: `20000`).
- `STAGING_FLUSH_RECORDS`: registros acumulados antes do `COPY` (padrão: `20000`).
- `POSTGRES_FETCH_SIZE`: registros lidos por lote do PostgreSQL de origem (padrão: `20000`).

**Credenciais PostgreSQL Origem (usado no motor Postgres)**:
- Váriaveis prefixadas com `GW_` (Ex: `GW_SOURCE_HOST`, `GW_SOURCE_PORT`, `GW_SOURCE_USER`, `GW_SOURCE_PASSWORD`).

---

## 5. Como Executar (Manualmente e no Servidor)

Você pode passar a variável de ambiente dinamicamente via terminal para escolher qual módulo executar.

### Executando Módulos Oracle
```bash
# Executa o módulo padrão (oracle_jobs.yml)
python oracle_etl.py

# Executa um módulo específico (ex: Financeiro)
ETL_JOBS_FILE=oracle_jobs_finance.yml python oracle_etl.py

# Filtra apenas tabelas específicas (útil para desenvolvimento rápido)
python oracle_etl.py --tables "SC1,SC7"

# Executa ignorando algumas tabelas
python oracle_etl.py --exclude-tables "TMP,ZZ1e2"

# Modo exclusão (Apaga as chaves extraídas da origem)
python oracle_etl.py --delete-mode
```

### Executando Integrações PostgreSQL
```bash
# Executa os pipelines do postgres_jobs.yml
python postgres_to_supabase.py

# Rodando apenas um job específico (pelo target_table)
python postgres_to_supabase.py --tables "sales"
```

### GitHub Actions
Na aba "Actions" do GitHub, você pode rodar os pipelines remotamente (as secrets já estão salvas).
Há opções como **Exportar para Excel** (sem subir pro Supabase, o GitHub gera um `.xlsx` como artefato) e seletores para módulos modulares e específicos.

---

## 6. Como Criar Novos Processos (Passo a Passo)

A ferramenta é orientada a configuração. **Você raramente precisará alterar o código em Python (`.py`)** para criar um novo pipeline.

### Cenário A: Criar um Novo Pipeline Oracle (Ex: Módulo Estoque)

1. **Crie o arquivo YAML**
   Crie um arquivo chamado `oracle_jobs_estoque.yml` na raiz do projeto.

2. **Defina a estrutura**
   ```yaml
   oracle_schema: U_PROTHEUS_PRD  # Substitua pelo schema correto
   jobs:
     - oracle_table: SB1010
       target_table: ESTOQUE_PRODUTOS
       query: >
         SELECT
             B1_COD || '_' || B1_FILIAL AS SUPER_CHAVE,
             B1_DESC, B1_TIPO, B1_PESO
         FROM {schema}.SB1010
         WHERE D_E_L_E_T_ = ' '
   ```
   **Regras da Query Oracle**:
   - A query **DEVE** retornar uma coluna para ser a chave de negócio. Recomenda-se batizá-la de `SUPER_CHAVE`.
   - Lembre-se sempre de filtrar registros excluídos logicamente (`WHERE D_E_L_E_T_ = ' '`).
   - Use o placeholder `{schema}` para referenciar o owner de forma dinâmica.

3. **Teste Localmente**
   ```bash
   ETL_JOBS_FILE=oracle_jobs_estoque.yml python oracle_etl.py
   ```

### Cenário B: Criar um Novo Pipeline PostgreSQL (Ex: Tabela de Comissões)

1. **Escreva a Query SQL Isolada**
   Crie o arquivo `postgres_comissoes_query.sql` com a sua consulta longa. Assegure-se de trazer uma coluna única, como `chave_comissao`.

2. **Registre o Job no YAML**
   Edite o `postgres_jobs.yml` (ou crie um novo) e adicione o bloco:
   ```yaml
   - target_schema: gw
     target_table: comissoes
     business_key: chave_comissao
     query_file: postgres_comissoes_query.sql
   ```

3. **Execute**
   ```bash
   python postgres_to_supabase.py --tables "comissoes"
   ```

---

## 7. Manutenção e Dicas Vitais

1. **Schema Drift (Alteração de Estrutura)**:
   Se você adicionar uma nova coluna na sua query SQL (tanto Oracle quanto Postgres), o motor Python vai detectar automaticamente e fará um `ALTER TABLE ADD COLUMN` no Supabase. **Não é necessário criar colunas manualmente no banco de dados de destino**.

2. **D_E_L_E_T_ do TOTVS Protheus**:
   O Python **não filtra automaticamente** os registros deletados quando você usa `query` customizada. Você **precisa** adicionar `WHERE D_E_L_E_T_ = ' '` (são 8 caracteres) no seu YAML.

3. **Gerenciamento de Falhas e Retry**:
   Os motores são resilientes. O `JOB_MAX_ATTEMPTS = 3` isola cada tabela. Se o banco destino travar ou a rede cair no meio da carga de uma tabela de 5GB, ele fechará a conexão isolada, aguardará 10s, e reiniciará apenas aquela tabela.

4. **Colunas Incompatíveis**:
   Campos do Oracle contendo caracteres lixo como byte nulo (`\x00`) travam o Postgres. O ETL já limpa isso automaticamente. O motor também faz um mapeamento burro-inteligente de tipos: se no Pandas for string, vira `TEXT`, se for número virá `NUMERIC`/`BIGINT`, se for data virá `TIMESTAMPTZ` ou `DATE`.

5. **Lógica de "Delete-Mode"**:
   O script `oracle_etl.py` aceita a flag `--delete-mode`. O `oracle_jobs_cleanup.yml` inclui automaticamente os jobs principais e financeiros, extrai apenas suas chaves ativas e reconcilia cada tabela do Supabase, removendo registros que já não fazem parte do resultado da origem. Novos jobs adicionados aos arquivos incluídos entram automaticamente no cleanup.
