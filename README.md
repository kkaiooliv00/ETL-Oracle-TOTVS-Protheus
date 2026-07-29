# Projeto ETL: Oracle TOTVS Protheus -> PostgreSQL

Este documento resume o comportamento, configurações e processos do projeto de ETL. O objetivo é servir como guia para que novos desenvolvedores possam entender, dar manutenção e evoluir o código.

## 1. Visão Geral

O projeto é um pipeline de dados (ETL) construído em Python. Sua função principal é extrair dados de tabelas do ERP Oracle TOTVS Protheus e carregá-los em um banco de dados PostgreSQL (ex: Supabase).

**Principais Tecnologias Usadas:**
- **Python 3+**: Linguagem principal.
- **oracledb**: Driver Thin para conexão com o Oracle (não exige instalação do Oracle Instant Client).
- **psycopg3 (`psycopg[binary]`)**: Para comunicação de alta performance com PostgreSQL, especialmente usando o comando `COPY`.
- **SQLAlchemy**: Para gerenciamento de pool de conexões (QueuePool) e execução de DDL/Upserts no PostgreSQL.
- **pandas**: Usado na etapa de transformação para normalização, limpeza e manipulação em memória.
- **PyYAML / python-dotenv**: Para leitura de configurações.

## 2. Arquitetura e Comportamento

O script principal (`oracle_etl.py`) executa o processo ETL da seguinte forma para cada job definido:

1. **Extração (Oracle)**:
   - Os dados são extraídos em lotes (*batches*) usando um cursor do Oracle com `FETCH_SIZE = 10_000` para otimizar o uso de memória.
   - Cada tabela possui uma query customizada definida no YAML.

2. **Transformação (Pandas)**:
   - Limpeza de caracteres inválidos comuns no TOTVS (como espaços sem quebra `\xa0` e *trailing spaces*).
   - Composição da **Chave de Negócio (`business_key`)** se não estiver diretamente na query, utilizando as colunas especificadas em `business_key_columns`.
   - Limpeza de bytes nulos (`\x00`) incompatíveis com o tipo TEXT do PostgreSQL e formatação de tipos complexos para JSON.

3. **Carga (PostgreSQL)**:
   - **Staging Area**: Os dados processados no buffer (`STAGING_FLUSH_RECORDS = 10_000`) são inseridos rapidamente no PostgreSQL usando o comando `COPY` para uma tabela temporária de staging (`tabela_staging`).
   - **Deduplicação**: Como o processo incremental pode gerar linhas duplicadas, é aplicada uma lógica no banco (`ROW_NUMBER() OVER(PARTITION BY chave ORDER BY ctid DESC)`) para manter apenas a versão mais recente em uma tabela `tabela_staging_dedup`.
   - **Upsert Final**: Usando a instrução `INSERT ... ON CONFLICT DO UPDATE` (Upsert), os dados são gravados definitivamente na tabela de destino no schema `tables`. Se a tabela não possuir uma chave (`super_chave`), é feito um `TRUNCATE` seguido de um `INSERT` total.

## 3. Configuração

Toda a configuração é descentralizada para não precisar alterar o código fonte constantemente.

### Variáveis de Ambiente (`.env`)
Arquivo responsável pelos segredos e conexões (baseado no `.env.example`):
- `ORA_HOST`, `ORA_PORT`, `ORA_SERVICE`, `ORA_USER`, `ORA_PASSWORD`: Credenciais do Oracle.
- `DATABASE_URL`: String de conexão do PostgreSQL (Ex: `postgresql://user:pass@host:port/db`).
- `ETL_JOBS_FILE` (Opcional): Caminho para o YAML de configuração de jobs. Padrão: `oracle_jobs.yml`.

### Mapeamento de Tabelas (`oracle_jobs.yml`)
O arquivo central que define **o que** será extraído. Para cada job, define-se:
- `oracle_table`: Tabela origem (ex: `SC1010`).
- `target_table`: Tabela destino no Postgres (ex: `SC1`).
- `query`: Query SQL a ser executada no Oracle. Recomenda-se explicitar os campos. Usa a tag `{schema}` para definir o owner. A query já deve retornar a coluna definida como chave (ex: `SUPER_CHAVE`).
- `date_column` (opcional): Coluna de data utilizada para carga incremental (não é estritamente usado na query customizada atual, mas compõe as regras do motor).
- `business_key` (opcional): Coluna de chave única, padrão é `super_chave`.

## 4. Processos e Execução

Para executar o pipeline, basta chamar o script via linha de comando. O script aceita parâmetros que facilitam a execução pontual:

```bash
# Executa todos os jobs definidos no YAML
python oracle_etl.py

# Carga Incremental vs Carga Total
# O padrão é lookback-days de 7 dias (se aplicável nas regras no futuro).
# Use 0 para forçar ignorar o filtro incremental (caso estivesse configurado nas queries).
python oracle_etl.py --lookback-days 0

# Filtrar tabelas específicas (útil para testes ou reprocessamentos rápidos)
python oracle_etl.py --tables "SC1,SC7"

# Excluir tabelas específicas da carga
python oracle_etl.py --exclude-tables "TMP,ZZ1e2"
```

### Exportar para Excel pelo GitHub Actions

No menu **Actions**, execute o workflow manualmente e escolha:

- `destination: excel` para extrair do Oracle sem usar qualquer conexao ou segredo do Supabase;
- o modo de carga (`all`, `custom` ou `exclude`) para escolher as tabelas.

Ao terminar, o GitHub disponibiliza o artefato **exportacao-oracle-xlsx** na pagina da execucao. Baixe-o para obter um unico arquivo `oracle_export.xlsx`, com uma aba por tabela selecionada. Arquivos ficam disponiveis por 7 dias. O Excel suporta ate 1.048.576 linhas por aba; acima disso, o processo cria novas abas para a mesma tabela.

No terminal, a mesma exportacao pode ser feita assim:

```bash
python oracle_etl.py --tables "SC1,SC7" --export-xlsx "artifacts/oracle_export.xlsx"
```

O envio por e-mail tambem e possivel, mas requer uma configuracao adicional de provedor (por exemplo, Microsoft 365/Graph ou SMTP) e credenciais armazenadas como secrets do GitHub. Para arquivos grandes, e preferivel enviar um link de download em vez de anexar o XLSX.

### PostgreSQL de origem para Supabase (`gw.sales`)

O workflow tambem possui o destino `postgres-to-supabase`. Ele executa a consulta em `postgres_sales_query.sql` no banco de leitura e faz *upsert* em `gw.sales` usando `super_chave`.

Cadastre estas secrets no GitHub, todas com prefixo `GW_`:

- `GW_SOURCE_HOST`: `rds-los-12-leitura-bi-revolux.cbqxjhiaugyj.us-east-2.rds.amazonaws.com`
- `GW_SOURCE_PORT`: `5432`
- `GW_SOURCE_DATABASE`: `postgres`
- `GW_SOURCE_USER`: `bi_machine`
- `GW_SOURCE_PASSWORD`: senha do banco de leitura
- `GW_SOURCE_SSLMODE`: opcional; use `require` (tambem e o padrao)
- `GW_DATABASE_URL`: opcional; URL de conexão do Supabase exclusiva para esta carga. Se não for cadastrada, a carga reutiliza a secret `DATABASE_URL` já existente.

As secrets Oracle não são usadas no modo `postgres-to-supabase`. A `DATABASE_URL` existente é reutilizada como destino do Supabase quando `GW_DATABASE_URL` não for cadastrada. A query atual filtra emissões a partir de `2026-05-01`; esse marco pode ser alterado em `postgres_sales_query.sql`.

### Outros Scripts
- `verify_tables.py`: É um script utilitário simples que lê o `oracle_jobs.yml` e checa se as tabelas contidas lá estão listadas no arquivo `tables.txt`. Ele informa se falta alguma tabela no ambiente Oracle (baseado nesse log/arquivo TXT externo).

## 5. Manutenção e Evolução do Código (Dicas)

1. **Adicionar nova tabela**:
   - Não altere o `oracle_etl.py`.
   - Adicione um novo bloco no `oracle_jobs.yml`.
   - Certifique-se de escrever a query SQL retornando a coluna de chave (geralmente gerada via `|| '_' ||` de campos relevantes e nomeada como `SUPER_CHAVE`).
   - Rode `python oracle_etl.py --tables "NOVA_TABELA"` para testar isoladamente.

2. **Gerenciamento de Falhas e Retry**:
   - O orquestrador tem a constante `JOB_MAX_ATTEMPTS = 3`.
   - Caso um job falhe por lock de banco ou perda de conexão, ele aguardará alguns segundos e tentará novamente, reiniciando o pool do Postgres e a conexão do Oracle para aquele job, isolando o erro e garantindo resiliência.

3. **Lógica de Deleção TOTVS (`D_E_L_E_T_`)**:
   - É responsabilidade da query SQL no arquivo YAML definir `WHERE D_E_L_E_T_ = ' '` para garantir que apenas os registros não excluídos sejam trazidos. O script Python não faz essa filtragem magicamente se a query customizada for informada.

4. **Alteração de Estrutura**:
   - O pipeline tem um comportamento de evolução de schema (*Schema Drift*). A função `add_missing_target_columns()` identifica se a origem retornou colunas novas e adiciona essas colunas automaticamente na tabela destino via `ALTER TABLE ADD COLUMN`. Isso reduz muito o overhead de manutenção!
