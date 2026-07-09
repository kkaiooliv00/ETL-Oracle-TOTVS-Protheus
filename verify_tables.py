import yaml

# Carregar o yaml
with open('oracle_jobs.yml', 'r', encoding='utf-8') as f:
    jobs = yaml.safe_load(f)

# Carregar a lista de tabelas
with open(r'c:\Users\KaioOliveira\OneDrive - grupolos.com.br\Documentos\oracle_etl\tables.txt', 'r', encoding='utf-8') as f:
    tables = {line.strip() for line in f}

print("Iniciando verificação de tabelas...")
missing = []
for job in jobs:
    tb = job['oracle_table']
    if tb not in tables:
        missing.append(tb)

if not missing:
    print("Sucesso! Todas as tabelas do YAML existem no Oracle.")
else:
    print(f"ATENÇÃO! {len(missing)} tabelas no YAML não foram encontradas no Oracle:")
    for m in missing:
        print(f" - {m}")
