import psycopg

db_url = 'postgresql://postgres:Grupolos202601@db.yxkffqzozzlrhopfrjzy.supabase.co:6543/postgres'

try:
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            """)
            tables = cur.fetchall()
            schemas = {}
            for schema, table in tables:
                if schema not in schemas:
                    schemas[schema] = []
                schemas[schema].append(table)
            
            for schema, tbls in schemas.items():
                print(f'Schema {schema}: {len(tbls)} tables')
                print(', '.join(tbls))
except Exception as e:
    print(f'Error: {e}')
