import pandas as pd
from sqlalchemy import create_engine
import os

DB_URL = "postgresql://hanna:password123@localhost:5433/antaq_dw"
engine = create_engine(DB_URL)

def ingest_csv(file_path, table_name):
    print(f"🚀 Iniciando ingestão do arquivo: {file_path}")
    
    # O CSV da ANTAQ geralmente usa ';' como separador e encoding 'utf-8'
    # Lemos em chunks (pedaços) se o arquivo for muito grande, ou direto se for médio
    try:
        df = pd.read_csv(file_path, sep=';', encoding='utf-8', dtype=str) # Lê tudo como string para evitar problemas de tipo
        
        # O 'if_exists=replace' garante que ele crie a tabela na primeira vez
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"✅ Tabela '{table_name}' criada com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao processar {file_path}: {e}")

if __name__ == "__main__":
    # Ajuste o caminho para onde você baixou os arquivos da ANTAQ
    # Exemplo:
    ingest_csv('data/2025Atracacao.txt', 'raw_atracacao')
    ingest_csv('data/2025Carga.txt', 'raw_carga')