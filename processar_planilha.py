import pandas as pd
import gc
import os
import glob
import sqlite3

folder_path = r"C:\Users\samuelbarroso\Downloads\Dados\SCR data antiga\planilha_2019"
output_file = r"C:\Users\samuelbarroso\Downloads\Dados\SCR data antiga\planilha_2019\planilha_2019_agrupada_unpivot.csv"
sqlite_db = os.path.join(folder_path, "agg_temp.sqlite")

# Remove banco de dados antigo se existir em execuções passadas
if os.path.exists(sqlite_db):
    os.remove(sqlite_db)

conn = sqlite3.connect(sqlite_db)

files = glob.glob(os.path.join(folder_path, "planilha_2019*.csv"))
files = [f for f in files if "completa" not in f and "agrupada" not in f]
files.sort()

value_vars = [
    "numero_de_operacoes",
    "a_vencer_ate_90_dias",
    "a_vencer_de_91_ate_360_dias",
    "a_vencer_de_361_ate_1080_dias",
    "a_vencer_de_1081_ate_1800_dias",
    "a_vencer_de_1801_ate_5400_dias",
    "a_vencer_acima_de_5400_dias",
    "vencido_acima_de_15_dias",
    "carteira_ativa",
    "carteira_inadimplida_arrastada",
    "ativo_problematico"
]

id_vars = [
    "data_base", "uf", "tcb", "sr", "cliente", "ocupacao", 
    "cnae_secao", "cnae_subclasse", "porte", "modalidade", "origem", "indexador"
]

chunksize = 1_000_000

print(f"Iremos processar {len(files)} arquivos, usando um Banco SQLite temporário para poupar a memória RAM.")

for file_idx, filepath in enumerate(files):
    print(f"\n[{file_idx+1}/{len(files)}] Lendo {os.path.basename(filepath)}...")
    
    chunks = pd.read_csv(filepath, sep=";", encoding="ISO-8859-1", chunksize=chunksize, dtype=str)
    
    for i, chunk in enumerate(chunks):
        print(f"  -> Processando bloco {i+1}...")
        
        chunk.rename(columns=lambda x: x.replace('ï»¿', '').replace('\ufeff', '').strip(), inplace=True)
        
        for col in id_vars:
            if col in chunk.columns:
                chunk[col] = chunk[col].str.strip()
        
        if 'numero_de_operacoes' in chunk.columns:
            chunk['numero_de_operacoes'] = chunk['numero_de_operacoes'].astype(str).str.replace('<= 15', '7').str.replace('<=15', '7').str.strip()
            chunk['numero_de_operacoes'] = pd.to_numeric(chunk['numero_de_operacoes'], errors='coerce').fillna(0).astype('float64')

        for col in value_vars:
            if col != 'numero_de_operacoes' and col in chunk.columns:
                chunk[col] = chunk[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce').fillna(0.0).astype('float64')

        cols_to_melt = [c for c in value_vars if c in chunk.columns]
        
        melted = chunk.melt(id_vars=id_vars, value_vars=cols_to_melt, var_name='Atributo', value_name='Valor')
        
        grouped = melted.groupby(id_vars + ['Atributo'], as_index=False)['Valor'].sum()
        
        # O SEGOC: JOGA O RESULTADO NO BANCO EM VEZ DA RAM
        grouped.to_sql("tb_temp", conn, if_exists="append", index=False)
        
        del chunk, melted, grouped
        gc.collect()

print("\nTodos os arquivos foram inseridos no BD temporário.")
print("Iniciando a agregação final via SQL (isso pode levar alguns minutos)...")

query = f"""
    SELECT
        {', '.join(id_vars)},
        Atributo,
        SUM(Valor) as Valor
    FROM tb_temp
    GROUP BY {', '.join(id_vars)}, Atributo
"""

# Exportamos em blocos direto do banco para o CSV
output_chunks = pd.read_sql_query(query, conn, chunksize=1_000_000)

first_chunk = True
for output_idx, df_out in enumerate(output_chunks):
    print(f"  -> Salvando bloco final {output_idx+1} do CSV...")
    if first_chunk:
        df_out.to_csv(output_file, mode='w', sep=';', encoding='ISO-8859-1', index=False)
        first_chunk = False
    else:
        df_out.to_csv(output_file, mode='a', sep=';', encoding='ISO-8859-1', index=False, header=False)


# Limpeza Final (Deleta o DB gigante após terminar)
conn.close()
if os.path.exists(sqlite_db):
    os.remove(sqlite_db)

print("\nFinalizado!\nO processamento foi inteiro concluído usando Banco de Dados (sem consumir sua RAM extra).")
print(f"Arquivo CSV final salvo com sucesso em: {output_file}")
