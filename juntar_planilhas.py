import os
import glob

# Path to the folder
folder_path = r"C:\Users\samuelbarroso\Downloads\Dados\SCR data antiga\planilha_2021"
output_file = r"C:\Users\samuelbarroso\Downloads\Dados\SCR data antiga\planilha_2021\planilha_2021_completa.csv"

# Get all csv files
csv_files = glob.glob(os.path.join(folder_path, "planilha_2021*.csv"))
# Ensure we don't include the output file itself if it already exists
csv_files = [f for f in csv_files if f != output_file]

# Sort files to maintain chronological order
csv_files.sort()

print(f"Encontrados {len(csv_files)} arquivos para agrupar.")

with open(output_file, 'wb') as f_out:
    for i, file_path in enumerate(csv_files):
        print(f"Processando {os.path.basename(file_path)} ({i+1}/{len(csv_files)})...")
        with open(file_path, 'rb') as f_in:
            if i != 0:
                # Pula a primeira linha (cabeçalho) dos demais arquivos
                f_in.readline()
            
            # Lê em blocos de 10MB para não encher a memória
            while True:
                chunk = f_in.read(10 * 1024 * 1024)
                if not chunk:
                    break
                f_out.write(chunk)

print(f"\nConcluído! Arquivo final salvo em:\n{output_file}")
