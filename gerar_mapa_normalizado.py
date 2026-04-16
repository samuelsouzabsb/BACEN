import pandas as pd
import numpy as np
import plotly.express as px
import json
import urllib.request

print("Carregando base de dados...")
caminho = r"C:\Users\samuelbarroso\Downloads\Agencias\Agencias_Consolidado.csv"
df = pd.read_csv(caminho, sep=';', on_bad_lines='skip')
df_min = df.groupby('coluna_grupo')['coluna_valor'].min()
# 1. CORREÇÃO DA UF
df['UF'] = df['UF'].astype(str).str.strip()

# 2. Extrair o Ano e a Data
df['Data'] = df['Arquivo_Origem'].astype(str).str[:7]
df['Ano'] = df['Data'].str[:4]

# 3. CORREÇÃO DA AGREGAÇÃO
max_datas_por_ano = df.groupby('Ano')['Data'].max().reset_index()
df_ultimo_mes_do_ano = df.merge(max_datas_por_ano, on=['Ano', 'Data'])

if 'Quantidade' not in df_ultimo_mes_do_ano.columns:
    df_ultimo_mes_do_ano['Quantidade'] = 1
df_ultimo_mes_do_ano['Quantidade'] = pd.to_numeric(df_ultimo_mes_do_ano['Quantidade'], errors='coerce').fillna(1)
    
df_agg = df_ultimo_mes_do_ano.groupby(['Ano', 'UF'])['Quantidade'].sum().reset_index()
df_agg = df_agg.sort_values(['Ano', 'UF'])

# 4. NORMALIZAÇÃO: Criandos uma Escala Logarítmica
# O log na base 10 comprime valores muito altos (SP em ~8000) e dá destaque às diferenças nas centenas (outros estados)
df_agg['Quantidade_Log'] = np.log10(df_agg['Quantidade'] + 1)

print("Baixando malha geográfica...")
url_geojson = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
with urllib.request.urlopen(url_geojson) as url:
    brazil_geojson = json.loads(url.read().decode())

for feature in brazil_geojson['features']:
    feature['id'] = feature['properties']['sigla']

print("Gerando o mapa animado...")
fig = px.choropleth(
    df_agg,
    locations="UF", 
    geojson=brazil_geojson, 
    color="Quantidade_Log", # Colorindo pela escala normalizada!
    animation_frame="Ano",
    hover_name="UF",
    hover_data={"Quantidade_Log": False, "Quantidade": True}, # Aqui garantimos que ao passar o mouse, ele veja a quantidade real
    color_continuous_scale="Viridis",
    range_color=[df_agg['Quantidade_Log'].min(), df_agg['Quantidade_Log'].max()],
    title="Evolução de Agências por Ano (Visão Normalizada por Escala Logarítmica)"
)

fig.update_geos(fitbounds="locations", visible=False)
fig.update_layout(margin={"r":0,"t":50,"l":0,"b":0})

# Ajustar a barra lateral de cores para ficar fácil de ler os valores reais
fig.update_layout(
    coloraxis_colorbar=dict(
        title="Agências",
        tickvals=[1, 2, 3, 4], # Escala log: 10^1=10, 10^2=100, 10^3=1000, 10^4=10000
        ticktext=["10", "100", "1.000", "10.000"]
    )
)

# Salvar e rodar
saida_html = r"C:\Users\samuelbarroso\Downloads\Agencias\Mapa_Evolucao_Agencias_Anual_Normalizado.html"
fig.write_html(saida_html)
print(f"Salvo em: {saida_html}")
