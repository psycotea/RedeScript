import pandas as pd
import os
from datetime import datetime
import numpy as np
from unidecode import unidecode

# Diretório onde os arquivos CSV estão localizados
diretorio = "pasta_vendas_rede"

# Caminho para o arquivo de mapeamento das maquininhas
arquivo_mapeamento = "mapeamento_maquininhas.csv"

# Ler o arquivo de mapeamento das maquininhas
df_mapeamento = pd.read_csv(arquivo_mapeamento, sep=";", encoding="utf-8")

# Lista para armazenar os DataFrames de cada arquivo CSV
lista_dataframes = []

# Loop para percorrer todos os arquivos no diretório
for arquivo in os.listdir(diretorio):
    if arquivo.endswith(".csv"):
        caminho_arquivo = os.path.join(diretorio, arquivo)
        df = pd.read_csv(caminho_arquivo, sep=";", encoding="utf-8")

        # Filtra as vendas com status "aprovada" ou "estornada"
        df = df[df["status da venda"].isin(["aprovada", "estornada"])]

        # Seleciona as colunas relevantes
        df_selecionado = df[[
            "data da venda", "hora da venda", "valor da venda original", "valor líquido",
            "modalidade", "número de parcelas", "número do cartão", "id carteira digital",
            "código da maquininha"
        ]].copy()

        # Preenche os valores NaN da coluna "número do cartão" com os valores da coluna "id carteira digital"
        df_selecionado["número do cartão"].fillna(df_selecionado["id carteira digital"], inplace=True)

        # Remove a coluna "id carteira digital"
        df_selecionado.drop("id carteira digital", axis=1, inplace=True)

        # Renomeia a coluna "valor da venda original" para "valor bruto"
        df_selecionado.rename(columns={"valor da venda original": "valor bruto"}, inplace=True)

        lista_dataframes.append(df_selecionado)

# Concatenar todos os DataFrames em um único DataFrame
df_consolidado = pd.concat(lista_dataframes, ignore_index=True)

# Mesclar os DataFrames usando a coluna "código da maquininha" como chave
df_consolidado = pd.merge(df_consolidado, df_mapeamento[["código da maquininha", "nome_da_maquininha", "grupo"]], on="código da maquininha", how="left")

# Função para calcular o valor líquido calculado
def calcular_liquido_calculado(row):
    modalidade = row["modalidade"]
    parcelas = row["número de parcelas"]
    valor_bruto = row["valor bruto"]
    codigo_maquininha = row["código da maquininha"]

    try:
        # Remove os pontos de separação de milhar antes de converter para float
        valor_bruto = float(str(valor_bruto).replace(".", "").replace(",", "."))
    except ValueError:
        return np.nan  # Retorna NaN se o valor bruto não puder ser convertido para float

    taxa = 0.0  # Inicializa a taxa como um float padrão

    try:
        if modalidade == "débito":
            taxa = float(df_mapeamento.loc[df_mapeamento["código da maquininha"] == codigo_maquininha, "débito"].values[0])
        elif modalidade == "pré-pago débito":
            taxa = float(df_mapeamento.loc[df_mapeamento["código da maquininha"] == codigo_maquininha, "pré-pago débito"].values[0])
        elif modalidade == "crédito" and parcelas == 1:
            taxa = float(df_mapeamento.loc[df_mapeamento["código da maquininha"] == codigo_maquininha, "crédito"].values[0])
        elif modalidade == "pré-pago crédito":
            taxa = float(df_mapeamento.loc[df_mapeamento["código da maquininha"] == codigo_maquininha, "pré-pago crédito"].values[0])
        elif modalidade == "crédito" and parcelas > 1:
            taxa_coluna = f"taxa_credito_parcelado_{parcelas}"
            if taxa_coluna in df_mapeamento.columns:
                taxa = float(df_mapeamento.loc[df_mapeamento["código da maquininha"] == codigo_maquininha, taxa_coluna].values[0])
    except (ValueError, IndexError, TypeError):
        return np.nan  # Retorna NaN se houver algum erro na busca da taxa

    return valor_bruto * (1 - taxa)

# Aplicar a função para calcular o valor líquido calculado
df_consolidado["valor líquido calculado"] = df_consolidado.apply(calcular_liquido_calculado, axis=1)

# Arredondar e formatar a coluna "valor líquido calculado"
df_consolidado["valor líquido calculado"] = df_consolidado["valor líquido calculado"].round(2).astype(str).str.replace(".", ",")

# Inserir a coluna "valor líquido calculado" após a coluna "valor líquido"
colunas = list(df_consolidado.columns)
colunas_sem_liquido_calculado = [coluna for coluna in colunas if coluna != "valor líquido calculado"] # remove a coluna duplicada
colunas_sem_liquido_calculado.insert(colunas_sem_liquido_calculado.index("valor líquido") + 1, "valor líquido calculado")
df_consolidado = df_consolidado[colunas_sem_liquido_calculado]

# Remover acentos dos nomes das colunas
df_consolidado.columns = [unidecode(coluna) for coluna in df_consolidado.columns]

# Remover acentos dos valores da coluna "modalidade"
df_consolidado["modalidade"] = df_consolidado["modalidade"].apply(unidecode)

# Gerar o nome do arquivo com a data e hora atual
data_hora_atual = datetime.now().strftime("%Y_%m_%d_%H_%M")
nome_arquivo_consolidado = f"vendas_consolidado_{data_hora_atual}.csv"

# Salvar o DataFrame consolidado em um novo arquivo CSV
df_consolidado.to_csv(nome_arquivo_consolidado, index=False, sep=";", encoding="utf-8")

print(f"Arquivo consolidado gerado com sucesso: {nome_arquivo_consolidado}")