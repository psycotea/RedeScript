import pandas as pd
import os
from datetime import datetime
import numpy as np
from unidecode import unidecode

diretorio = "pasta_vendas_rede"

arquivo_mapeamento = "mapeamento_maquininhas.csv"

df_mapeamento = pd.read_csv(arquivo_mapeamento, sep=";", encoding="utf-8")

lista_dataframes = []

# Loop para percorrer todos os arquivos no diretório
for arquivo in os.listdir(diretorio):
    if arquivo.endswith(".csv"):
        caminho_arquivo = os.path.join(diretorio, arquivo)
        df = pd.read_csv(caminho_arquivo, sep=";", encoding="utf-8")

        df = df[df["status da venda"].isin(["aprovada", "estornada"])]

        df_selecionado = df[[
            "data da venda", "hora da venda", "valor da venda original", "valor líquido",
            "modalidade", "número de parcelas", "número do cartão", "id carteira digital",
            "código da maquininha"
        ]].copy()

        df_selecionado["número do cartão"].fillna(df_selecionado["id carteira digital"], inplace=True)

        df_selecionado.drop("id carteira digital", axis=1, inplace=True)

        df_selecionado.rename(columns={"valor da venda original": "valor bruto"}, inplace=True)

        lista_dataframes.append(df_selecionado)

# Concatenar todos os DataFrames em um único DataFrame
df_consolidado = pd.concat(lista_dataframes, ignore_index=True)

df_consolidado = pd.merge(df_consolidado, df_mapeamento[["código da maquininha", "nome_da_maquininha", "grupo"]], on="código da maquininha", how="left")

# Função para calcular o valor líquido calculado
def calcular_liquido_calculado(row):
    modalidade = row["modalidade"]
    parcelas = row["número de parcelas"]
    valor_bruto = row["valor bruto"]
    codigo_maquininha = row["código da maquininha"]

    try:
        valor_bruto = float(str(valor_bruto).replace(".", "").replace(",", "."))
    except ValueError:
        return np.nan

    taxa = 0.0

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
        return np.nan
    return valor_bruto * (1 - taxa)

df_consolidado["valor líquido calculado"] = df_consolidado.apply(calcular_liquido_calculado, axis=1)

df_consolidado["valor líquido calculado"] = df_consolidado["valor líquido calculado"].round(2).astype(str).str.replace(".", ",")

colunas = list(df_consolidado.columns)
colunas_sem_liquido_calculado = [coluna for coluna in colunas if coluna != "valor líquido calculado"] # remove a coluna duplicada
colunas_sem_liquido_calculado.insert(colunas_sem_liquido_calculado.index("valor líquido") + 1, "valor líquido calculado")
df_consolidado = df_consolidado[colunas_sem_liquido_calculado]

df_consolidado.columns = [unidecode(coluna) for coluna in df_consolidado.columns]

df_consolidado["modalidade"] = df_consolidado["modalidade"].apply(unidecode)

data_hora_atual = datetime.now().strftime("%Y_%m_%d_%H_%M")
nome_arquivo_consolidado = f"vendas_consolidado_{data_hora_atual}.csv"

df_consolidado.to_csv(nome_arquivo_consolidado, index=False, sep=";", encoding="utf-8")

print(f"Arquivo consolidado gerado com sucesso: {nome_arquivo_consolidado}")