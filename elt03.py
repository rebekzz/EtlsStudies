""" Exercício: ETL para Processamento de Vendas
Cenário: Você tem três arquivos de dados sobre vendas e precisa criar um processo ETL para consolidar esses dados em um único arquivo, já tratadinho e pronto para análise. O processo deve ser feito em três etapas: extração, transformação e carregamento.

Arquivos:

vendas_janeiro.csv: Dados de vendas de janeiro.

vendas_fevereiro.csv: Dados de vendas de fevereiro.

vendas_marco.csv: Dados de vendas de março.

Estrutura dos arquivos:

Coluna id_venda: Identificador único da venda.

Coluna produto: Nome do produto.

Coluna quantidade: Quantidade de produtos vendidos.

Coluna preco_unitario: Preço unitário do produto.

Coluna data_venda: Data da venda.

Objetivo:
Extração: Carregar os três arquivos CSV.

Transformação:

Unificar os dados em um único DataFrame.

Criar uma coluna total_venda (quantidade * preço_unitario).

Converter a coluna data_venda para o formato de data.

Filtrar as vendas que ocorreram no primeiro trimestre de 2025.

Carregamento: Salvar o DataFrame final em um novo arquivo CSV chamado vendas_1tri_2025.csv.

Passos:
Extração:

Use pandas.read_csv() para carregar os dados.

Transformação:

Utilize operações do pandas para unir os dados (concat() ou merge()).

Crie a coluna total_venda.

Aplique a transformação da data e filtre o período correto.

Carregamento:

Salve o DataFrame final usando to_csv(). """

import pandas as pd
import os

def juntar_arquivos(caminho1, caminho2, caminho3):
    # Função segura para carregar CSV
    def carregar_csv(caminho):
        if os.path.exists(caminho):
            return pd.read_csv(caminho)
        else:
            print(f"Arquivo não encontrado: {caminho}")
            return pd.DataFrame(columns=['id_venda', 'produto', 'quantidade', 'preco_unitario', 'data_venda'])

    # Extração
    df1 = carregar_csv(caminho1)
    df2 = carregar_csv(caminho2)
    df3 = carregar_csv(caminho3)

    # Transformação
    df_total = pd.concat([df1, df2, df3], ignore_index=True)
    df_total['total_venda'] = df_total['quantidade'] * df_total['preco_unitario']
    df_total['data_venda'] = pd.to_datetime(df_total['data_venda'], format='%Y-%m-%d')

    # Filtrar vendas do 1º trimestre de 2025
    df_filtrado = df_total[
        (df_total['data_venda'] >= '2025-01-01') &
        (df_total['data_venda'] <= '2025-03-31')
    ]

    # Carregamento
    df_filtrado.to_csv('vendas_1tri_2025.csv', index=False)

    return df_filtrado

# Exemplo de uso
caminho_pasta = r"#insira o caminho da pasta aqui"
arquivo_jan = os.path.join(caminho_pasta, "vendas_janeiro.csv")
arquivo_fev = os.path.join(caminho_pasta, "vendas_fevereiro.csv")
arquivo_mar = os.path.join(caminho_pasta, "vendas_marco.csv")

df_resultado = juntar_arquivos(arquivo_jan, arquivo_fev, arquivo_mar)


