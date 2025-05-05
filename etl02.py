import pandas as pd

def juntar_arquivos(excel_01, excel_02):
    df1 = pd.read_excel(excel_01)
    df2 = pd.read_excel(excel_02)

    resultado = pd.concat([df1, df2], ignore_index = True)

    resultado.to_excel('resultado.xlsx', index = False)

    print("Os arquivos foram unificados com sucesso!")

#E arquivos com diferentes pastas?
#resultado.to_excel(r"caminho do arquivo de destino\resultado.xlsx")

#para carregar os arquivos a partir de outra pasta, inclua após o print ou resultados:
#excel_01 = r"caminho do excel_01"
#excel_02 = r"caminho do excel_02"

#chamado da função com caminhos absolutos
#juntar_arquivos(excel_01, excel_02)