### 🧠 **Desafio de ETL - "Análise de Sentimentos de Feedback de Clientes"**

#**Cenário:**

#Você trabalha em uma empresa que recebe feedbacks de clientes através de um formulário. Esses feedbacks são salvos em um arquivo `.csv` com as seguintes colunas:

#```
#bash
#CopiarEditar
#id, cliente, data_feedback, mensagem

#```

#Seu objetivo é construir um pipeline ETL que:

### 🔧 **ETL - Etapas do Desafio**

### **Extract**

#- Carregue o arquivo `feedbacks.csv`.

### **Transform**

#1. Limpe os dados:
#    - Remova linhas com `mensagem` vazia.
#    - Normalize o texto da mensagem (letras minúsculas, remova pontuação).
#2. Aplique uma **análise de sentimento simples**:
#    - Se a mensagem contém palavras como `ótimo`, `bom`, `excelente`, marque como `positivo`.
#    - Se contém palavras como `ruim`, `péssimo`, `horrível`, marque como `negativo`.
#    - Caso contrário, `neutro`.
#3. Crie uma nova coluna chamada `sentimento` com o resultado.

### **Load**

#- Salve o novo DataFrame em um arquivo `feedbacks_transformados.csv`.

#---

### 💡 Bônus (se quiser ir além):

#- Gere uma contagem de sentimentos por dia.
#- Crie um gráfico de barras com a quantidade de feedbacks positivos, negativos e neutros.

import pandas as pd
import os
import string

arquivo_caminho = r"caminho da pasta onde o arquivo está"

#- Carregue o arquivo `feedbacks.csv`.
df = pd.read_csv(arquivo_caminho, encoding="cp1252")

#1. Limpe os dados:
#    - Remova linhas com `mensagem` vazia.
#    - Normalize o texto da mensagem (letras minúsculas, remova pontuação).
df = df[df['mensagem'].notna() & (df['mensagem'].str.strip() != '')]

def limpar_mensagem(mensagem):
    mensagem = mensagem.lower()
    mensagem = mensagem.translate(str.maketrans('', '', string.punctuation))
    return mensagem

df['mensagem'] = df['mensagem'].apply(limpar_mensagem)

#2. Aplique uma **análise de sentimento simples**:
#    - Se a mensagem contém palavras como `ótimo`, `bom`, `excelente`, marque como `positivo`.
#    - Se contém palavras como `ruim`, `péssimo`, `horrível`, marque como `negativo`.
#    - Caso contrário, `neutro`.

def classificar_sentimento(mensagem):
   palavras_positivas = ['otimo', 'bom', 'excelente']
   palavras_negativas = ['ruim', 'pessimo', 'horrivel']

   if any(palavra in mensagem for palavra in palavras_positivas):
       return 'positivo'
   elif any(palavra in mensagem for palavra in palavras_negativas):
       return 'negativo'
   else:
       return 'neutro'
   
#3. Crie uma nova coluna chamada `sentimento` com o resultado.
   
df['sentimento'] = df['mensagem'].apply(classificar_sentimento)

#- Gere uma contagem de sentimentos por dia.
sentimentos_negativos = 0
sentimentos_positivos = 0
sentimentos_neutros = 0

for sentimento in df['sentimento']:
    if sentimento == 'positivo':
        sentimentos_positivos+=1
    elif sentimento == 'negativo':
        sentimentos_negativos += 1
    else:
        sentimentos_neutros += 1
    
### **Load**

#- Salve o novo DataFrame em um arquivo `feedbacks_transformados.csv`.

output_caminho = os.path.join(os.path.dirname(arquivo_caminho), 'feedbacks_transformados.csv')
df.to_csv(output_caminho, index=False)