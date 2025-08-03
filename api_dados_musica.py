# Importações necessárias para o FastAPI, pandas e para lidar com arquivos
from fastapi import FastAPI, HTTPException
import pandas as pd
import os

# Cria uma instância do FastAPI com metadados para documentação
app = FastAPI(
    title="API de Dados de Música para ETL",
    description="Uma API simples para fornecer dados de audição de usuários a um pipeline ETL.",
)

# Nome do arquivo CSV de onde a API vai ler os dados
CSV_FILE_NAME = 'dados_audicao.csv'


def load_data_from_csv():
    """
    Carrega os dados do CSV. Inclui uma lógica para criar o CSV
    se ele não existir, garantindo que a API sempre tenha dados para servir.
    """
    if not os.path.exists(CSV_FILE_NAME):
        print(
            f"Arquivo '{CSV_FILE_NAME}' não encontrado para a API. Criando arquivo CSV...")
        data = {
            'user_id': [101, 101, 101, 102, 102, 102, 103, 103, 103, 104, 104, 104, 105, 105, 105, 106, 106, 106, 107, 107, 107, 108, 108, 108, 109, 109, 109, 110, 110, 110],
            'user_name': ['Ana Silva', 'Ana Silva', 'Ana Silva', 'Bruno Costa', 'Bruno Costa', 'Bruno Costa',
                          'Carla Dias', 'Carla Dias', 'Carla Dias', 'Daniel Alves', 'Daniel Alves', 'Daniel Alves',
                          'Eduarda Lima', 'Eduarda Lima', 'Eduarda Lima', 'Fernando Souza', 'Fernando Souza', 'Fernando Souza',
                          'Gabriela Mendes', 'Gabriela Mendes', 'Gabriela Mendes', 'Heloísa Neves', 'Heloísa Neves', 'Heloísa Neves',
                          'Igor Rocha', 'Igor Rocha', 'Igor Rocha', 'Julia Santos', 'Julia Santos', 'Julia Santos'],
            'artist_name': ['The Beatles', 'Queen', 'Michael Jackson', 'The Beatles', 'Queen', 'Coldplay',
                            'The Beatles', 'Queen', 'Michael Jackson', 'The Beatles', 'Adele', 'Ed Sheeran',
                            'The Beatles', 'Queen', 'Michael Jackson', 'Justin Bieber', 'Taylor Swift', 'Dua Lipa',
                            'The Beatles', 'Queen', 'Michael Jackson', 'The Beatles', 'Queen', 'Coldplay',
                            'The Beatles', 'Queen', 'Michael Jackson', 'Justin Bieber', 'Taylor Swift', 'Dua Lipa'],
            'play_count': [150, 120, 100,
                           160, 130, 40,
                           80, 70, 60,
                           200, 20, 10,
                           180, 150, 130,
                           30, 25, 20,
                           90, 85, 80,
                           110, 95, 30,
                           75, 65, 55,
                           40, 35, 30]
        }
        df = pd.DataFrame(data)
        df.to_csv(CSV_FILE_NAME, index=False)
        print(
            f"Arquivo '{CSV_FILE_NAME}' criado com sucesso. Usando o arquivo existente para a API.")
    else:
        print(
            f"Arquivo '{CSV_FILE_NAME}' encontrado. Usando o arquivo existente para a API.")

    try:
        df = pd.read_csv(CSV_FILE_NAME)
        return df
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao carregar os dados do CSV: {e}")


# IMPORTANT: Esta linha agora está no escopo global, fora de qualquer função
# Carrega os dados uma única vez quando a API é iniciada
df_audicao = load_data_from_csv()

# Rota principal para a documentação da API


@app.get("/")
async def read_root():
    return {"message": "Bem-vindo à API de Dados de Música! Acesse /music-data para obter os dados."}


# Rota para obter todos os dados de audição de música
@app.get("/music-data")
async def get_music_data():
    """ 
    Retorna todos os dados de audição de música disponíveis.
    """
    return df_audicao.to_dict(orient='records')
