import pandas as pd
import json
import requests
import os

# Vamos fazer o primeiro passo que é a extração dos dados


def extrair_dados_audicao_api(api_url):
    """
    Extrai os dados de audição de música da API.
    Retorna um DataFrame do Pandas.         
    """
    print(f"Extraindo dados da API: {api_url}")
    try:
        response = requests.get(api_url)
        # Lança um erro para status de resposta HTTP ruins (4xx ou 5xx)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        print("Dados extraídos da API com sucesso.")
        print(df.head(10))
        print("-" * 50)
        return df
    except requests.exceptions.ConnectionError:
        print(
            f"Erro de conexão. Verifique se de que a API está rodando em {api_url}.")
        return pd.DataFrame()

# Vamos para o segundo passo que é a transformação dos dados


def transformar_dados(df_audicao, artistas_alvo, limiar_play_count):
    print("Iniciando a Etapa de Transformação dos Dados ---")

    if df_audicao.empty:
        print("DataFrame de entrada vazio. Pulando transformação.")
        return []

    # filtrando os dados para incluir apenas os artistas alvo
    df_filtrando_artistas = df_audicao[df_audicao['artist_name'].isin(
        artistas_alvo)].copy()
    print(
        "\nDados filtrado para incluir apenas os artistas alvo {', '.join(artistas_alvo)}:")
    print(df_filtrando_artistas.head())

    # Agrupar por user_id, user_name e artist_name, somando o play_count
    df_agrupado_por_usuario_artista = df_filtrando_artistas.groupby(
        ['user_id', 'user_name', 'artist_name'])['play_count'].sum().reset_index()
    print(df_agrupado_por_usuario_artista)

    # Pivotar para ter artistas como colunas e verificar o limiar
    df_pivotado = df_agrupado_por_usuario_artista.pivot_table(
        index=['user_id', 'user_name'], columns='artist_name', values='play_count', fill_value=0)
    print("\nDados pivotados por usuário (reproduções de artistas alvo):")
    print(df_pivotado)

    # Criar uma série de condições para cada artista alvo
    condicoes_por_artista = []
    for artist in artistas_alvo:
        if artist in df_pivotado.columns:
            condicoes_por_artista.append(
                df_pivotado[artist] >= limiar_play_count)
        else:
            condicoes_por_artista.append(
                pd.Series(False, index=df_pivotado.index))

    # Identificar usuários que atendem ao critério para TODOS os artistas
    if condicoes_por_artista:
        usuarios_qualificados_df = df_pivotado[pd.concat(
            condicoes_por_artista, axis=1).all(axis=1)]
    else:
        usuarios_qualificados_df = pd.DataFrame()

    usuarios_qualificados_com_nomes = [
        {'user_id': idx[0], 'user_name': idx[1]}
        for idx in usuarios_qualificados_df.index
    ]

    print(
        f"\nUsuários qualificados (ouviram >= {limiar_play_count} vezes CADA um dos artistas):")
    for user_info in usuarios_qualificados_com_nomes:
        print(
            f"  - ID: {user_info['user_id']}, Nome: {user_info['user_name']}")
    print("-" * 50)
    return usuarios_qualificados_com_nomes


def gerar_mensagens_personalizadas(usuarios_qualificados, artistas_alvo):
    mensagens = {}
    if len(artistas_alvo) == 3:
        artistas_str = f"{artistas_alvo[0]}, {artistas_alvo[1]} e {artistas_alvo[2]}"
    else:
        artistas_str = ', '.join(
            artistas_alvo[:-1]) + ' e ' + artistas_alvo[-1] if len(artistas_alvo) > 1 else artistas_alvo[0]

    for user_info in usuarios_qualificados:
        user_id = user_info['user_id']
        user_name = user_info['user_name']
        mensagem = (f"Olá {user_name}! Percebemos que você tem um gosto musical "
                    f"incrível e ouve bastante {artistas_str}. Pensando em você, "
                    f"criamos uma playlist exclusiva que reúne o melhor desses três artistas! "
                    f"Esperamos que goste e descubra novas sonoridades!")
        mensagens[user_id] = {'user_name': user_name, 'message': mensagem}
    return mensagens


# --- 3. Carregamento dos Resultados ---
def carregar_resultados(mensagens_personalizadas, output_file='recomendacoes_playlist.json', min_usuarios_para_recomendar=5):
    print("--- Iniciando a Etapa de Carregamento ---")

    if len(mensagens_personalizadas) >= min_usuarios_para_recomendar:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(mensagens_personalizadas, f,
                      indent=4, ensure_ascii=False)
        print(
            f"\nRecomendações salvas com sucesso em '{output_file}' para {len(mensagens_personalizadas)} usuários qualificados.")
    else:
        print(
            f"\nNúmero de usuários qualificados ({len(mensagens_personalizadas)}) é menor que o mínimo exigido ({min_usuarios_para_recomendar}).")
        print("Nenhuma playlist personalizada será gerada neste momento.")

    print("-" * 50)


# --- Montando o Pipeline Principal ---
def pipeline_etl_musica_projeto(api_base_url, artistas_alvo, limiar_play_count, min_usuarios_para_recomendar):
    """
    Executa o pipeline completo de simulação ETL para o projeto do curso,
    consumindo dados de uma API.
    """
    print("=" * 60)
    print("         Iniciando o Pipeline ETL de Recomendações de Música         ")
    print("=" * 60)

    # 1. Extração de Dados da API
    df_dados_audicao = extrair_dados_audicao_api(f"{api_base_url}/music-data")

    if df_dados_audicao.empty:
        print("Não foi possível carregar os dados da API. Encerrando o pipeline.")
        return

    # 2. Transformação dos Dados
    usuarios_qualificados = transformar_dados(
        df_dados_audicao, artistas_alvo, limiar_play_count)

    # Verificar a condição de 5 usuários ou mais para gerar as recomendações
    if len(usuarios_qualificados) >= min_usuarios_para_recomendar:
        print(
            f"\nSucesso! {len(usuarios_qualificados)} usuários atenderam aos critérios. Gerando mensagens personalizadas...")
        mensagens_personalizadas = gerar_mensagens_personalizadas(
            usuarios_qualificados, artistas_alvo)
        for user_id, user_data in mensagens_personalizadas.items():
            print(
                f"\n--- Mensagem para {user_data['user_name']} (ID: {user_id}) ---")
            print(user_data['message'])
            print("-" * 30)
    else:
        print(f"\nInfelizmente, apenas {len(usuarios_qualificados)} usuários qualificados. "
              f"Não atingiu o mínimo de {min_usuarios_para_recomendar} para gerar as recomendações.")
        mensagens_personalizadas = {}

    # 3. Carregamento dos Resultados
    carregar_resultados(mensagens_personalizadas,
                        min_usuarios_para_recomendar=min_usuarios_para_recomendar)

    print("\n" + "=" * 60)
    print("         Pipeline ETL Concluído para o Projeto do Curso              ")
    print("=" * 60)


# --- Configurações do Projeto ---
API_BASE_URL = "http://127.0.0.1:8000"
MEUS_ARTISTAS_ALVO = ['The Beatles', 'Queen', 'Michael Jackson']
MEU_LIMIAR_PLAY_COUNT = 70
MINIMO_USUARIOS_PARA_RECOMENDAR = 5

# --- Executar o Pipeline ---
if __name__ == "__main__":
    pipeline_etl_musica_projeto(
        API_BASE_URL,
        MEUS_ARTISTAS_ALVO,
        MEU_LIMIAR_PLAY_COUNT,
        MINIMO_USUARIOS_PARA_RECOMENDAR
    )
