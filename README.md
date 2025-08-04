Projeto de Pipeline ETL com Python e FastAPI
Este projeto demonstra a criação de um pipeline de Extração, Transformação e Carregamento (ETL) simples e eficiente, utilizando a biblioteca Pandas para manipulação de dados e o framework FastAPI para construir uma API que serve como fonte de dados.

O objetivo é simular um cenário real onde dados de audição de música são extraídos de uma API, transformados para identificar usuários com um perfil musical específico e, por fim, carregados em um arquivo JSON com recomendações personalizadas.

Tecnologias Utilizadas
Python 3.x: Linguagem de programação principal.

FastAPI: Framework web moderno e de alto desempenho para construir a API.

Pandas: Biblioteca para análise e manipulação de dados em formato de DataFrame.

Uvicorn: Servidor web ASGI para rodar a aplicação FastAPI.

Requests: Biblioteca para fazer requisições HTTP e consumir a API.

Estrutura do Projeto
O projeto é composto por três arquivos principais:

api_dados_musica.py: Contém a lógica da API, que carrega um arquivo dados_audicao.csv e o disponibiliza através da rota /music-data.

pipeline_etl_musica.py: O script principal do pipeline ETL, que consome a API, processa os dados e gera o arquivo de saída.

requirements.txt: Lista todas as bibliotecas necessárias para o projeto, permitindo que qualquer pessoa configure o ambiente facilmente.

Como Configurar e Rodar o Projeto
Siga estes passos para executar o projeto localmente.

1. Pré-requisitos
Certifique-se de que o Python 3.x e o pip (gerenciador de pacotes do Python) estão instalados na sua máquina.

2. Instalação das Dependências
Instale todas as bibliotecas necessárias a partir do arquivo requirements.txt:

pip install -r requirements.txt

3. Executar a API
A API deve estar rodando em um terminal separado antes que o pipeline de ETL possa se conectar a ela.

Abra um terminal e navegue até a pasta do projeto.

Inicie a API usando o Uvicorn com a opção --reload, que reinicia o servidor automaticamente a cada mudança no código.

uvicorn api_dados_musica:app --reload

A API estará disponível em http://127.0.0.1:8000.

4. Executar o Pipeline ETL
Com a API rodando no primeiro terminal, você pode iniciar o pipeline de ETL em um segundo terminal.

Abra um novo terminal e navegue até a pasta do projeto.

Execute o script do pipeline:

python pipeline_etl_musica.py

Ao final da execução, o arquivo recomendacoes_playlist.json será criado na mesma pasta do projeto, contendo as recomendações de playlists para os usuários que atenderam aos critérios definidos.

Descrição do Pipeline
Extração (E): O pipeline usa a biblioteca requests para consumir a rota /music-data da API, extraindo os dados de audição de música em formato JSON.

Transformação (T): Usando o Pandas, os dados são transformados. O pipeline filtra os usuários que ouviram uma lista de artistas alvo (The Beatles, Queen, Michael Jackson) e verifica se o play_count para cada um desses artistas é maior ou igual a um limiar (definido como 70).

Carregamento (L): O resultado da transformação (os usuários qualificados) é usado para gerar mensagens personalizadas. Essas mensagens são então carregadas em um arquivo JSON para uso posterior, como envio de e-mails ou notificações.
