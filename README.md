# Processamento de clickstream

Sessionamento de eventos clickstream com o mínimo de dependências externas(apenas numpy).

## Uso

1. Colocar os arquivos a serem pocessados no diretório `data`
2. Criar um novo venv e instalar as dependências com `pip install -r requirements.txt`
3. Executar o script principal na raiz do projeto: `python src/main.py`

## Dependências

* Python 3.6+
* numpy

## Estrutura da solução

### Fluxo dos dados

Para evitar problemas de memória ao carregar um arquivo JSON inteiro na memória para sessionamento,
o conteúdo dos arquivos é primeiro carregado em um DB SQLite intermediário.

Dali os eventos correspondentes a cada conjunto de colunas usadas para determinar uma sessão 
são carregados e sessionados usando um algoritmo baseado em busca binária.

As sessões são então carregadas em outro DB SQLite para que possam ser realizadas consultas sobre elas.

### Uso do SQLite

Embora não seja uma solução altamente escalável, o SQLite é plenamente capaz de lidar com dados de algumas
dezenas de GB e possui um módulo de acesso na biblioteca padrão do Python, sendo assim de fácil uso na
prototipação de projetos.

Os principais cuidado tomados para manter a performance da aplicação nesse sentido foram:

* Inserts realizados em lote, com até 250k registros por transação.
* Uso de índices para agilizar as consultas posteriores.
* Criação dos índices apenas após a inserção dos dados, para evitar que o banco de dados perca tempo atualizando-os
 durante o processo de carga.

## Melhorias possíveis

* Uso de uma camada ORM(como SQLAlchemy) para facilitar a manutenção do código e permitir migrar do SQLite para bancos de dados mais robustos de forma transparente.
* Refatoração das classes para aderir melhor ao princípio da responsabilidade única.