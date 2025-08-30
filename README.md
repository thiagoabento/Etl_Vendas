# ETL Vendas

Este projeto realiza um processo de **ETL (Extract, Transform, Load)** utilizando Python, Pandas, um arquivo CSV de entrada e um banco de dados SQLite. O objetivo é extrair dados de vendas, realizar transformações e carregar em tabelas de banco de dados para análises futuras.

Tecnologias utilizadas

- **Python 3.8+**
- **Pandas**: manipulação e transformação de dados
- **SQLite**: banco de dados local
- **CSV**: formato de dados de entrada

```text
etl_vendas/
├── data/
│   └── vendas.csv # Arquivo de leitura
├── database/
│   └── ecommerce.db # Banco SQLite gerado
├── scripts/
│   └── etl.py # Arquivos de leitura, transformação e carregamento na base de dados
├── README.md
└── requirements.txt # Dependências do projeto
