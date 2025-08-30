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
│   └── vendas.db # Banco SQLite gerado
├── etl/
│   └── etl.py # Arquivos de leitura, transformação e carregamento na base de dados
├── log/
│   └── job_etl.log # Arquivos de log do processo etl
├── README.md
└── requirements.txt # Dependências do projeto
