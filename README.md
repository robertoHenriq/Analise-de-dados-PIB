# Análise de Dados do PIB Municipal

Este projeto implementa um pipeline de ETL para coleta, transformação e armazenamento de dados do Produto Interno Bruto (PIB) por município no Brasil, utilizando os dados da Base dos Dados via Google BigQuery.  
O objetivo é fornecer uma base de dados tratada pronta para análise e visualização.

## Estrutura do Repositório

AnlisePIB/
├── src/
│ ├── ingest/
│ │ └── fetcher.py
│ ├── processing/
│ │ ├── run.py
│ │ ├── processing.py
│ │ ├── clean.py
│ │ ├── metrics.py
│ │ └── logging_utils.py
│ ├── storage/
│ │ └── storage.py
│ ├── queries/
│ │ └── queries.py
│ └── config.py
├── data/
├── requirements.txt
└── README.md


- **src/ingest**: contém lógica para ingestão de dados via bigquery/SQL.  
- **src/processing**: contém o pipeline ETL (extração, transformação, carga), limpeza, métricas e logs.  
- **src/storage**: implementação para salvamento/versionamento de arquivos de dados.  
- **src/queries**: SQLs de consulta utilizados no pipeline.  
- **src/config.py**: arquivo de configuração global (diretório de dados, billing ID etc.).  
- **data/**: saída do pipeline (dados processados e métricas exportadas).  
- **requirements.txt**: lista de bibliotecas Python necessárias.

## Requisitos

- Python 3.8 ou superior  
- Projeto no Google Cloud com BigQuery habilitado e credenciais configuradas  
- Variável de ambiente ou configuração com o `BILLING_ID` (ID do projeto Google Cloud usado para cobrança ou leitura via BigQuery)

## Instalação e uso

### 1. Clonar o repositório  
```bash
git clone https://github.com/robertoHenriq/Analise-de-dados-PIB.git
cd Analise-de-dados-PIB


python -m venv .venv
# No PowerShell:
.\.venv\Scripts\Activate.ps1
# No bash ou macOS:
source .venv/bin/activate


python -m pip install --upgrade pip
pip install -r requirements.txt


# PowerShell
$env:BILLING_ID = "seu-projeto-gcp"
# Bash
export BILLING_ID="seu-projeto-gcp"


python -m src.processing.run


