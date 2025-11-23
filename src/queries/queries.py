# src/queries/queries.py

PIB_MUNICIPIO_QUERY = """
SELECT
  dados.id_municipio AS id_municipio,
  diretorio_id_municipio.nome AS id_municipio_nome,
  dados.ano AS ano,
  dados.pib AS pib,
  dados.impostos_liquidos AS impostos_liquidos,
  dados.va AS va,
  dados.va_agropecuaria AS va_agropecuaria,
  dados.va_industria AS va_industria,
  dados.va_servicos AS va_servicos,
  dados.va_adespss AS va_adespss
FROM `basedosdados.br_ibge_pib.municipio` AS dados
LEFT JOIN (
  SELECT DISTINCT id_municipio, nome
  FROM `basedosdados.br_bd_diretorios_brasil.municipio`
) AS diretorio_id_municipio
  ON dados.id_municipio = diretorio_id_municipio.id_municipio
"""
