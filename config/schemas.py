# -------- CONFIGURAÇÕES DE SCHEMAS --------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# -------- Schemas Bronze --------
BRONZE_SCHEMA_EMPRESAS = StructType([
    StructField("cnpj", StringType(), True, {"comment": "CONTÉM O NÚMERO DE INSCRIÇÃO NO CNPJ (CADASTRO NACIONAL DA PESSOA JURÍDICA)."}),
    StructField("razao_social", StringType(), True, {"comment": "CORRESPONDE AO NOME EMPRESARIAL DA PESSOA JURÍDICA."}),
    StructField("natureza_juridica", StringType(), True, {"comment": "CÓDIGO DA NATUREZA JURÍDICA."}),
    StructField("qualificacao_responsavel", StringType(), True, {"comment": "QUALIFICAÇÃO DA PESSOA FÍSICA RESPONSÁVEL PELA EMPRESA."}),
    StructField("capital_social", StringType(), True, {"comment": "CAPITAL SOCIAL DA EMPRESA."}),
    StructField("cod_porte", StringType(), True, {"comment": "CÓDIGO DO PORTE DA EMPRESA."})
])

BRONZE_SCHEMA_SOCIOS = StructType([
    StructField("cnpj", StringType(), True, {"comment": "CONTÉM O NÚMERO DE INSCRIÇÃO NO CNPJ (CADASTRO NACIONAL DA PESSOA JURÍDICA)."}),
    StructField("tipo_socio", StringType(), True, {"comment": "IDENTIFICADOR DE SÓCIO."}),
    StructField("nome_socio", StringType(), True, {"comment": "CORRESPONDE AO NOME DO SÓCIO PESSOA FÍSICA, RAZÃO SOCIAL E/OU NOME EMPRESARIAL DA PESSOA JURÍDICA E NOME DO SÓCIO/RAZÃO SOCIAL DO SÓCIO ESTRANGEIRO."}),
    StructField("documento_socio", StringType(), True, {"comment": "É PREENCHIDO COM CPF OU CNPJ DO SÓCIO; NO CASO DE SÓCIO ESTRANGEIRO, É PREENCHIDO COM 'NOVES'. O ALINHAMENTO PARA CPF É FORMATADO COM ZEROS À ESQUERDA."}),
    StructField("codigo_qualificacao_socio", StringType(), True, {"comment": "CÓDIGO DA QUALIFICAÇÃO DO SÓCIO."}),
])


# -------- Schemas Silver --------
SILVER_SCHEMA_EMPRESAS = StructType([
    StructField("cnpj", StringType(), True, {"comment": "CONTÉM O NÚMERO DE INSCRIÇÃO NO CNPJ (CADASTRO NACIONAL DA PESSOA JURÍDICA)."}),
    StructField("razao_social", StringType(), True, {"comment": "CORRESPONDE AO NOME EMPRESARIAL DA PESSOA JURÍDICA."}),
    StructField("natureza_juridica", IntegerType(), True, {"comment": "CÓDIGO DA NATUREZA JURÍDICA."}),
    StructField("qualificacao_responsavel", IntegerType(), True, {"comment": "QUALIFICAÇÃO DA PESSOA FÍSICA RESPONSÁVEL PELA EMPRESA."}),
    StructField("capital_social", DoubleType(), True, {"comment": "CAPITAL SOCIAL DA EMPRESA."}),
    StructField("cod_porte", StringType(), True, {"comment": "CÓDIGO DO PORTE DA EMPRESA."})
])

SILVER_SCHEMA_SOCIOS = StructType([
    StructField("cnpj", StringType(), True, {"comment": "CONTÉM O NÚMERO DE INSCRIÇÃO NO CNPJ (CADASTRO NACIONAL DA PESSOA JURÍDICA)."}),
    StructField("tipo_socio", IntegerType(), True, {"comment": "IDENTIFICADOR DE SÓCIO."}),
    StructField("nome_socio", StringType(), True, {"comment": "CORRESPONDE AO NOME DO SÓCIO PESSOA FÍSICA, RAZÃO SOCIAL E/OU NOME EMPRESARIAL DA PESSOA JURÍDICA E NOME DO SÓCIO/RAZÃO SOCIAL DO SÓCIO ESTRANGEIRO."}),
    StructField("documento_socio", StringType(), True, {"comment": "É PREENCHIDO COM CPF OU CNPJ DO SÓCIO; NO CASO DE SÓCIO ESTRANGEIRO, É PREENCHIDO COM 'NOVES'. O ALINHAMENTO PARA CPF É FORMATADO COM ZEROS À ESQUERDA."}),
    StructField("codigo_qualificacao_socio", StringType(), True, {"comment": "CÓDIGO DA QUALIFICAÇÃO DO SÓCIO."})
])