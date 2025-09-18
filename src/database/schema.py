import polars as pl
from typing import Dict, List, Tuple

# Mapeamento de tipos Polars para PostgreSQL
POLARS_TO_POSTGRES_TYPES = {
    pl.Int8: "SMALLINT",
    pl.Int16: "SMALLINT",
    pl.Int32: "INTEGER",
    pl.Int64: "BIGINT",
    pl.UInt8: "SMALLINT",
    pl.UInt16: "INTEGER",
    pl.UInt32: "BIGINT",
    pl.UInt64: "BIGINT",
    pl.Float32: "REAL",
    pl.Int32: "DOUBLE PRECISION",
    pl.Boolean: "BOOLEAN",
    pl.String: "TEXT",
    pl.Date: "DATE",
    pl.Datetime: "TIMESTAMP",
}

TABLE_SCHEMAS: Dict[str, Dict[str, any]] = {
    "internacoes": {
        "table_name": "internacoes",
        "columns": {
            "CNES": pl.String,
            "N_AIH": pl.String,
            "ESPEC": pl.String,
            "IDENT": pl.String,
            "DT_INTER": pl.Date,
            "DT_SAIDA": pl.Date,
            "DIAS_PERM":pl.Int16,
            "VAL_SH": pl.Int32,
            "VAL_SP": pl.Int32,
            "VAL_TOT": pl.Int32,
            "COMPLEX": pl.String,
            "MUNIC_MOV": pl.String,
            "DIAG_PRINC": pl.String,
            "CID_ASSO": pl.String,
            "NASC": pl.Date,
            "SEXO": pl.Int8,
            "IDADE": pl.Int16,
            "NACIONAL": pl.Int16,
            "NUM_FILHOS": pl.Int8,
            "RACA_COR": pl.String,
            "MUNIC_RES": pl.String,
            "CEP": pl.String,
        },
        "primary_key": ["N_AIH"],
        "foreign_keys": [
            {"column": "CNES", "references_table": "hospital", "references_column": "CNES"},
            {"column": "MUNIC_RES", "references_table": "municipios", "references_column": "codigo_6d"},
            {"column": "MUNIC_MOV", "references_table": "municipios", "references_column": "codigo_6d"},
            {"column": "DIAG_PRINC", "references_table": "cid10", "references_column": "CID"},
            {"column": "CID_ASSO", "references_table": "cid10", "references_column": "CID"},
        ]
    },
    "atendimentos": {
        "table_name": "atendimentos",
        "columns": {
            "N_AIH": pl.String,
            "PROC_REA": pl.String,
        },
        "primary_key": ["N_AIH", "PROC_REA"],
        "foreign_keys": [
            {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"},
            {"column": "PROC_REA", "references_table": "procedimentos", "references_column": "PROC_REA"},
        ]
    },
    "uti_detalhes": {
        "table_name": "uti_detalhes",
        "columns": {
            "N_AIH": pl.String,
            "UTI_MES_TO": pl.Int32,
            "MARCA_UTI": pl.String,
            "UTI_INT_TO": pl.Int32,
            "VAL_UTI": pl.Int32
        },
        "primary_key": ["N_AIH"],
        "foreign_keys": [
            {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"}
        ]
    },

     "etnia": {
        "table_name": "etnia",
        "columns": {
            "N_AIH": pl.String,
            "ETNIA": pl.String,
            
        },
        "primary_key": ["N_AIH"],
        "foreign_keys": [
            {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"}
        ]
    },

    "diagnosticos": {
        "table_name": "diagnosticos",
        "columns": {
            "N_AIH": pl.String,
            "DIAG_SECUN": pl.String,
        },
        "primary_key": ["N_AIH"],
        "foreign_keys": [
            {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"}
        ]
    },
    "municipios": {
        "table_name": "municipios",

        "columns": {
            "codigo_6d": pl.String,
            "codigo_ibge": pl.String,
            "nome": pl.String,
            "latitude": pl.Int32,
            "longitude": pl.Int32,
            "estado": pl.String
        },
        "primary_key": ["codigo_6d"],
        "foreign_keys": [],
        "uniques": ["codigo_ibge"]
    },
    "condicoes_especificas": {
        "table_name": "condicoes_especificas",
        "columns": {
            "N_AIH": pl.String,
            "IND_VDRL": pl.String
        },
        "primary_key": ["N_AIH"],
        "foreign_keys": [
            {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"}
        ]
    },
    "hospital": {
        "table_name": "hospital",
        "columns": {
            "CNES": pl.String,
            "NATUREZA": pl.String,
            "GESTAO": pl.String,
            "NAT_JUR": pl.String
        },
        "primary_key": ["CNES"],
        "foreign_keys": []
    },
    "procedimentos": {
        "table_name": "procedimentos",
        "columns": {
            "PROC_REA": pl.String,
            "NOME_PROC": pl.String
        },
        "primary_key": ["PROC_REA"],
        "foreign_keys": []
    },
    "obstetricos": {
        "table_name": "obstetricos",
        "columns": {
            "N_AIH": pl.String,
            "INSC_PN": pl.String
        },
        "primary_key": ["N_AIH"],
        "foreign_keys": [
            {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"}
        ]
    },

    "instrucao": {
    "table_name": "instrucao",
    "columns": {
        "N_AIH": pl.String,   
        "INSTRU": pl.String   
    },
    "primary_key": ["N_AIH"],
    "foreign_keys": [
        {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"}
    ]
    },

    "cid10": {
        "table_name": "cid10",
        "columns": {
            "CID": pl.String,
            "CD_DESCRICAO": pl.String
        },
        "primary_key": ["CID"],
        "foreign_keys": []
    },

    "mortes": {
            "table_name": "mortes",
            "columns": {
                "N_AIH": pl.String,      # PK e FK para internacoes
                "CID_MORTE": pl.String   # Código CID da causa da morte
            },
            "primary_key": ["N_AIH"],
            "foreign_keys": [
                {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"}
            ]
         },
    "infehosp": {
            "table_name": "infehosp",
            "columns": {
                "N_AIH": pl.String,      
                "INFEHOSP": pl.String   
                },
                
            "primary_key": ["N_AIH"],
            "foreign_keys": [
                    {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"}
                ]
            },

    "vincprev": {
        "table_name": "vincprev",
        "columns": {
            "N_AIH": pl.String,     
            "VINCPREV": pl.String   
        },
        "primary_key": ["N_AIH"],
        "foreign_keys": [
            {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"}
        ]
    },

    "cbor": {
        "table_name": "cbor",
        "columns": {
            "N_AIH": pl.String,   
            "CBOR": pl.String     
        },
        "primary_key": ["N_AIH"],
        "foreign_keys": [
            {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"}
        ]
    },

    "dado_ibge": {
    "table_name": "dado_ibge",
    "columns": {
        "uf": pl.Int64,
        "nome_uf": pl.String,
        "regiao_geografica_intermediaria": pl.Int64,
        "nome_regiao_geografica_imediata": pl.String,
        "regiao_geografica_imediata": pl.Int64,
        "municipio": pl.Int64,
        "codigo_municipio_completo": pl.String,  
        "nome_municipio": pl.String,
        "sigla_estado": pl.String,
        "populacao": pl.Int32,
        "densidade_demografica": pl.Int32,
        "salario_medio": pl.Int32,
        "pessoal_ocupado": pl.Int32,
        "ideb_anos_iniciais_ensino_fundamental": pl.Int32,
        "ideb_anos_finais_ensino_fundamental": pl.Int32,
        "receita_bruta": pl.Int32,
        "dependencia_financeira": pl.Int32,
        "despesas_empenhadas": pl.Int32,
        "mortalidade_infantil": pl.Int32,
        "internacoes_por_diarreia": pl.Int32,
        "area_cidade": pl.Int32,
    },
    "primary_key": ["codigo_municipio_completo"],
    "foreign_keys": [
        {"column": "codigo_municipio_completo", "references_table": "municipios", "references_column": "codigo_ibge"}
    ]
},

    "contraceptivos": {
    "table_name": "contraceptivos",
    "columns": {
        "N_AIH": pl.String,
        "TIPO": pl.String,
        "CODIGO_METODO": pl.String,
    },
    "primary_key": ["N_AIH", "TIPO"],
    "foreign_keys": [
        {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"}
    ]
},
    "notificacoes": {
    "table_name": "notificacoes",
    "columns": {
        "N_AIH": pl.String,
        "CID_NOTIF": pl.String,
    },
    "primary_key": ["N_AIH"],
    "foreign_keys": [
        {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"}
    ]
},

"pernoite": {
    "columns": {
        "N_AIH": pl.String,
        "DIAR_ACOM": pl.Int32
    },
    "primary_key": ["N_AIH"],
    "foreign_keys": [
       {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"}
    ]
},


    }




def get_create_table_sql(table_name: str, with_constraints: bool = True) -> str:
    schema_info = TABLE_SCHEMAS.get(table_name)
    if not schema_info:
        raise ValueError(f"Esquema para a tabela '{table_name}' não encontrado.")

    columns_sql = []
    for col_name, col_type in schema_info["columns"].items():
        postgres_type = POLARS_TO_POSTGRES_TYPES.get(col_type, "TEXT")

        if col_name == "id" and col_name in schema_info["primary_key"] and col_type in [pl.Int32, pl.Int64]:
            postgres_type = "SERIAL" if col_type == pl.Int32 else "BIGSERIAL"

        if col_name in schema_info["primary_key"] and col_name != "id":
            postgres_type += " NOT NULL"

        columns_sql.append(f'"{col_name}" {postgres_type}')

    if with_constraints and schema_info["primary_key"]:
        pk_cols = [f'"{col}"' for col in schema_info["primary_key"]]
        columns_sql.append(f"PRIMARY KEY ({', '.join(pk_cols)})")

    if with_constraints:
        for fk in schema_info["foreign_keys"]:
            columns_sql.append(
                f"FOREIGN KEY (\"{fk['column']}\") REFERENCES \"{fk['references_table']}\" (\"{fk['references_column']}\") ON DELETE CASCADE"
            )

    return f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({', '.join(columns_sql)})"
