import polars as pl
from typing import Dict, List, Tuple

# Define the schema for each table in the database
# Each table schema includes column names, data types, primary keys, and foreign keys

TABLE_SCHEMAS: Dict[str, Dict[str, any]] = {
    "internacoes": {
        "table_name": "internacoes",
        "columns": {
            "CNES": pl.Int64,
            "N_AIH": pl.Int64,
            "ESPEC": pl.Int8,
            "IDENT": pl.Int8,
            "DT_INTER": pl.Date,
            "DT_SAIDA": pl.Date,
            "DIAS_PERM":pl.Int16,
            "VAL_SH": pl.Int32,
            "VAL_SP": pl.Int32,
            "VAL_TOT": pl.Int32,
            "COMPLEX": pl.String,
            "MUNIC_MOV": pl.Int32,
            "DIAG_PRINC": pl.String,
            "NASC": pl.Date,
            "SEXO": pl.Int8,
            "IDADE": pl.Int16,
            "NACIONAL": pl.Int16,
            "NUM_FILHOS": pl.Int8,
            "RACA_COR": pl.Int8,
            "MUNIC_RES": pl.Int32,
            "CEP": pl.Int64,
        },
        "primary_key": ["N_AIH"],
        "foreign_keys": [
            {"column": "CNES", "references_table": "hospital", "references_column": "CNES"},
            {"column": "MUNIC_RES", "references_table": "municipios", "references_column": "codigo_6d"},
            {"column": "MUNIC_MOV", "references_table": "municipios", "references_column": "codigo_6d"},
            {"column": "DIAG_PRINC", "references_table": "cid10", "references_column": "CID"},
        ]
    },
    "atendimentos": {
        "table_name": "atendimentos",
        "columns": {
            "id_atendimento": pl.UInt64, 
            "N_AIH": pl.Int64,
            "PROC_REA": pl.String,
        },
        "primary_key": ["id_atendimento"],
        "foreign_keys": [
            {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"},
            {"column": "PROC_REA", "references_table": "procedimentos", "references_column": "PROC_REA"},
        ]
    },
    "uti_detalhes": {
        "table_name": "uti_detalhes",
        "columns": {
            "N_AIH": pl.Int64,
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
            "N_AIH": pl.Int64,
            "ETNIA": pl.Int32,
            
        },
        "primary_key": ["N_AIH"],
        "foreign_keys": [
            {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"}
        ]
    },

    "diagnosticos": {
        "table_name": "diagnosticos",
        "columns": {
            "N_AIH": pl.Int64,
            "DIAG_SECUN": pl.String,
        },
        "primary_key": ["N_AIH"],
        "foreign_keys": [
            {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"},
            {"column": "DIAG_SECUN", "references_table": "cid10", "references_column": "CID"}

        ]
    },
    "municipios": {
        "table_name": "municipios",

        "columns": {
            "codigo_6d": pl.Int64,
            "codigo_ibge": pl.Int64,
            "nome": pl.String,
            "latitude": pl.Float32,
            "longitude": pl.Float32,
            "estado": pl.String
        },
        "primary_key": ["codigo_6d"],
        "foreign_keys": [],
        "uniques": ["codigo_ibge"]
    },
    "condicoes_especificas": {
        "table_name": "condicoes_especificas",
        "columns": {
            "N_AIH": pl.Int64,
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
            "CNES": pl.Int64,
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
            "PROC_REA": pl.Int64,
            "NOME_PROC": pl.String
        },
        "primary_key": ["PROC_REA"],
        "foreign_keys": []
    },
    "obstetricos": {
        "table_name": "obstetricos",
        "columns": {
            "N_AIH": pl.Int64,
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
        "N_AIH": pl.Int64,   
        "INSTRU": pl.Int8   
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
                "N_AIH": pl.Int64,      # PK e FK para internacoes
                "CID_MORTE": pl.String   # Código CID da causa da morte
            },
            "primary_key": ["N_AIH"],
            "foreign_keys": [
                {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"},
                {"column": "CID_MORTE", "references_table": "cid10", "references_column": "CID"}

            ]
         },
    "infehosp": {
            "table_name": "infehosp",
            "columns": {
                "N_AIH": pl.Int64,      
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
            "N_AIH": pl.Int64,     
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
            "N_AIH": pl.Int64,   
            "CBOR": pl.String     
        },
        "primary_key": ["N_AIH"],
        "foreign_keys": [
            {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"}
        ]
    },

    
    "indicadores": {
    "table_name": "indicadores",
    "columns": {
        "codigo_6d": pl.String,
        "ano": pl.Int32,
        "metrica": pl.String,
        "valor": pl.Float64,
        "escala": pl.String,
    },
    "primary_key": ["codigo_6d", "ano", "metrica"],
    "foreign_keys": [
        {"column": "codigo_6d", "references_table": "municipios", "references_column": "codigo_6d"}
    ]
},


    "contraceptivos": {
    "table_name": "contraceptivos",
    "columns": {
        "N_AIH": pl.Int64,
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
        "N_AIH": pl.Int64,
        "CID_NOTIF": pl.String,
    },
    "primary_key": ["N_AIH"],
    "foreign_keys": [
        {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"},
        {"column": "CID_NOTIF", "references_table": "cid10", "references_column": "CID"}
    ]
},

"pernoite": {
    "columns": {
        "N_AIH": pl.Int64,
        "DIAR_ACOM": pl.Int32
    },
    "primary_key": ["N_AIH"],
    "foreign_keys": [
       {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"}
    ]
},


    }