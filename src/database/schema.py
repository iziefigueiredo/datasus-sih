import polars as pl
from typing import Dict, List, Tuple

# O mapeamento de tipos foi movido para o load.py para evitar redundância.
# Este arquivo agora contém apenas a definição do esquema.

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
        ]
    },
    "atendimentos": {
        "table_name": "atendimentos",
        "columns": {
            "id_atendimento": pl.UInt64, 
            "N_AIH": pl.String,
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
            {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"},
            {"column": "DIAG_SECUN", "references_table": "cid10", "references_column": "CID"}

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
                {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"},
                {"column": "CID_MORTE", "references_table": "cid10", "references_column": "CID"}

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
        {"column": "N_AIH", "references_table": "internacoes", "references_column": "N_AIH"},
        {"column": "CID_NOTIF", "references_table": "cid10", "references_column": "CID"}
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