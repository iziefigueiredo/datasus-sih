-- Versão de teste final e mais robusta com CAST para BIGINT

SELECT
    "N_AIH",
    "DIAS_PERM" AS dias_perm_registrado,
    ("DT_SAIDA" - "DT_INTER") AS dias_perm_calculado
FROM
    {{ source('public', 'internacoes') }}
WHERE
    -- Comparamos ambos os valores como BIGINT para garantir consistência total de tipo.
    CAST("DIAS_PERM" AS BIGINT) != CAST(("DT_SAIDA" - "DT_INTER") AS INTEGER)
