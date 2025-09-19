
SELECT
    "N_AIH",
    "DT_INTER",
    "DT_SAIDA",
    "DIAS_PERM" AS dias_perm_registrado,
    -- A subtração de duas colunas DATE no PostgreSQL retorna um INTEGER com o número de dias.
    ("DT_SAIDA" - "DT_INTER") AS dias_perm_calculado
FROM
    {{ source('public', 'internacoes') }}
WHERE
    -- A condição WHERE agora é muito mais simples e legível.
    -- Seleciona as linhas onde o valor registrado é diferente do valor calculado.
    "DIAS_PERM" != ("DT_SAIDA" - "DT_INTER")
