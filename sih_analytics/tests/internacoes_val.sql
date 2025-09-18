-- Este teste seleciona todas as internações onde o valor total
-- é MENOR OU IGUAL a zero, o que é um erro.
-- O teste passará se esta consulta não encontrar nenhuma linha.

SELECT
    "N_AIH",
    "VAL_TOT"
FROM
    {{ source('public', 'internacoes') }} -- Usando a FONTE diretamente
WHERE
    "VAL_TOT" <= 0 -- CORRIGIDO: Procurando pelas linhas RUINS
