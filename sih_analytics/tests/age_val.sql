-- Este teste verifica se a coluna "IDADE" registrada corresponde à idade
-- calculada a partir da data de internação e da data de nascimento,
-- replicando a lógica exata do pipeline de pré-processamento.

-- O teste passa se esta consulta retornar 0 linhas.

WITH idade_calculada AS (
    SELECT
        "N_AIH",
        "IDADE" AS idade_registrada,
        "NASC",
        "DT_INTER",

        -- Lógica de cálculo da idade que espelha o código Polars
        (
            EXTRACT(YEAR FROM "DT_INTER") - EXTRACT(YEAR FROM "NASC")
        ) - 
        (
            CASE 
                WHEN 
                    -- Condição: o aniversário ainda não ocorreu no ano da internação
                    EXTRACT(MONTH FROM "DT_INTER") < EXTRACT(MONTH FROM "NASC") OR
                    (
                        EXTRACT(MONTH FROM "DT_INTER") = EXTRACT(MONTH FROM "NASC") AND 
                        EXTRACT(DAY FROM "DT_INTER") < EXTRACT(DAY FROM "NASC")
                    )
                THEN 1 -- Se ainda não fez aniversário, subtrai 1
                ELSE 0 -- Se já fez aniversário, não subtrai nada
            END
        ) AS idade_calculada

    FROM
        {{ source('public', 'internacoes') }}
    WHERE
        -- Pré-filtra registros onde o cálculo seria impossível ou inválido
        "DT_INTER" IS NOT NULL
        AND "NASC" IS NOT NULL
        AND "DT_INTER" > "NASC"
)

-- Seleciona apenas as linhas onde há inconsistência
SELECT
    *
FROM
    idade_calculada
WHERE
    -- Compara a idade registrada com a idade recém-calculada
    CAST(idade_registrada AS INTEGER) != CAST(idade_calculada AS INTEGER)

