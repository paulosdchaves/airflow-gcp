-- TABLE 4: Consolidado de vendas por linha, ano e mês
SELECT
    sales.LINHA,
    sales.ANO,
    sales.MES,
    SUM(sales.QTD_VENDA) AS TOTAL_VENDAS
FROM
    (
        SELECT
            "sales"."LINHA" as LINHA,
            SUBSTR("sales"."DATA_VENDA", 1, 2) as MES,
            SUBSTR("sales"."DATA_VENDA", 7, 4) as ANO,
            "sales"."QTD_VENDA" as QTD_VENDA
        FROM
            "sales"
    ) sales
GROUP BY
    sales.LINHA,
    sales.ANO,
    sales.MES
ORDER BY
    sales.LINHA ASC,
    sales.ANO ASC,
    sales.MES ASC;