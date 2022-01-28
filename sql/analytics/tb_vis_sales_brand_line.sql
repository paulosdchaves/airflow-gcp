-- TABLE 2: Consolidado de vendas por marca e linha
SELECT
    sales.MARCA,
    sales.LINHA,
    SUM(sales.QTD_VENDA) AS TOTAL_VENDAS
FROM
    (
        SELECT
            "sales"."MARCA" as MARCA,
            "sales"."LINHA" as LINHA,
            "sales"."QTD_VENDA" as QTD_VENDA
        FROM
            "sales"
    ) sales
GROUP BY
    sales.MARCA,
    sales.LINHA
ORDER BY
    sales.MARCA ASC,
    sales.LINHA ASC;