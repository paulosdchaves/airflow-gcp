-- TABLE 3: Consolidado de vendas por marca, ano e mês
SELECT
    sales.MARCA,
    sales.ANO,
    sales.MES,
    SUM(sales.QTD_VENDA) AS TOTAL_VENDAS
FROM
    (
        SELECT
            "sales"."MARCA" as MARCA,
            SUBSTR("sales"."DATA_VENDA", 1, 2) as MES,
            SUBSTR("sales"."DATA_VENDA", 7, 4) as ANO,
            "sales"."QTD_VENDA" as QTD_VENDA
        FROM
            "sales"
    ) sales
GROUP BY
    sales.MARCA,
    sales.ANO,
    sales.MES
ORDER BY
    sales.MARCA ASC,
    sales.ANO ASC,
    sales.MES ASC;