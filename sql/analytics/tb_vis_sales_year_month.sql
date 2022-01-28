-- TABLE 1: Consolidado de vendas por ano e mês
SELECT
    sales.ANO,
    sales.MES,
    SUM(sales.QTD_VENDA) AS TOTAL_VENDAS
FROM
    (
        SELECT
            SUBSTR(sales.DATA_VENDA, 1, 2) as MES,
            SUBSTR(sales.DATA_VENDA, 7, 4) as ANO,
            sales.QTD_VENDA as QTD_VENDA
        FROM
            sales
    ) sales
GROUP BY
    sales.ANO,
    sales.MES
ORDER BY
    sales.ANO ASC,
    sales.MES ASC;