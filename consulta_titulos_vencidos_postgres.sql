SELECT
    se2."E2_FILIAL",
    se2."E2_NOMFOR",
    se2."E2_TIPO",
    se2."E2_NUM",
    se2."E2_VALOR",
    se2."E2_SALDO",
    se2."E2_PARCELA",
    se2."E2_FORNECE",
    se2."E2_LOJA",
    se2."E2_VENCREA",
    se2."E2_HIST",
    flf."FLF_MOTIVO"
FROM tables."SE2" AS se2
LEFT JOIN tables."FLF" AS flf
    ON flf.flf_presta = SUBSTRING(se2."E2_HIST" FROM 4 FOR 10)
WHERE se2."E2_VENCTO" <= TO_CHAR(CURRENT_DATE, 'YYYYMMDD')
  AND NULLIF(BTRIM(se2."E2_VENCTO"), '') IS NOT NULL
  AND se2."E2_XPAGO" <> 'S'
  AND se2."E2_XBLOQ" <> 'S'
  AND se2."E2_TIPO" NOT IN ('CR', 'SES', 'INS')
ORDER BY se2."E2_VENCTO" DESC;
