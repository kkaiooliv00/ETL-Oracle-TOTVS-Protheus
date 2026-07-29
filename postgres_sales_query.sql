WITH vendas_filtradas AS (
    SELECT sl.*
    FROM sales sl
    WHERE sl.emissao_em >= DATE '2026-05-01'
      AND NOT sl.is_cancelado
      AND sl.ctrc_anulacao_id IS NULL
),
_soma_iss AS (
    SELECT ss2.sale_id,
           SUM(ss2.qtd_dias * ss2.quantidade * ss2.valor * ss2.iss / 100) AS total_iss
    FROM vendas_filtradas sl
    INNER JOIN sale_services ss2 ON ss2.sale_id = sl.id
    GROUP BY ss2.sale_id
),
cte_notas AS (
    SELECT nf.idconhecimento,
           SUM(nf.volume) AS tot_volume,
           SUM(nf.valor) AS tot_valor,
           SUM(nf.peso) AS tot_peso
    FROM nota_fiscal nf
    WHERE nf.idconhecimento IN (SELECT sl.id FROM vendas_filtradas sl)
    GROUP BY nf.idconhecimento
)
SELECT
    CONCAT_WS('-', sl.numero, sl.tipo, sl.serie, fl.abreviatura) AS super_chave,
    sl.id AS conhecimento_id,
    sl.numero,
    sl.tipo,
    sl.emissao_em,
    sl.emissao_as,
    sl.serie,
    sl.especie,
    CASE sl.tipo
        WHEN 'n' THEN 'NORMAL'
        WHEN 'l' THEN 'ENTREGA LOCAL (COBRANÇA)'
        WHEN 'i' THEN 'DIÁRIAS'
        WHEN 'p' THEN 'PALLETS'
        WHEN 'c' THEN 'COMPLEMENTAR'
        WHEN 'r' THEN 'REENTREGA'
        WHEN 'd' THEN 'DEVOLUÇÃO'
        WHEN 'b' THEN 'CORTESIA'
        WHEN 's' THEN 'SUBSTITUIÇÃO'
        WHEN 'a' THEN 'ANULAÇÃO'
        WHEN 't' THEN 'SUBSTITUÍDO'
        ELSE ''
    END AS descricao_tipo_conhecimento,
    fl.abreviatura AS filial,
    fl.cnpj AS cnpj_filial,
    cl.razaosocial AS consignatario,
    cl.cnpj AS cnpj_consignatario,
    clrem.razaosocial AS remetente,
    clrem.cnpj AS cnpj_remetente,
    cldes.razaosocial AS destinatario,
    cldes.cnpj AS cnpj_destinatario,
    COALESCE(sl.total_receita, 0)::numeric(15, 2) AS valor_ns_cte,
    COALESCE(nf.tot_volume, 0) AS volume_notas,
    COALESCE(nf.tot_valor, 0)::numeric(15, 2) AS valor_notas,
    COALESCE(nf.tot_peso, 0) AS peso_notas,
    COALESCE((ct.base_calculo * ct.aliquota / 100), 0)::numeric(15, 2) AS valor_icms,
    COALESCE(
        CASE WHEN cfg.tipo_calculo_iss = '1' THEN TRUNC(soma_iss.total_iss, 2)
             ELSE ROUNDABNT(soma_iss.total_iss, 2) END,
        0
    )::numeric(15, 2) AS valor_iss,
    COALESCE(sl.total_receita * ct.perc_irpj / 100, 0)::numeric(15, 2) AS valor_irpj,
    COALESCE(sl.total_receita * ct.perc_cssl / 100, 0)::numeric(15, 2) AS valor_cssl,
    COALESCE(sl.total_receita * ct.perc_pis / 100, 0)::numeric(15, 2) AS valor_pis,
    COALESCE(sl.total_receita * ct.perc_cofins / 100, 0)::numeric(15, 2) AS valor_cofins,
    CASE
        WHEN ct.remetente_id = sl.consignatario_id THEN 'CIF'
        WHEN ct.destinatario_id = sl.consignatario_id THEN 'FOB'
        WHEN ct.redespacho_id = sl.consignatario_id THEN 'RED'
        ELSE 'CON'
    END AS tipo_frete,
    CASE st.status
        WHEN 'C' THEN 'CONFIRMADO'
        WHEN 'N' THEN 'NEGADO'
        WHEN 'E' THEN 'ENVIADO'
        WHEN 'L' THEN 'CANCELADO'
        WHEN 'F' THEN 'FS-DA'
        ELSE 'PENDENTE'
    END AS status_cte,
    pl.descricao AS plano_custo,
    cf.idcartafrete AS numero_contrato_frete,
    COALESCE(cf.vlfretemotorista, 0)::numeric(15, 2) AS valor_frete_carreteiro,
    COALESCE(cf.vlliquido, 0)::numeric(15, 2) AS valor_liquido_contrato,
    COALESCE(cf.vlavaria, 0)::numeric(15, 2) AS valor_avarias,
    COALESCE(cf.vloutrasdeducoes, 0)::numeric(15, 2) AS valor_outras_deducoes,
    COALESCE(cf.valor_pedagio, 0)::numeric(15, 2) AS valor_pedagio,
    COALESCE(cf.valor_diaria, 0)::numeric(15, 2) AS valor_diaria,
    COALESCE(cf.valor_descarga, 0)::numeric(15, 2) AS valor_descarga,
    COALESCE(cf.outrosdescontos, 0)::numeric(15, 2) AS outros_descontos,
    COALESCE(cf.peso_manifesto, 0) AS total_peso_contrato
FROM vendas_filtradas sl
CROSS JOIN config cfg
INNER JOIN filial fl ON sl.filial_id = fl.idfilial
LEFT JOIN ctrcs ct ON sl.id = ct.sale_id
LEFT JOIN cliente cl ON sl.consignatario_id = cl.idcliente
LEFT JOIN cliente clrem ON ct.remetente_id = clrem.idcliente
LEFT JOIN cliente cldes ON ct.destinatario_id = cldes.idcliente
INNER JOIN appropriations ap ON sl.id = ap.sale_id
INNER JOIN planocusto pl ON ap.planocusto_id = pl.idconta
LEFT JOIN cte_notas nf ON nf.idconhecimento = sl.id
LEFT JOIN _soma_iss soma_iss ON soma_iss.sale_id = sl.id
LEFT JOIN LATERAL (
    SELECT crc.status
    FROM ctrc_recibo_cte crc
    WHERE crc.ctrc_id = sl.id
    ORDER BY crc.id DESC
    LIMIT 1
) st ON TRUE
LEFT JOIN LATERAL (
    SELECT cf2.idcartafrete,
           cf2.vlfretemotorista,
           cf2.vlliquido,
           cf2.vlavaria,
           cf2.vloutrasdeducoes,
           cf2.valor_pedagio,
           cf2.valor_diaria,
           cf2.valor_descarga,
           cf2.outrosdescontos,
           SUM(nfcf.peso) AS peso_manifesto
    FROM manifesto_conhecimento mc
    INNER JOIN nota_fiscal nfcf ON nfcf.idconhecimento = mc.idconhecimento
    INNER JOIN cartafrete_manifesto cfm ON cfm.idmanifesto = mc.idmanifesto
    INNER JOIN carta_frete cf2 ON cf2.idcartafrete = cfm.idcartafrete
    WHERE mc.idconhecimento = sl.id
    GROUP BY cf2.idcartafrete, cf2.vlfretemotorista, cf2.vlliquido, cf2.vlavaria,
             cf2.vloutrasdeducoes, cf2.valor_pedagio, cf2.valor_diaria,
             cf2.valor_descarga, cf2.outrosdescontos
) cf ON TRUE;
