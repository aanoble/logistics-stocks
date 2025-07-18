QUERY_ETAT_STOCK = """
SELECT 
    prod.*, 
    CASE 
        WHEN EXTRACT(MONTH FROM st.date_report) = 12
        THEN st.stock_theorique_final_sage
        ELSE st.stock_theorique_final_attendu
    END AS stock_theorique_mois_precedent
FROM 
    {schema_name}.stock_track st
INNER JOIN 
   {schema_name}.dim_produit_stock_track prod 
    ON st.id_dim_produit_stock_track_fk = prod.id_dim_produit_stock_track_pk
WHERE 
    prod.programme = '{programme}'
    AND st.date_report = '{date_report_prec}'
ORDER BY prod.id_dim_produit_stock_track_pk
"""

QUERY_ETAT_STOCK_PROGRAMME = """
SELECT * 
FROM dap_tools.recap_stock_prog_nat recap
INNER JOIN dap_tools.dim_produit prod ON recap.id_produit_fk = prod.id_produit_pk
WHERE date_report='{eomonth}' AND "Programme"='{programme}'
"""

QUERY_ETAT_STOCK_PERIPH = """
SELECT *
FROM dap_tools.etat_de_stock ets
INNER JOIN dap_tools.dim_produit dp on ets.id_produit_fk = dp.id_produit_pk
WHERE date_report='{eomonth}'
"""
