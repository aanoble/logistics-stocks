QUERY_TRANSMISSION = """
WITH SubmissionDates AS (
    SELECT
        rnrid,
        MIN(createddate) FILTER (WHERE status = 'SUBMITTED')::date as date_soumission,
        MIN(createddate) FILTER (WHERE status = 'AUTHORIZED')::date as date_autorisation,
        MIN(createddate) FILTER (WHERE status = 'AUTHORIZED')::timestamp as min_date_autorisation
    FROM requisition_status_changes
    WHERE status IN ('SUBMITTED', 'AUTHORIZED')
    GROUP BY rnrid
)
SELECT
    d.region_name AS region,
    d.region_id as id_region_esigl,
    cast(f.code as INTEGER) as code,
    f.name as facility,
    g.name as district,
    g.id AS id_district_esigl,
    pr.name as program,
    r.emergency as cde_urgente,
    p.name as period,
    r.status as statut,
    u.lastname || ' ' || u.firstname as user,
    sd.date_soumission,
    sd.date_autorisation,
    TO_CHAR(age(localtimestamp, sd.min_date_autorisation), 'FMDDD HH24:MI:SS') as time_ago
FROM requisitions r 
JOIN facilities f on f.id = r.facilityid 
JOIN vw_districts d ON f.geographiczoneid = d.district_id
JOIN geographic_zones g on g.id=f.geographiczoneid
JOIN processing_periods p on p.id = r.periodid 
JOIN programs pr on pr.id = r.programid
JOIN users u on u.id = r.createdBy
LEFT JOIN SubmissionDates sd ON r.id = sd.rnrid
WHERE pr.id IN ('23','24','25','26','27','31','32','43','22','28','36')
    AND upper(p.name) in {date_report}
    AND r.emergency = 'false'
    AND r.status IN ('AUTHORIZED','APPROVED','RELEASED')
ORDER BY period asc
"""

QUERY_ETAT_STOCK = """
SELECT
    requisitions.emergency as Commande_urgente,
    programs.name AS Programme,
    processing_periods.name AS Periode,
    vw_districts.region_name AS Region,
    vw_districts.region_id as id_region_esigl,
    geographic_zones.name AS District,
    geographic_zones.id AS id_district_esigl, -- inclus ici pour effectuer le filtre pour la liste des districts sanitaires en routines pnn
    CAST(facilities.code AS INTEGER) AS Code,
    facilities.name AS Etablissement,
    facility_operators.text as Type_structure,
    product_categories.name AS categorie_produit,
    requisition_line_items.productcode as Code_produit,
    requisition_line_items.product as Designation,
    requisition_line_items.dispensingunit as Unite,
    requisition_line_items.beginningbalance as Stock_initial,
    requisition_line_items.quantityreceived as Quantite_recue,
    requisition_line_items.quantitydispensed as Quantite_distribuee,
    requisition_line_items.totallossesandadjustments as Perte_ajustement,
    requisition_line_items.stockinhand as SDU,
    requisition_line_items.amc as CMM,
    requisition_line_items.stockoutdays as NbreJrsRupture,
    requisition_line_items.calculatedorderquantity as Quantite_proposee,
    requisition_line_items.quantityrequested as Quantite_commandee,
    requisition_line_items.quantityapproved as Quantite_approuvee,
    requisition_line_items.reasonforrequestedquantity as Explication_de_la_qte_cmdee
FROM requisition_line_items
    JOIN requisitions ON requisition_line_items.rnrid = requisitions.id
    JOIN products ON requisition_line_items.productcode::text = products.code::text
    JOIN programs ON requisitions.programid = programs.id
    JOIN program_products ON products.id = program_products.productid AND program_products.programid = programs.id
    JOIN processing_periods ON requisitions.periodid = processing_periods.id
    JOIN product_categories ON program_products.productcategoryid = product_categories.id
    JOIN processing_schedules ON processing_periods.scheduleid = processing_schedules.id
    JOIN facilities ON requisitions.facilityid = facilities.id
    JOIN facility_operators ON facilities.operatedbyid = facility_operators.id
    JOIN facility_types ON facilities.typeid = facility_types.id
    JOIN vw_districts  ON facilities.geographiczoneid = vw_districts.district_id
    JOIN geographic_zones ON facilities.geographiczoneid = geographic_zones.id
    LEFT JOIN product_forms ON products.formid = product_forms.id
    LEFT JOIN dosage_units ON products.dosageunitid = dosage_units.id
WHERE skipped='false' and upper(processing_periods.name) in {date_report} and programs.id in ('23','24','25','26','27','31','32','43','22','28','36') and requisitions.status<>'INITIATED' and  requisitions.status<>'SUBMITTED' and requisitions.emergency='FALSE' and requisition_line_items.fullsupply='true' and 
(requisition_line_items.beginningbalance + requisition_line_items.quantityreceived + requisition_line_items.quantitydispensed + requisition_line_items.totallossesandadjustments + requisition_line_items.stockinhand +
requisition_line_items.amc + requisition_line_items.stockoutdays + requisition_line_items.calculatedorderquantity + requisition_line_items.quantityrequested) > 0
"""

QUERY_DISTRICT = """
SELECT DISTINCT
    vw_districts.region_name AS region,
    geographic_zones.name AS district,
    geographic_zones.id AS id_district,
    facilities.code AS code,
    facilities.name AS etablissement
FROM vw_districts
    JOIN facilities ON vw_districts.district_id  = facilities.geographiczoneid
    JOIN geographic_zones ON facilities.geographiczoneid = geographic_zones.id
ORDER BY region
"""

QUERY_FACILITY = """
SELECT DISTINCT
    facilities.code AS code,
    facility_operators.text as Type_structure
FROM facilities
JOIN facility_operators ON facilities.operatedbyid = facility_operators.id
"""