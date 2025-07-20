/*
Script utilisé pour la création des tables
*/

DROP TABLE IF EXISTS dap_tools.dim_region CASCADE;
CREATE TABLE dap_tools.dim_region(
"Code_region" VARCHAR(20) PRIMARY KEY,
"Region" VARCHAR(100),
region_order INTEGER,
CONSTRAINT unique_code_region UNIQUE ("Code_region")
);


DROP TABLE IF EXISTS dap_tools.dim_district CASCADE;
CREATE TABLE dap_tools.dim_district( 
"Code_district" VARCHAR(20) PRIMARY KEY,
"District" VARCHAR(100),
"Code_region" VARCHAR(20) REFERENCES dap_tools.dim_region("Code_region") ON DELETE CASCADE,
CONSTRAINT unique_code_district UNIQUE ("Code_district")
);

DROP TABLE IF EXISTS dap_tools.dim_structure CASCADE;
CREATE TABLE dap_tools.dim_structure(
"Code_ets" VARCHAR(20) PRIMARY KEY,
"Structure" VARCHAR(250),
"Code_district" VARCHAR(20) REFERENCES dap_tools.dim_district("Code_district") ON DELETE CASCADE,
CONSTRAINT unique_code_ets UNIQUE ("Code_ets")
);


DROP TABLE IF EXISTS dap_tools.dim_sous_programme CASCADE;
CREATE TABLE dap_tools.dim_sous_programme(
"Code_sous_prog" VARCHAR(20) PRIMARY KEY,
"Sous_programme" VARCHAR(250),
"Programme" VARCHAR(20) REFERENCES dap_tools.dim_programme("Programme") ON DELETE CASCADE,
CONSTRAINT unique_code_sous_prog UNIQUE ("Code_sous_prog")
);


DROP TABLE IF EXISTS dap_tools.dim_programme CASCADE;
CREATE TABLE dap_tools.dim_programme(
"Programme" VARCHAR(20) PRIMARY KEY,
programme_order INTEGER,
CONSTRAINT unique_code_Programme UNIQUE ("Programme")
);

DROP SEQUENCE IF EXISTS dap_tools.dim_produit_seq;
CREATE SEQUENCE dap_tools.dim_produit_seq
    INCREMENT BY 1
    START WITH 1
    CACHE 1;

DROP TABLE IF EXISTS dap_tools.dim_produit CASCADE;
CREATE TABLE dap_tools.dim_produit(
id_produit_pk BIGINT PRIMARY KEY,
"Code_produit" VARCHAR(20),
"Produit_designation" VARCHAR(250),
"Unit_rapportage" VARCHAR(100),
"Categorie_produit" VARCHAR(100), --produit traceur ou non
"Categorie_du_produit" VARCHAR(100),
"Code_sous_prog" VARCHAR(20) REFERENCES dap_tools.dim_sous_programme("Code_sous_prog") ON DELETE CASCADE,
CONSTRAINT unique_id_produit_pk UNIQUE ("id_produit_pk")
);

DROP TABLE IF EXISTS dap_tools.comp_promp_par_ets CASCADE;
CREATE TABLE dap_tools.comp_promp_par_ets(
"id_comp_promp_par_ets" SERIAL PRIMARY KEY,
"Code_ets" VARCHAR(20) REFERENCES dap_tools.dim_structure("Code_ets") ON DELETE CASCADE,
indicateur_type	VARCHAR(20),
arv VARCHAR(10),
trc VARCHAR(10),
lab VARCHAR(10),
charge_virale VARCHAR(10),
pnlp VARCHAR(10),
pnsme VARCHAR(10),
pnsme_grat VARCHAR(10),
pnn VARCHAR(10),
tbs VARCHAR(10),
tbmr VARCHAR(10),
tblab VARCHAR(10),
pnls_recu INTEGER,
pnls_attendu INTEGER,
taux_indicateur_pnlp REAL,
taux_indicateur_pnsme REAL,
taux_indicateur_pnn REAL,
taux_indicateur_pnlt REAL,
taux_indicateur_pnls REAL,
sum_produit_inline INTEGER,
count_produit_inline INTEGER,
date_report DATE,
CONSTRAINT unique_id_comp_promp_par_ets UNIQUE ("id_comp_promp_par_ets"));

DROP TABLE IF EXISTS dap_tools.comp_promp_attendu_region CASCADE;
CREATE TABLE dap_tools.comp_promp_attendu_region(
"id_comp_promp_region" SERIAL PRIMARY KEY,
"Code_region" VARCHAR(20) REFERENCES dap_tools.dim_region("Code_region") ON DELETE CASCADE,
indicateur_type	VARCHAR(20),
total_rapports_attendus_arv	INTEGER,
taux_par_region_arv	REAL,
total_rapports_attendus_trc INTEGER,
taux_par_region_trc REAL,
total_rapports_attendus_lab INTEGER,
taux_par_region_lab	REAL,
total_rapports_attendus_charge_virale INTEGER,
taux_par_region_charges_virales	REAL,
total_rapports_attendus_pnlp INTEGER,
taux_par_region_pnlp REAL,
total_rapports_attendus_pnn	INTEGER,
taux_par_region_pnn	REAL,
total_rapports_attendus_pnsme INTEGER,
taux_par_region_pnsme REAL,
total_rapports_attendus_pnlt INTEGER,
taux_par_region_pnlt REAL,
total_rapports_attendus_pnls INTEGER,
taux_par_region_pnls REAL,
taux_indicateur_region REAL,
date_report DATE,
CONSTRAINT unique_id_id_comp_promp_region UNIQUE ("id_comp_promp_region"));

DROP TABLE IF EXISTS dap_tools.recap_stock_by_region CASCADE;
CREATE TABLE dap_tools.recap_stock_by_region(
"id_recap_stock_pk" SERIAL PRIMARY KEY,
"Code_region" VARCHAR(20) REFERENCES dap_tools.dim_region("Code_region") ON DELETE CASCADE,
"Programme" VARCHAR(20) REFERENCES dap_tools.dim_programme("Programme") ON DELETE CASCADE,
dispo_globale REAL,
dispo_traceur REAL,
date_report DATE,
CONSTRAINT unique_id_recap_stock_pk UNIQUE ("id_recap_stock_pk")

DROP TABLE IF EXISTS dap_tools.recap_stock_prog_region CASCADE;
CREATE TABLE dap_tools.recap_stock_prog_region(
"id_stock_prog_region_pk" SERIAL PRIMARY KEY,
"id_produit_fk" BIGINT REFERENCES dap_tools.dim_produit("id_produit_pk") ON DELETE CASCADE,
"Programme" VARCHAR(20) REFERENCES dap_tools.dim_programme("Programme") ON DELETE CASCADE,
"Code_region" VARCHAR(20) REFERENCES dap_tools.dim_region("Code_region") ON DELETE CASCADE,
MSD VARCHAR(250), --REAL,
STATUT VARCHAR(50),
date_report DATE,
CONSTRAINT unique_id_stock_prog_region_pk UNIQUE ("id_stock_prog_region_pk")
);

DROP TABLE IF EXISTS dap_tools.recap_stock_prog_nat CASCADE;
CREATE TABLE dap_tools.recap_stock_prog_nat(
"id_stock_prog_nat_pk" SERIAL PRIMARY KEY,
"id_produit_fk" BIGINT REFERENCES dap_tools.dim_produit("id_produit_pk") ON DELETE CASCADE,
"Programme" VARCHAR(20) REFERENCES dap_tools.dim_programme("Programme") ON DELETE CASCADE,
"Code_region" VARCHAR(20) REFERENCES dap_tools.dim_region("Code_region") ON DELETE CASCADE,
MSD VARCHAR(50), --REAL,
STATUT VARCHAR(50),
CONSO REAL,
SDU REAL,
CMM REAL,
dispo_globale REAL,
dispo_globale_cible	REAL,
dispo_traceur REAL,
dispo_traceur_cible REAL,
statut_pourcentage REAL,
date_report DATE,
CONSTRAINT unique_id_stock_prog_nat_pk UNIQUE ("id_stock_prog_nat_pk")
);

DROP TABLE IF EXISTS dap_tools.etat_de_stock CASCADE;
CREATE TABLE dap_tools.etat_de_stock(
"id_etat_stock_pk" SERIAL PRIMARY KEY,
"id_produit_fk" BIGINT REFERENCES dap_tools.dim_produit("id_produit_pk") ON DELETE CASCADE,
"Code_ets" VARCHAR(20) REFERENCES dap_tools.dim_structure("Code_ets") ON DELETE CASCADE,
"Periode" VARCHAR(250),
stock_initial REAL,
qte_recue REAL,
qte_utilisee REAL,
perte_ajust REAL,
j_rupture REAL,
sdu REAL,
cmm_esigl REAL,
cmm_gest REAL,
qte_prop REAL,
qte_cmde REAL,
qte_approuv REAL,
msd REAL,
etat_stock VARCHAR(250),
besoin_cmde_urg	REAL,
besoin_trsf_in REAL,
qte_trsf_out REAL,
qte_cmde_mois_prec REAL,
etat_stock_mois_prec VARCHAR(250),
date_report DATE,
CONSTRAINT unique_id_etat_stock_pk UNIQUE ("id_etat_stock_pk")


DROP TABLE IF EXISTS dap_tools.share_link_fbr CASCADE;
CREATE TABLE dap_tools.share_link_fbr(
id_share_link_fbr_pk SERIAL PRIMARY KEY,
code_region VARCHAR(20) REFERENCES dap_tools.dim_region("Code_region") ON DELETE CASCADE,
share_link TEXT,
date_report DATE,
CONSTRAINT unique_id_share_link_fbr UNIQUE (id_share_link_fbr_pk)
);

CREATE INDEX idx_date_report_share_link_fbr ON dap_tools.share_link_fbr (date_report)



DROP TABLE IF EXISTS dap_tools.users_region_level_fbr CASCADE;
CREATE TABLE dap_tools.users_region_level_fbr(
id_users_region_level_fbr_pk SERIAL PRIMARY KEY,
code_region VARCHAR(20) REFERENCES dap_tools.dim_region("Code_region") ON DELETE CASCADE,
email VARCHAR(250),
CONSTRAINT unique_id_users_region_level_fbr UNIQUE (id_users_region_level_fbr_pk)
);