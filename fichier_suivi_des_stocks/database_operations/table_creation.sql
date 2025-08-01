-- suivi_stock.dim_produit_stock_track definition

-- drop table if exists suivi_stock.dim_produit_stock_track cascade;

create table suivi_stock.dim_produit_stock_track (
	id_dim_produit_stock_track_pk serial4 not null,
	code_produit int8 null,
	ancien_code varchar(250) null,
	categorie varchar(250) null,
	designation text null,
	type_produit varchar(100) null,
	unit_niveau_central varchar(250) null,
	unit_niveau_peripherique varchar(250) null,
	facteur_de_conversion int4 null,
	designation_acronym varchar(250) null,
	code_qat varchar(250) null,
	cout_unitaire_moyen_qat float4,
	facteur_de_conversion_qat_sage float4,
	programme varchar(20) null,
	constraint unique_id_produit_st primary key (id_dim_produit_stock_track_pk),
	constraint unique_code_produit_programme unique (code_produit,
programme),
-- Nouvelle contrainte d'unicité
	constraint dim_produit_stock_track_programme_fk foreign key (programme) references dap_tools.dim_programme("Programme") on
delete
	cascade
);
-- suivi_stock.stock_track definition

-- drop table if exists suivi_stock.stock_track cascade;

create table suivi_stock.stock_track (
	id_stock_track_pk serial4 not null,
	id_dim_produit_stock_track_fk int8 null,
	stock_theorique_mois_precedent float4 null,
	distribution_effectuee float4 null,
	quantite_recue_stock float4 null,
	quantite_ppi float4 null,
	quantite_prelevee_cq float4 null,
	ajustement_stock float4 null,
	stock_theorique_final_sage float4 null,
	stock_theorique_final_attendu float4 null,
	ecarts float4 null,
	justification_ecarts text null,
	diligences text null,
-- dilig_choisie text NULL,
sdu_central_annexe_2 float4 null,
	dmm_central_annexe_2 float4 null,
	msd_central_annexe_2 varchar(250) null,
	statut_central_annexe_2 varchar(250) null,
	conso_decentralise_annexe_2 float4 null,
	sdu_decentralise_annexe_2 float4 null,
	cmm_decentralise_annexe_2 float4 null,
	msd_decentralise_annexe_2 varchar(250) null,
	statut_decentralise_annexe_2 varchar(250) null,
	nombre_de_site_en_rupture_annexe_2 int4 null,
	sdu_national_annexe_2 float4 null,
	cmm_national_annexe_2 float4 null,
	msd_national_annexe_2 varchar(250) null,
	statut_national_annexe_2 varchar(250) null,
	date_peremption_plus_proche_brute_annexe_2 date null,
	date_peremption_plus_proche_annexe_2 date null,
	quantite_correspondante_annexe_2 float4 null,
	msd_correspondant_annexe_2 varchar(250) null,
-- les autres champs ont été gérés directement par l'outil de visualisation
quantite_attendue_annexe_2 float4 null,
	msd_attendu_annexe_2 varchar(250) null,
	quantite_non_stockee_annexe_2 float4 null,
	msd_recu_annexe_2 varchar(250) null,
--	tx_satisfaction_annexe_2 varchar(250) NULL,
financement_annexe_2 varchar(250) null,
	date_probable_livraison_annexe_2 date null,
--	duree_transit_annexe_2 varchar(250) NULL,
date_effective_livraison_annexe_2 date null,
--	retard_livraison_annexe_2 varchar(250) NULL,
statut_annexe_2 varchar(250) null,
--	jours_rupture_avant_livraison_npsp_annexe_2 float4 NULL,
--	risque_rupture_annexe_2 varchar(250) NULL,
--	risque_peremption_annexe_2 varchar(250) NULL,
analyse_risque_commentaires_annexe_2 text null,
	diligences_central_annexe_2 text null,
	diligences_peripherique_annexe_2 text null,
	responsable_annexe_2 varchar(250) null,
	dilig_choisie_annexe_2 text null,
	date_report date null,
	constraint unique_id_stock_track primary key (id_stock_track_pk),
	constraint stock_track_id_dim_produit_stock_track_fk foreign key (id_dim_produit_stock_track_fk) references suivi_stock.dim_produit_stock_track(id_dim_produit_stock_track_pk) on
delete
	cascade
);

create index idx_date_report on
suivi_stock.stock_track
	using btree (date_report);
-- Drop table

-- drop table if exists suivi_stock.stock_track_cmm cascade;

create table suivi_stock.stock_track_cmm (
	id_stock_track_cmm_pk serial4 not null,
	id_dim_produit_stock_track_fk int8 null,
	date_report date null,
	cmm float4 null,
	nbre_mois_consideres float4 null,
	conso_mois_consideres float4 null,
	cmm_calculee float4 null,
--	cmm_validee_precedent float4 NULL,
commentaire text null,
	constraint unique_id_stock_track_cmm primary key (id_stock_track_cmm_pk),
	constraint stock_track_cmm_id_dim_produit_stock_track_fk foreign key (id_dim_produit_stock_track_fk) references suivi_stock.dim_produit_stock_track(id_dim_produit_stock_track_pk) on
delete
	cascade
);

create index idx_date_report_cmm on
suivi_stock.stock_track_cmm
	using btree (date_report);
-- Drop table

-- drop table if exists suivi_stock.stock_track_cmm_histo cascade;

create table suivi_stock.stock_track_cmm_histo (
	id_stock_track_cmm_histo_pk serial4 not null,
	id_dim_produit_stock_track_fk int8 null,
	date_report date not null,
	date_report_prev date not null,
	cmm float4 null,
	constraint unique_id_stock_track_cmm_histo primary key (id_stock_track_cmm_histo_pk),
	constraint stock_track_cmm_histo_id_dim_produit_stock_track_fk foreign key (id_dim_produit_stock_track_fk) references suivi_stock.dim_produit_stock_track(id_dim_produit_stock_track_pk) on
delete
	cascade
);

create index idx_date_report_cmm_histo on
suivi_stock.stock_track_cmm_histo
	using btree (date_report);
-- Drop table

-- drop table if exists suivi_stock.stock_track_dmm cascade;

create table suivi_stock.stock_track_dmm (
	id_stock_track_dmm_pk serial4 not null,
	id_dim_produit_stock_track_fk int8 null,
	date_report date null,
	dmm float4 null,
	nbre_mois_consideres float4 null,
	distributions_mois_consideres float4 null,
	dmm_calculee float4 null,
--	dmm_validee_precedent float4 NULL,
commentaire text null,
	constraint unique_id_stock_track_dmm primary key (id_stock_track_dmm_pk),
	constraint stock_track_dmm_id_dim_produit_stock_track_fk foreign key (id_dim_produit_stock_track_fk) references suivi_stock.dim_produit_stock_track(id_dim_produit_stock_track_pk) on
delete
	cascade
);

create index idx_date_report_dmm on
suivi_stock.stock_track_dmm
	using btree (date_report);

-- drop table if exists suivi_stock.stock_track_dmm_histo cascade;

create table suivi_stock.stock_track_dmm_histo (
	id_stock_track_dmm_histo serial4 not null,
	id_dim_produit_stock_track_fk int8 null,
	date_report date not null,
	date_report_prev date not null,
	dmm float4 null,
	constraint unique_id_stock_track_dmm_histo primary key (id_stock_track_dmm_histo),
	constraint id_stock_track_dmm_histo_id_dim_produit_stock_track_fk foreign key (id_dim_produit_stock_track_fk) references suivi_stock.dim_produit_stock_track(id_dim_produit_stock_track_pk) on
delete
	cascade
);

create index idx_date_report_dmm_histo on
suivi_stock.stock_track_dmm_histo
	using btree (date_report);
-- Drop table

-- drop table if exists suivi_stock.stock_track_detaille cascade;

create table suivi_stock.stock_track_detaille (
	stock_track_detaille_pk serial4 not null,
	id_dim_produit_stock_track_fk int8 null,
	date_limite_consommation date null,
	qte_physique float4 null,
	qte_livrable float4 null,
	date_report date null,
	constraint unique_id_stock_track_detaille_pk primary key (stock_track_detaille_pk),
	constraint stock_track_detaille_id_dim_produit_stock_track_fk foreign key (id_dim_produit_stock_track_fk) references suivi_stock.dim_produit_stock_track(id_dim_produit_stock_track_pk) on
delete
	cascade
);

create index idx_date_report_detaille on
suivi_stock.stock_track_detaille
	using btree (date_report);

-- drop table if exists suivi_stock.stock_track_prevision cascade;

create table suivi_stock.stock_track_prevision (
	id_stock_track_prevision_pk serial4 not null,
	id_dim_produit_stock_track_fk int8 null,
	stock_central float4 null,
	dmm_central float4 null,
	stock_prev_central varchar(250) null,
	stock_national float4 null,
	cmm_national float4 null,
	stock_prev_national varchar(250) null,
	peremption_prev_central_qte float4 null,
	peremption_prev_central_msd float4 null,
	peremption_prev_central_usd float4 null,
	period_prev date null,
	date_report date null,
	constraint unique_id_stock_track_prevision_pk primary key (id_stock_track_prevision_pk),
	constraint stock_track_prevision_id_dim_produit_stock_track_fk foreign key (id_dim_produit_stock_track_fk) references suivi_stock.dim_produit_stock_track(id_dim_produit_stock_track_pk) on
delete
	cascade
);

create index idx_date_report_prevision on
suivi_stock.stock_track_prevision
	using btree (date_report);

-- drop table if exists suivi_stock.stock_track_npsp cascade;

create table suivi_stock.stock_track_npsp (
	id_stock_track_npsp_pk serial4 not null,
	code_produit int8 null,
	designation text null,
	contenance varchar(250),
	dmm varchar(250),
	traceurs int8,
	stock_theorique_bke float4 null,
	stock_theorique_abj float4 null,
	stock_theorique_central float4 null,
	stock_theorique_fin_mois float4 null,
	msd varchar(250) null,
	statut_stock varchar(250) null,
	nb_jour_rupture varchar(250) null,
	programme varchar(20) null,
	date_report date null,
	constraint unique_id_stock_track_npsp_pk primary key (id_stock_track_npsp_pk),
	constraint dim_produit_stock_track_npsp_programme_fk foreign key (programme) references dap_tools.dim_programme("Programme") on
delete
	cascade
);

create index idx_date_report_stock_track_npsp on
suivi_stock.stock_track_npsp
	using btree (date_report);
--- plan d'approvisionnement

-- drop table if exists suivi_stock.plan_approv cascade;

create table suivi_stock.plan_approv (
	id_plan_approv_pk serial4 not null,
    standard_product_code int8,
    id_produit_qat int8,
    designation VARCHAR(255),
    id_envoi_qat VARCHAR(250),
    centrale_achat VARCHAR(250),
    source_financement VARCHAR(100),
    status VARCHAR(250),
    quantite float4,
    facteur_conversion_qat_vers_sage float4,
    quantite_harmonisee_sage float4,
    date DATE,
    cout_produits float4,
    cout_fret float4,
    cout_total float4,
    date_report date,
    programme varchar(20) null,
    constraint unique_id_plan_approv_pk primary key (id_plan_approv_pk),
    constraint dim_produit_plan_approv_programme_fk foreign key (programme) references dap_tools.dim_programme("Programme") on
delete
	cascade
);

create index idx_date_report_plan_approv on
suivi_stock.plan_approv
	using btree (date_report);
-- Recherche des produits d'état de stock

drop view suivi_stock.etat_stock;

create or replace
view suivi_stock.etat_stock
as (
select
	eds.*,
	dp.*,
	dpst.type_produit,
	dp2."Programme"
from
	dap_tools.etat_de_stock eds
join dap_tools.dim_produit dp on
	eds.id_produit_fk = dp.id_produit_pk
join dap_tools.dim_sous_programme dsp on
	dp."Code_sous_prog" = dsp."Code_sous_prog"
join dap_tools.dim_programme dp2 on
	dsp."Programme" = dp2."Programme"
join suivi_stock.dim_produit_stock_track dpst on
	dpst.code_produit = cast(dp."Code_produit" as INTEGER)
where
	dpst.programme = dsp."Programme"
	--and eds.etat_stock not in ('EN BAS DU PCU', 'ENTRE PCU et MIN')
);

drop view if exists suivi_stock.view_dispo_district;

create or replace
view suivi_stock.view_dispo_district
as (
select
	--	dp."Code_produit",
	--	dpst.programme,
	dpst.id_dim_produit_stock_track_pk as id_dim_produit_stock_track_fk,
	dd."Code_district",
	--	dd."District",
	DATE_TRUNC('month',
	eds.date_report) as date_report,
	SUM(eds.sdu) as sdu,
	SUM(eds.cmm_gest) as cmm_gest,
	case
		when SUM(eds.cmm_gest) = 0
			or SUM(eds.cmm_gest) is null then null
			else SUM(eds.sdu) / SUM(eds.cmm_gest)
		end as msd
	from
		dap_tools.etat_de_stock eds
	join dap_tools.dim_produit dp on
		eds.id_produit_fk = dp.id_produit_pk
	join dap_tools.dim_sous_programme dsp on
		dp."Code_sous_prog" = dsp."Code_sous_prog"
	join dap_tools.dim_programme dp2 on
		dsp."Programme" = dp2."Programme"
	join suivi_stock.dim_produit_stock_track dpst
    on
		dpst.code_produit = dp."Code_produit"::INTEGER
		and dpst.programme = dsp."Programme"
	join dap_tools.dim_structure ds on
		eds."Code_ets" = ds."Code_ets"::INTEGER
	join dap_tools.dim_district dd on
		ds."Code_district" = dd."Code_district"
	group by
		--	dp."Code_produit",
		--	dpst.programme,
		id_dim_produit_stock_track_fk,
		dd."Code_district",
		--	dd."District",
		date_report
	having
		date_report > '2024-12-01'
	order by
		date_report
);
--select distinct etat_stock
--from dap_tools.etat_de_stock eds
-- suivi_stock.share_link definition
-- Drop table


-- drop table if exists suivi_stock.share_link;

create table suivi_stock.share_link (
	id_share_link_pk serial4 not null,
	programme varchar(20) null,
	download_url text null,
	date_report date null,
	constraint unique_id_share_link primary key (id_share_link_pk),
	constraint share_link_programme_fkey foreign key (programme) references dap_tools.dim_programme("Programme") on
delete
	cascade
);

create index idx_date_report_share_link on
suivi_stock.share_link
	using btree (date_report);


-- ajouté récemment pour capter les informations du coût unitaire moyen qat
-- et de la version du plan d'approvisionnement et de la date d'extraction
alter table suivi_stock.plan_approv 
	add column cout_unitaire_moyen_qat float4,
	add column version_pa INTEGER,
	add column date_extraction_pa date;