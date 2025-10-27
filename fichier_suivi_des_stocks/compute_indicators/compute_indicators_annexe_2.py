import math
import numpy as np
import pandas as pd
from IPython.display import display


def _get_etat_stock_first_part(
    df_etat_stock: pd.DataFrame,
    df_dmm_curent: pd.DataFrame,
    df_stock_prog_nat: pd.DataFrame,
    df_etat_stock_periph: pd.DataFrame,
    date_report: str,
) -> pd.DataFrame:
    """
    Cette fonction calcule les indicateurs de la feuilles Annexe 2 - Consolidation
    """

    df_etat_stock["SDU_CENTRAL"] = df_etat_stock["Stock Théorique Final SAGE"]

    assert (
        df_etat_stock.merge(
            df_dmm_curent[["id_dim_produit_stock_track_fk", "dmm_calculee"]],
            left_on="id_dim_produit_stock_track_pk",
            right_on="id_dim_produit_stock_track_fk",
            how="left",
        )
        .rename(columns={"dmm_calculee": "DMM_CENTRAL"})
        .shape[0]
        == df_etat_stock.shape[0]
    )

    df_etat_stock = (
        df_etat_stock.merge(
            df_dmm_curent[["id_dim_produit_stock_track_fk", "dmm_calculee"]].drop_duplicates(),
            left_on="id_dim_produit_stock_track_pk",
            right_on="id_dim_produit_stock_track_fk",
            how="left",
        )
        .rename(columns={"dmm_calculee": "DMM_CENTRAL"})
        .drop(columns="id_dim_produit_stock_track_fk")
    )

    df_etat_stock["MSD_CENTRAL"] = df_etat_stock.apply(
        lambda row: 0
        if row["SDU_CENTRAL"] == 0
        else "ND"
        if row["DMM_CENTRAL"] == 0
        else row["SDU_CENTRAL"] / row["DMM_CENTRAL"],
        axis=1,
    )

    df_etat_stock["STATUT_CENTRAL"] = df_etat_stock.apply(
        lambda row: "Rupture"
        if row["SDU_CENTRAL"] == 0
        else "Stock dormant"
        if row["DMM_CENTRAL"] == 0
        else "Sous-Stock"
        if row["MSD_CENTRAL"] < 3
        else "SurStock"
        if row["MSD_CENTRAL"] > 8
        else "Bien Stocké",
        axis=1,
    )

    df_etat_stock["date_report"] = pd.to_datetime(date_report, format="%Y-%m-%d")

    assert (
        df_etat_stock.merge(
            df_stock_prog_nat[["Code_produit", "CONSO", "SDU", "CMM", "MSD"]].rename(
                columns={
                    "CONSO": "CONSO_DECENTRALISE",
                    "SDU": "SDU_DECENTRALISE",
                    "CMM": "CMM_DECENTRALISE",
                    "MSD": "MSD_DECENTRALISE",
                }
            ),
            left_on="code_produit",
            right_on="Code_produit",
            how="left",
        )
        .drop(columns="Code_produit")
        .shape[0]
        == df_etat_stock.shape[0]
    )

    df_etat_stock = df_etat_stock.merge(
        df_stock_prog_nat[["Code_produit", "CONSO", "SDU", "CMM", "MSD"]].rename(
            columns={
                "CONSO": "CONSO_DECENTRALISE",
                "SDU": "SDU_DECENTRALISE",
                "CMM": "CMM_DECENTRALISE",
                "MSD": "MSD_DECENTRALISE",
            }
        ),
        left_on="code_produit",
        right_on="Code_produit",
        how="left",
    ).drop(columns="Code_produit")

    # math.ceil(row.CONSO/row.cdtmt) if not pd.isna(row.cdtmt) and row.cdtmt!=0

    for col in ["CONSO_DECENTRALISE", "SDU_DECENTRALISE", "CMM_DECENTRALISE"]:
        df_etat_stock[col] = df_etat_stock.apply(
            lambda row: math.ceil(row[col] / row["facteur_de_conversion"])
            if not pd.isna(row[col]) and row["facteur_de_conversion"] != 0
            else 0,
            axis=1,
        )
        df_etat_stock[col] = df_etat_stock[col].fillna(0)

    df_etat_stock["MSD_DECENTRALISE"] = (
        df_etat_stock["MSD_DECENTRALISE"]
        .apply(
            lambda x: float(x.replace(",", ".")) if "," in str(x) else float(x) if x != "NA" else x
        )
        .fillna(0)
    )

    df_etat_stock["STATUT_DECENTRALISE"] = df_etat_stock.apply(
        lambda row: "Rupture"
        if row["SDU_DECENTRALISE"] == 0
        else "Stock dormant"
        if row["CMM_DECENTRALISE"] == 0
        else "Sous-Stock"
        if row["MSD_DECENTRALISE"] < 2
        else "SurStock"
        if row["MSD_DECENTRALISE"] > 4
        else "Bien Stocké",
        axis=1,
    )
    df_etat_stock["nombre_de_site_en_rupture_annexe_2"] = df_etat_stock["code_produit"].apply(
        lambda x: df_etat_stock_periph.loc[
            (df_etat_stock_periph.Code_produit == x)
            & (df_etat_stock_periph.etat_stock == "RUPTURE")
        ].shape[0]
    )

    df_etat_stock["SDU_NATIONAL"] = df_etat_stock["SDU_CENTRAL"] + df_etat_stock["SDU_DECENTRALISE"]

    df_etat_stock["CMM_NATIONAL"] = df_etat_stock["CMM_DECENTRALISE"]

    df_etat_stock["MSD_NATIONAL"] = df_etat_stock.apply(
        lambda row: 0
        if row["SDU_NATIONAL"] == 0
        else "ND"
        if row["CMM_NATIONAL"] == 0
        else row["SDU_NATIONAL"] / row["CMM_NATIONAL"],
        axis=1,
    )

    df_etat_stock["STATUT_NATIONAL"] = df_etat_stock.apply(
        lambda row: "Rupture"
        if row["SDU_NATIONAL"] == 0
        else "Stock dormant"
        if row["CMM_NATIONAL"] == 0
        else "Sous-Stock"
        if row["MSD_NATIONAL"] < 5
        else "SurStock"
        if row["MSD_NATIONAL"] > 12
        else "Bien Stocké",
        axis=1,
    )

    # display(df_etat_stock.head(3))

    return df_etat_stock.round(0)


def _get_etat_stock_second_part(
    df_etat_stock: pd.DataFrame,
    df_stock_detaille: pd.DataFrame,
    df_receptions: pd.DataFrame,
    df_plan_approv: pd.DataFrame,
    date_report: str,
) -> pd.DataFrame:
    """
    Cette fonction permet de calculer les indicateurs de la feuille Annexe - 2 Consilidation séconde partie
    """
    code_col = [col for col in df_stock_detaille.columns if "CODE" in str(col).upper()][0]

    eomonth = (pd.to_datetime(date_report).replace(day=1) + pd.offsets.MonthEnd(0)).strftime(
        "%Y-%m-%d"
    )
    date_report = pd.to_datetime(date_report, format="%Y-%m-%d")
    df_etat_stock["Date de Péremption la plus proche (BRUTE)"] = df_etat_stock[
        "code_produit"
    ].apply(
        lambda x: df_stock_detaille.loc[
            (df_stock_detaille[code_col] == x) & (df_stock_detaille["Qté \nPhysique"] > 0),
            "Date limite de consommation",
        ].min()
        if not df_stock_detaille.loc[
            (df_stock_detaille[code_col] == x) & (df_stock_detaille["Qté \nPhysique"] > 0)
        ].empty
        else np.nan
    )

    df_etat_stock["Date de Péremption la plus proche"] = df_etat_stock[
        "Date de Péremption la plus proche (BRUTE)"
    ].apply(lambda x: np.nan if pd.isna(x) else x)

    df_etat_stock["Quantité correspondante"] = df_etat_stock.apply(
        lambda row: np.nan
        if pd.isna(row["Date de Péremption la plus proche"])
        else df_stock_detaille.loc[
            (df_stock_detaille[code_col] == row["code_produit"])
            & (
                df_stock_detaille["Date limite de consommation"]
                == row["Date de Péremption la plus proche"]
            ),
            "Qté \nPhysique",
        ].sum()
        if not df_stock_detaille.loc[
            (df_stock_detaille[code_col] == row["code_produit"])
            & (
                df_stock_detaille["Date limite de consommation"]
                == row["Date de Péremption la plus proche"]
            )
        ].empty
        else np.nan,
        axis=1,
    )

    def divide_if_error(x, y):
        try:
            return x / y
        except Exception:
            return "NA"

    df_etat_stock["MSD correspondant"] = df_etat_stock.apply(
        lambda row: ""
        if pd.isna(row["Quantité correspondante"])
        else divide_if_error(row["Quantité correspondante"], row["DMM_CENTRAL"]),
        axis=1,
    )

    df_etat_stock["Qtité réceptionnés non en Stock Annexe 2"] = df_etat_stock["code_produit"].apply(
        lambda x: df_receptions.loc[
            (df_receptions["Nouveau code"] == x)
            & (
                (df_receptions["Date_entree_machine"] > date_report)
                | (df_receptions["Date_entree_machine"].isna())
            ),
            "Quantité réceptionnée",
        ].sum()
    )

    df_etat_stock["MSD reçu Annexe 2"] = df_etat_stock.apply(
        lambda row: row["Qtité réceptionnés non en Stock Annexe 2"] / row.DMM_CENTRAL
        if not pd.isna(row.DMM_CENTRAL) and row.DMM_CENTRAL != 0
        else 0,
        axis=1,
    )

    df_etat_stock["Date Probable de Livraison Annexe 2"] = df_etat_stock.apply(
        lambda row: df_plan_approv.loc[
            (df_plan_approv["Standard product code"] == row["code_produit"])
            & (df_plan_approv["DATE"] >= eomonth),
            "DATE",
        ].min()
        if not df_plan_approv.loc[
            (df_plan_approv["Standard product code"] == row["code_produit"])
            & (df_plan_approv["DATE"] >= eomonth)
        ].empty
        else "",
        axis=1,
    )
    # code_col = [col for col in df_receptions.columns if 'CODE' in str(col).upper()][0]
    df_etat_stock["Date Effective de Livraison Annexe 2"] = df_etat_stock.apply(
        lambda row: df_receptions.loc[
            (df_receptions["Nouveau code"] == row["code_produit"])
            & (
                (df_receptions["Date_entree_machine"] >= date_report)
                | (df_receptions["Date_entree_machine"].isna())
            ),
            "Date de réception effective",
        ].max(),
        axis=1,
    )

    df_etat_stock["Qtité attendue Annexe 2"] = df_etat_stock.apply(
        lambda row: np.nan
        if pd.isna(row["Date Probable de Livraison Annexe 2"])
        else df_plan_approv.loc[
            (df_plan_approv["Standard product code"] == row["code_produit"])
            & (df_plan_approv["DATE"] == row["Date Probable de Livraison Annexe 2"]),
            "Quantité harmonisée (SAGE)",
        ].sum(),
        axis=1,
    )

    df_etat_stock["MSD attendu Annexe 2"] = df_etat_stock.apply(
        lambda row: row["Qtité attendue Annexe 2"] / row.DMM_CENTRAL
        if not pd.isna(row.DMM_CENTRAL)
        and row.DMM_CENTRAL != 0
        and row["Qtité attendue Annexe 2"] != ""
        else 0,
        axis=1,
    )

    df_etat_stock["code_and_date_concate"] = df_etat_stock.apply(
        lambda row: str(int(row["code_produit"]))
        + "_"
        + str(row["Date Probable de Livraison Annexe 2"]).replace(" 00:00:00", "")
        if not pd.isna(row["code_produit"])
        else "_" + str(row["Date Probable de Livraison Annexe 2"]).replace(" 00:00:00", "")
        if pd.isna(row["code_produit"]) and not pd.isna(row["Date Probable de Livraison Annexe 2"])
        else np.nan,
        axis=1,
    )

    def get_financement(row):
        try:
            index = df_plan_approv.loc[
                df_plan_approv.code_and_date_concate == row.code_and_date_concate
            ].index[0]
            return df_plan_approv["Source Financement"].iloc[index]
        except Exception:
            return ""

    df_etat_stock["Financement Annexe 2"] = df_etat_stock.apply(
        lambda row: get_financement(row), axis=1
    )

    def get_delivery_status(row):
        try:
            index = df_plan_approv.loc[
                df_plan_approv.code_and_date_concate == row.code_and_date_concate
            ].index[0]
            return df_plan_approv["Status"].iloc[index]
        except Exception:
            return ""

    df_etat_stock["Delivery status Annexe 2"] = df_etat_stock.apply(
        lambda row: get_delivery_status(row), axis=1
    )

    df_etat_stock["Delivery status Annexe 2"] = (
        df_etat_stock["Delivery status Annexe 2"]
        .replace("ReÃ§u", "Recu")
        .replace("ApprouvÃ©", "Approuved")
    )

    df_etat_stock.drop(columns="code_and_date_concate", inplace=True)
    # display(df_etat_stock.head(3))
    return df_etat_stock.round(0)


def _get_etat_stock_end_part(
    df_etat_stock: pd.DataFrame,
) -> pd.DataFrame:
    """
    Cette fonction ainsi définie permet d'avoir la suite des calculs de la feuille Annexe - 2 Consolidation
    """

    cols = [
        "Analyse du risque / Commentaires",
        "Diligences au niveau Central",
        "Diligences au niveau périphérique",
        "Responsable",
        "Dilig. Choisie",
    ]

    for col in cols:
        df_etat_stock[f"{col} Annexe 2"] = ""

    del col, cols

    df_etat_stock.rename(
        columns={
            "Distribution effectuée": "distribution_effectuee",
            "Quantité reçue entrée en stock": "quantite_recue_stock",
            "Quantité de PPI": "quantite_ppi",
            "Quantité prélévée en Contrôle Qualité (CQ)": "quantite_prelevee_cq",
            "Ajustement de stock": "ajustement_stock",
            "Stock Théorique Final SAGE": "stock_theorique_final_sage",
            "Stock Théorique Final Attendu": "stock_theorique_final_attendu",
            "ECARTS": "ecarts",
            "Justification des écarts": "justification_ecarts",
            "Diligences": "diligences",
            "SDU_CENTRAL": "sdu_central_annexe_2",
            "DMM_CENTRAL": "dmm_central_annexe_2",
            "MSD_CENTRAL": "msd_central_annexe_2",
            "STATUT_CENTRAL": "statut_central_annexe_2",
            "CONSO_DECENTRALISE": "conso_decentralise_annexe_2",
            "SDU_DECENTRALISE": "sdu_decentralise_annexe_2",
            "CMM_DECENTRALISE": "cmm_decentralise_annexe_2",
            "MSD_DECENTRALISE": "msd_decentralise_annexe_2",
            "STATUT_DECENTRALISE": "statut_decentralise_annexe_2",
            "SDU_NATIONAL": "sdu_national_annexe_2",
            "CMM_NATIONAL": "cmm_national_annexe_2",
            "MSD_NATIONAL": "msd_national_annexe_2",
            "STATUT_NATIONAL": "statut_national_annexe_2",
            "Date de Péremption la plus proche (BRUTE)": "date_peremption_plus_proche_brute_annexe_2",
            "Date de Péremption la plus proche": "date_peremption_plus_proche_annexe_2",
            "Quantité correspondante": "quantite_correspondante_annexe_2",
            "MSD correspondant": "msd_correspondant_annexe_2",
            "Qtité attendue Annexe 2": "quantite_attendue_annexe_2",
            "MSD attendu Annexe 2": "msd_attendu_annexe_2",
            "Qtité réceptionnés non en Stock Annexe 2": "quantite_non_stockee_annexe_2",
            "MSD reçu Annexe 2": "msd_recu_annexe_2",
            "Financement Annexe 2": "financement_annexe_2",
            "Date Probable de Livraison Annexe 2": "date_probable_livraison_annexe_2",
            "Date Effective de Livraison Annexe 2": "date_effective_livraison_annexe_2",
            "Delivery status Annexe 2": "statut_annexe_2",
            "Analyse du risque / Commentaires Annexe 2": "analyse_risque_commentaires_annexe_2",
            "Diligences au niveau Central Annexe 2": "diligences_central_annexe_2",
            "Diligences au niveau périphérique Annexe 2": "diligences_peripherique_annexe_2",
            "Responsable Annexe 2": "responsable_annexe_2",
            "Dilig. Choisie Annexe 2": "dilig_choisie_annexe_2",
            "date_report": "date_report",
        },
        inplace=True,
    )

    df_etat_stock = df_etat_stock[
        [
            "id_dim_produit_stock_track_pk",
            "stock_theorique_mois_precedent",
            "distribution_effectuee",
            "quantite_recue_stock",
            "quantite_ppi",
            "quantite_prelevee_cq",
            "ajustement_stock",
            "stock_theorique_final_sage",
            "stock_theorique_final_attendu",
            "ecarts",
            "justification_ecarts",
            "diligences",
            # "dilig_choisie",
            "sdu_central_annexe_2",
            "dmm_central_annexe_2",
            "msd_central_annexe_2",
            "statut_central_annexe_2",
            "conso_decentralise_annexe_2",
            "sdu_decentralise_annexe_2",
            "cmm_decentralise_annexe_2",
            "msd_decentralise_annexe_2",
            "statut_decentralise_annexe_2",
            "nombre_de_site_en_rupture_annexe_2",
            "sdu_national_annexe_2",
            "cmm_national_annexe_2",
            "msd_national_annexe_2",
            "statut_national_annexe_2",
            "date_peremption_plus_proche_brute_annexe_2",
            "date_peremption_plus_proche_annexe_2",
            "quantite_correspondante_annexe_2",
            "msd_correspondant_annexe_2",
            "quantite_attendue_annexe_2",
            "msd_attendu_annexe_2",
            "quantite_non_stockee_annexe_2",
            "msd_recu_annexe_2",
            # "tx_satisfaction_annexe_2",
            "financement_annexe_2",
            "date_probable_livraison_annexe_2",
            # "duree_transit_annexe_2",
            "date_effective_livraison_annexe_2",
            # "retard_livraison_annexe_2",
            # "delivery_status_annexe_2",
            # "jours_rupture_avant_livraison_npsp_annexe_2",
            # "risque_rupture_annexe_2",
            # "risque_peremption_annexe_2",
            "statut_annexe_2",
            "analyse_risque_commentaires_annexe_2",
            "diligences_central_annexe_2",
            "diligences_peripherique_annexe_2",
            "responsable_annexe_2",
            "dilig_choisie_annexe_2",
            "date_report",
        ]
    ].rename(columns={"id_dim_produit_stock_track_pk": "id_dim_produit_stock_track_fk"})

    # Formattage des champs MSD
    cols = [col for col in df_etat_stock.columns if "msd" in col]
    for col in cols:
        df_etat_stock[col] = df_etat_stock[col].apply(
            lambda x: str(round(float(x), 1)).replace(".", ",")
            if not pd.isna(x) and x != "ND" and x != "NA" and x != ""
            else x
        )

    # Formattage du champ date
    cols = [col for col in df_etat_stock.columns if "date" in col]

    for col in cols:
        try:
            # df_etat_stock[col] = df_etat_stock[col].str.replace('T00:00:00Z', '')
            df_etat_stock[col] = df_etat_stock[col].apply(
                lambda x: pd.to_datetime(str(x)[:10], format="%Y-%m-%d")
                if len(str(x)) >= 10
                else np.nan
            )
        except Exception:
            # df_etat_stock[col] = df_etat_stock[col].apply(lambda x: pd.to_datetime(str(x)[:10], format="%Y-%m-%d"))
            pass

    del cols

    display(df_etat_stock.head(3))

    return df_etat_stock.round(0)


def compute_indicators_annexe_2(
    df_etat_stock: pd.DataFrame,
    df_dmm_curent: pd.DataFrame,
    df_stock_prog_nat: pd.DataFrame,
    df_etat_stock_periph: pd.DataFrame,
    df_stock_detaille: pd.DataFrame,
    df_receptions: pd.DataFrame,
    df_plan_approv: pd.DataFrame,
    date_report: str,
) -> pd.DataFrame:
    """
    Cette fonction permet de calculer les indicateurs de la feuille Annexe - 2 Consolidation
    """

    df_etat_stock = _get_etat_stock_first_part(
        df_etat_stock, df_dmm_curent, df_stock_prog_nat, df_etat_stock_periph, date_report
    )

    df_etat_stock = _get_etat_stock_second_part(
        df_etat_stock, df_stock_detaille, df_receptions, df_plan_approv, date_report
    )

    df_etat_stock = _get_etat_stock_end_part(df_etat_stock)

    return df_etat_stock.round(0)
