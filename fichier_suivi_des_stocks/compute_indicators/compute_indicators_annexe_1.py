import math
from typing import Tuple

import numpy as np
import pandas as pd
from IPython.display import display
from sqlalchemy import Engine


def get_etat_stock_current_month(
    df_etat_stock: pd.DataFrame,
    df_stock_detaille: pd.DataFrame,
    df_distribution: pd.DataFrame,
    df_ppi: pd.DataFrame,
    df_prelevement: pd.DataFrame,
    df_receptions: pd.DataFrame,
    date_report: str,
) -> pd.DataFrame:
    """
    Calcule et enrichit les indicateurs de stock pour le mois courant à partir de plusieurs sources de données.
    Cette fonction prend en entrée plusieurs DataFrames contenant des informations sur l'état du stock, les détails du stock,
    les distributions, les PPI, les prélèvements, et les réceptions, ainsi qu'une date de rapport. Elle calcule pour chaque
    produit les quantités distribuées, reçues, de PPI, prélevées pour contrôle qualité, le stock théorique final attendu et SAGE,
    les écarts, et ajoute des colonnes pour la justification et les diligences.

    Args:
        df_etat_stock (pd.DataFrame): DataFrame principal contenant l'état du stock pour chaque produit.
        df_stock_detaille (pd.DataFrame): DataFrame contenant les détails du stock physique par produit.
        df_distribution (pd.DataFrame): DataFrame contenant les distributions effectuées par produit.
        df_ppi (pd.DataFrame): DataFrame contenant les quantités de PPI par produit.
        df_prelevement (pd.DataFrame): DataFrame contenant les prélèvements pour contrôle qualité par produit.
        df_receptions (pd.DataFrame): DataFrame contenant les réceptions de produits en stock.
        date_report (str): Date du rapport au format "YYYY-MM-DD".

    Returns:
        pd.DataFrame: DataFrame enrichi avec les indicateurs calculés pour le mois courant, incluant les colonnes :
            - Distribution effectuée
            - Quantité reçue entrée en stock
            - Quantité de PPI
            - Quantité prélévée en Contrôle Qualité (CQ)
            - Ajustement de stock
            - Stock Théorique Final SAGE
            - Stock Théorique Final Attendu
            - ECARTS
            - Justification des écarts
            - Diligences
            - Dilig. Choisie
    Raises:
        KeyError: Si les colonnes attendues ne sont pas présentes dans les DataFrames d'entrée.
        Exception: Pour toute erreur lors de la conversion des types de colonnes.
    """
    try:
        df_etat_stock["Distribution effectuée"] = df_etat_stock["code_produit"].apply(
            lambda x: df_distribution.loc[df_distribution.Article == x, "Quantité livrée"].sum()
        )
    except KeyError:
        df_etat_stock["Distribution effectuée"] = df_etat_stock["code_produit"].apply(
            lambda x: df_distribution.loc[df_distribution.Article == x, "Qté livrée"].sum()
        )

    # A modifier ici
    date_report_dt = pd.to_datetime(date_report, format="%Y-%m-%d")

    df_etat_stock["Quantité reçue entrée en stock"] = df_etat_stock["code_produit"].apply(
        lambda x: df_receptions.loc[
            (df_receptions["Nouveau code"] == x)
            & (df_receptions["Date_entree_machine"].dt.month == date_report_dt.month)
            & (df_receptions["Date_entree_machine"].dt.year == date_report_dt.year),
            "Quantité réceptionnée",
        ].sum()
    )

    code_col = [col for col in df_ppi.columns if "CODE" in str(col).upper()][0]
    df_etat_stock["Quantité de PPI"] = df_etat_stock["code_produit"].apply(
        lambda x: df_ppi.loc[df_ppi[code_col] == x, "Quantité"].sum()
    )

    code_col = [col for col in df_prelevement.columns if "CODE" in str(col).upper()][0]
    df_etat_stock["Quantité prélévée en Contrôle Qualité (CQ)"] = df_etat_stock[
        "code_produit"
    ].apply(lambda x: df_prelevement.loc[df_prelevement[code_col] == x, "Quantité"].sum())

    df_etat_stock["Ajustement de stock"] = np.nan

    code_col = [col for col in df_stock_detaille.columns if "CODE" in str(col).upper()][0]
    # col_stock_theo = [col for col in df_etat_stock.columns if 'Stock Théorique fin' in str(col)][0]

    df_etat_stock["Stock Théorique Final SAGE"] = df_etat_stock["code_produit"].apply(
        lambda x: df_stock_detaille.loc[df_stock_detaille[code_col] == x, "Qté \nPhysique"].sum()
    )

    # del df_distribution, df_ppi, df_prelevement ,df_receptions, df_stock_detaille

    df_etat_stock["Stock Théorique Final Attendu"] = df_etat_stock.apply(
        lambda row: np.nan
        if pd.isna(row["Stock Théorique Final SAGE"])
        else (
            -row["Distribution effectuée"]
            + row["Quantité reçue entrée en stock"]
            - row["Quantité de PPI"]
            - row["Quantité prélévée en Contrôle Qualité (CQ)"]
        )
        if pd.isna(row["stock_theorique_mois_precedent"])
        else (
            row["stock_theorique_mois_precedent"]
            - row["Distribution effectuée"]
            + row["Quantité reçue entrée en stock"]
            - row["Quantité de PPI"]
            - row["Quantité prélévée en Contrôle Qualité (CQ)"]
        ),
        axis=1,
    )

    df_etat_stock["ECARTS"] = (
        df_etat_stock["Stock Théorique Final SAGE"] - df_etat_stock["Stock Théorique Final Attendu"]
    )

    df_etat_stock["Justification des écarts"] = ""

    df_etat_stock["Diligences"] = ""

    df_etat_stock["Dilig. Choisie"] = ""

    display(df_etat_stock.head(3))

    return df_etat_stock


def get_dmm_current_month(
    df_etat_stock: pd.DataFrame,
    programme: str,
    date_report: str,
    engine: Engine,
    schema_name: str = "suivi_stock",
    auto_computed_dmm: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Obtient les informations de DMM du mois courant.

    Args:
        df_etat_stock (pd.DataFrame): DataFrame contenant les données d'état de stock.
        programme (str): Nom du programme.
        date_report (str): Date de rapport (format 'YYYY-MM-DD').
        engine: Objet de connexion SQLAlchemy.
        schema_name (str, optional): Schéma de la base de données. Defaults to "suivi_stock".
        auto_computed_dmm (bool, optional): Indique si la DMM doit être calculée automatiquement. Defaults to True.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: df_dmm_current et df_dmm_histo.
    """
    cols_prod = [
        "id_dim_produit_stock_track_pk",
        "code_produit",
        "ancien_code",
        "categorie",
        "designation",
        "type_produit",
        "unit_niveau_central",
        "unit_niveau_peripherique",
        "facteur_de_conversion",
        "programme",
        "designation_acronym",
    ]

    date_report_prec = (
        pd.to_datetime(date_report).replace(day=1) - pd.offsets.MonthBegin()
    ).strftime("%Y-%m-%d")

    # Checking des informations de la DMM
    df_dmm_current = (
        df_etat_stock[cols_prod + ["Distribution effectuée"]]
        .rename(columns={"Distribution effectuée": "dmm"})
        .drop_duplicates()
    )

    df_dmm_current["date_report"] = pd.to_datetime(date_report, format="%Y-%m-%d")

    # Récupération des informations DMM du mois précédent depuis la BDD
    query_dmm_past = f"""
        SELECT prod.*, st_dmm.nbre_mois_consideres
        FROM {schema_name}.stock_track_dmm st_dmm
        INNER JOIN {schema_name}.dim_produit_stock_track prod 
            ON st_dmm.id_dim_produit_stock_track_fk = prod.id_dim_produit_stock_track_pk
        WHERE prod.programme='{programme}' AND st_dmm.date_report = '{date_report_prec}'
    """
    df_dmm_past = pd.read_sql(query_dmm_past, engine)

    assert (
        df_dmm_current.shape[0]
        == df_dmm_current.merge(df_dmm_past, on="id_dim_produit_stock_track_pk", how="left").shape[
            0
        ]
    )

    df_dmm_current = df_dmm_current.merge(
        df_dmm_past[["id_dim_produit_stock_track_pk", "nbre_mois_consideres"]],
        on="id_dim_produit_stock_track_pk",
        how="left",
    )
    # A ce niveau il faudra récupérer les mois pour lesquels les distributions ont été validées (cochées)
    # Récupération des informations d'historique des distributions validées
    query_dmm_histo = f"""
        SELECT prod.*, st_dmm_histo.*
        FROM {schema_name}.stock_track_dmm_histo st_dmm_histo
        INNER JOIN {schema_name}.dim_produit_stock_track prod
            ON st_dmm_histo.id_dim_produit_stock_track_fk = prod.id_dim_produit_stock_track_pk
        WHERE prod.programme='{programme}' AND st_dmm_histo.date_report = '{date_report_prec}'
    """
    df_dmm_histo = pd.read_sql(query_dmm_histo, engine)

    df_dmm_histo["date_report"] = pd.to_datetime(date_report, format="%Y-%m-%d")
    df_dmm_histo["date_report_prev"] = pd.to_datetime(df_dmm_histo["date_report_prev"])
    df_dmm_histo["date_report_prev_min"] = df_dmm_histo.groupby("id_dim_produit_stock_track_pk")[
        "date_report_prev"
    ].transform("min")

    df_dmm_current = df_dmm_current.merge(
        df_dmm_histo[["id_dim_produit_stock_track_pk", "date_report_prev_min"]].drop_duplicates(),
        on="id_dim_produit_stock_track_pk",
        how="left",
    )

    def compute_distributions(row, auto_computed_dmm: bool = True):
        if pd.isna(row.nbre_mois_consideres):
            return np.nan
        # Générer la série des mois (en début de mois) entre date_report_prev_min et date_report
        months = pd.date_range(start=row.date_report_prev_min, end=row.date_report, freq="MS")
        months = months[1:] if auto_computed_dmm else months[:-1]
        # Somme des distributions validées pour les mois correspondants
        total = df_dmm_histo.loc[
            (df_dmm_histo.id_dim_produit_stock_track_pk == row.id_dim_produit_stock_track_pk)
            & (df_dmm_histo.date_report_prev.isin(months)),
            "dmm",
        ].sum()
        value = total + row.dmm if auto_computed_dmm else total
        return value

    # Vectorisation de la mise à jour de nbre_mois_consideres :
    # 1. Pour les lignes où nbre_mois_consideres est NaN et dmm n'est pas NaN, on assigne 1.
    df_dmm_current["nbre_mois_consideres"] = np.where(
        df_dmm_current["nbre_mois_consideres"].isna() & df_dmm_current["dmm"].notna(),
        1,
        df_dmm_current["nbre_mois_consideres"],
    )
    # 2. Pour les valeurs non-NaN et différentes de 6, on ajoute 1.
    mask = df_dmm_current["nbre_mois_consideres"].notna() & (
        df_dmm_current["nbre_mois_consideres"].astype(int) != 6
    )
    df_dmm_current.loc[mask, "nbre_mois_consideres"] += 1

    df_dmm_current["distributions_mois_consideres"] = df_dmm_current.apply(
        lambda row: compute_distributions(row, auto_computed_dmm), axis=1
    )

    # Calcul de la DMM calculée
    df_dmm_current["dmm_calculee"] = df_dmm_current.apply(
        lambda row: row.distributions_mois_consideres / row.nbre_mois_consideres
        if pd.notna(row.nbre_mois_consideres) and row.nbre_mois_consideres != 0
        else np.nan,
        axis=1,
    )

    df_dmm_current["commentaire"] = ""

    df_dmm_current = df_dmm_current[
        [
            "id_dim_produit_stock_track_pk",
            "date_report",
            "dmm",
            "nbre_mois_consideres",
            "distributions_mois_consideres",
            "dmm_calculee",
            "commentaire",
        ]
    ].rename(columns={"id_dim_produit_stock_track_pk": "id_dim_produit_stock_track_fk"})

    if auto_computed_dmm:
        df_dmm_histo = df_dmm_histo.loc[
            df_dmm_histo["date_report_prev"] > df_dmm_histo["date_report_prev_min"],
            ["id_dim_produit_stock_track_fk", "date_report", "date_report_prev", "dmm"],
        ]

        df_dmm_current["date_report_prev"] = df_dmm_current["date_report"]
        cols = ["id_dim_produit_stock_track_fk", "date_report", "date_report_prev", "dmm"]
        df_dmm_histo = pd.concat(
            [
                df_dmm_histo[cols],
                df_dmm_current[cols],
            ],
            ignore_index=True,
        ).sort_values(["id_dim_produit_stock_track_fk", "date_report_prev"])

        df_dmm_current.drop(columns="date_report_prev", inplace=True)
    else:
        cols = ["id_dim_produit_stock_track_fk", "date_report", "date_report_prev", "dmm"]
        df_dmm_histo = df_dmm_histo[cols].sort_values(
            ["id_dim_produit_stock_track_fk", "date_report_prev"]
        )

    return df_dmm_current.round(2), df_dmm_histo.round(2)


def get_cmm_current_month(
    df_etat_stock: pd.DataFrame,
    df_stock_prog_nat: pd.DataFrame,
    programme: str,
    date_report: str,
    engine: Engine,
    schema_name: str = "suivi_stock",
    auto_computed_cmm: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Obtient les informations de DMM du mois courant.

    Args:
        df_etat_stock (pd.DataFrame): DataFrame contenant les données d'état de stock.
        df_stock_prog_nat (pd.DataFrame): DataFrame contenant les données de stock du programme au niveau périphérique.
        programme (str): Nom du programme.
        date_report (str): Date de rapport (format 'YYYY-MM-DD').
        engine: Objet de connexion SQLAlchemy.
        schema_name (str, optional): Schéma de la base de données. Defaults to "suivi_stock".
        auto_computed_cmm (bool, optional): Indique si la CMM doit être calculée automatiquement. Defaults to True.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: df_cmm_current et df_cmm_histo.
    """

    cols_prod = [
        "id_dim_produit_stock_track_pk",
        "code_produit",
        "ancien_code",
        "categorie",
        "designation",
        "type_produit",
        "unit_niveau_central",
        "unit_niveau_peripherique",
        "facteur_de_conversion",
        "programme",
        "designation_acronym",
    ]

    date_report_prec = (
        pd.to_datetime(date_report).replace(day=1) - pd.offsets.MonthBegin()
    ).strftime("%Y-%m-%d")

    # Checking des informations de la CMM
    df_cmm_current = df_etat_stock[cols_prod]

    df_cmm_current["date_report"] = pd.to_datetime(date_report, format="%Y-%m-%d")

    assert (
        df_cmm_current.merge(
            df_stock_prog_nat[["Code_produit", "CONSO"]],
            left_on="code_produit",
            right_on="Code_produit",
            how="left",
        )
        .drop(columns="Code_produit")
        .shape[0]
        == df_cmm_current.shape[0]
    )

    df_cmm_current = df_cmm_current.merge(
        df_stock_prog_nat[["Code_produit", "CONSO"]],
        left_on="code_produit",
        right_on="Code_produit",
        how="left",
    ).drop(columns="Code_produit")

    df_cmm_current["CONSO"] = df_cmm_current["CONSO"].fillna(0)

    df_cmm_current["cmm"] = df_cmm_current.apply(
        lambda row: math.ceil(row.CONSO / row.facteur_de_conversion)
        if not pd.isna(row.facteur_de_conversion) and row.facteur_de_conversion != 0
        else 0,
        axis=1,
    )

    df_cmm_current.drop(columns="CONSO", inplace=True)

    # Récupération des informations CMM du mois précédent depuis la BDD
    query_cmm_past = f"""
        SELECT prod.*, st_cmm.nbre_mois_consideres
        FROM {schema_name}.stock_track_cmm st_cmm
        INNER JOIN {schema_name}.dim_produit_stock_track prod ON st_cmm.id_dim_produit_stock_track_fk = prod.id_dim_produit_stock_track_pk
        WHERE prod.programme='{programme}' AND st_cmm.date_report = '{date_report_prec}'"""

    df_cmm_past = pd.read_sql(query_cmm_past, engine)

    assert (
        df_cmm_current.shape[0]
        == df_cmm_current.merge(df_cmm_past, on="id_dim_produit_stock_track_pk", how="left").shape[
            0
        ]
    )

    df_cmm_current = df_cmm_current.merge(
        df_cmm_past[["id_dim_produit_stock_track_pk", "nbre_mois_consideres"]],
        on="id_dim_produit_stock_track_pk",
        how="left",
    )

    del df_cmm_past

    # A ce niveau il faudra récupérer les mois pour lesquels les distributions ont été validées (cochées)
    # Récupération des informations d'historique des distributions validées
    query_cmm_histo = f"""
        SELECT prod.*, st_cmm_histo.*
        FROM {schema_name}.stock_track_cmm_histo st_cmm_histo
        INNER JOIN {schema_name}.dim_produit_stock_track prod
            ON st_cmm_histo.id_dim_produit_stock_track_fk = prod.id_dim_produit_stock_track_pk
        WHERE prod.programme='{programme}' AND st_cmm_histo.date_report = '{date_report_prec}'
    """
    df_cmm_histo = pd.read_sql(query_cmm_histo, engine)

    df_cmm_histo["date_report"] = pd.to_datetime(date_report, format="%Y-%m-%d")
    df_cmm_histo["date_report_prev"] = pd.to_datetime(df_cmm_histo["date_report_prev"])
    df_cmm_histo["date_report_prev_min"] = df_cmm_histo.groupby("id_dim_produit_stock_track_pk")[
        "date_report_prev"
    ].transform("min")

    df_cmm_current = df_cmm_current.merge(
        df_cmm_histo[["id_dim_produit_stock_track_pk", "date_report_prev_min"]].drop_duplicates(),
        on="id_dim_produit_stock_track_pk",
        how="left",
    )

    def compute_consommations(row, auto_computed_cmm: bool = True):
        if pd.isna(row.nbre_mois_consideres):
            return np.nan
        # Générer la série des mois (en début de mois) entre date_report_prev_min et date_report
        months = pd.date_range(start=row.date_report_prev_min, end=row.date_report, freq="MS")
        months = months[1:] if auto_computed_cmm else months[1:]
        # Somme des distributions validées pour les mois correspondants
        total = df_cmm_histo.loc[
            (df_cmm_histo.id_dim_produit_stock_track_pk == row.id_dim_produit_stock_track_pk)
            & (df_cmm_histo.date_report_prev.isin(months)),
            "cmm",
        ].sum()
        value = total + row.cmm if auto_computed_cmm else total
        return value

    # Vectorisation de la mise à jour de nbre_mois_consideres :
    df_cmm_current["nbre_mois_consideres"] = np.where(
        df_cmm_current["nbre_mois_consideres"].isna() & df_cmm_current["cmm"].notna(),
        1,
        df_cmm_current["nbre_mois_consideres"],
    )
    mask = df_cmm_current["nbre_mois_consideres"].notna() & (
        df_cmm_current["nbre_mois_consideres"].astype(int) != 6
    )
    df_cmm_current.loc[mask, "nbre_mois_consideres"] += 1

    df_cmm_current["conso_mois_consideres"] = df_cmm_current.apply(
        lambda row: compute_consommations(row, auto_computed_cmm), axis=1
    )

    df_cmm_current["cmm_calculee"] = df_cmm_current.apply(
        lambda row: row.conso_mois_consideres / row.nbre_mois_consideres
        if not pd.isna(row.nbre_mois_consideres) and row.nbre_mois_consideres != 0
        else np.nan,
        axis=1,
    )

    df_cmm_current["commentaire"] = ""

    df_cmm_current = df_cmm_current[
        [
            "id_dim_produit_stock_track_pk",
            "date_report",
            "cmm",
            "nbre_mois_consideres",
            "conso_mois_consideres",
            "cmm_calculee",
            "commentaire",
        ]
    ].rename(columns={"id_dim_produit_stock_track_pk": "id_dim_produit_stock_track_fk"})

    if auto_computed_cmm:
        df_cmm_histo = df_cmm_histo.loc[
            df_cmm_histo["date_report_prev"] > df_cmm_histo["date_report_prev_min"],
            ["id_dim_produit_stock_track_fk", "date_report", "date_report_prev", "cmm"],
        ]
        df_cmm_current["date_report_prev"] = df_cmm_current["date_report"]

        cols = ["id_dim_produit_stock_track_fk", "date_report", "date_report_prev", "cmm"]

        df_cmm_histo = pd.concat(
            [
                df_cmm_histo[cols],
                df_cmm_current[cols],
            ],
            ignore_index=True,
        ).sort_values(["id_dim_produit_stock_track_fk", "date_report_prev"])

        df_cmm_current.drop(columns="date_report_prev", inplace=True)
    else:
        cols = ["id_dim_produit_stock_track_fk", "date_report", "date_report_prev", "cmm"]
        df_cmm_histo = df_cmm_histo[cols].sort_values(
            ["id_dim_produit_stock_track_fk", "date_report_prev"]
        )

    return df_cmm_current.round(2), df_cmm_histo.round(2)
