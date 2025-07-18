import os
import re
from datetime import datetime
from pathlib import PosixPath

import numpy as np
import pandas as pd


def process_pa_files(fp_plan_approv: PosixPath, fp_map_prod: PosixPath, programme: str):
    """
    Process and merge plan approval files with product mapping data.
    Args:
        fp_plan_approv (PosixPath): Path to the directory or file containing plan approval CSV files.
        fp_map_prod (PosixPath): Path to the Excel file containing product mapping data.
        programme (str): The sheet name in the Excel file to be used for product mapping.
    Returns:
        pd.DataFrame: A DataFrame containing the processed and merged data.
    """
    # Liste pour accumuler les données
    data = []

    # Fonction pour traiter un fichier unique
    def _process_pa_file(fichier):
        nonlocal data
        with open(file=fichier) as file:
            for line in file.readlines():
                if len(line.split(",")) == 17:
                    data.append([col.replace('"', "") for col in line.strip().split(",")])

    if os.path.isdir(fp_plan_approv):
        for root, _, files in os.walk(fp_plan_approv):
            for file in files:
                if file.endswith(".csv"):
                    _process_pa_file(os.path.join(root, file))
    else:
        _process_pa_file(fp_plan_approv)

    df_plan_approv = pd.DataFrame(data[1:], columns=data[0])

    # Bad index
    bad_index = df_plan_approv.loc[
        df_plan_approv["ID de produit QAT / Identifiant de produit (prévision)"]
        == "ID de produit QAT / Identifiant de produit (prévision)"
    ].index

    df_plan_approv.drop(index=bad_index, inplace=True)

    for col in ["ID de produit QAT / Identifiant de produit (prévision)", "ID de l`envoi QAT"]:
        try:
            df_plan_approv[col] = df_plan_approv[col].astype("Int64")
        except Exception:
            pass

    for col in [
        "Coût unitaire de produit (USD)",
        "Coût du fret (USD)",
        "Quantité",
        "Coût total (USD)",
    ]:
        try:
            df_plan_approv[col] = df_plan_approv[col].astype(float)
        except Exception:
            pass

    try:
        df_plan_approv["date de réception"] = df_plan_approv["date de réception"].apply(
            lambda date_str: datetime.strptime(date_str, "%d-%b-%Y")
        )
    except Exception:
        pass

    # Nettoyage des espaces blancs dans les données
    df_plan_approv = df_plan_approv.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # Charger le fichier de mappage des produits
    df_map_prod = pd.read_excel(fp_map_prod, sheet_name=programme)  # ou pd.read_csv selon le type
    df_map_prod.columns = (
        df_map_prod.columns.str.replace("Ã©", "é").str.replace("â", "").str.rstrip().str.lstrip()
    )

    df_map_prod.rename(
        columns={
            "Code QAT": "ID de produit QAT / Identifiant de produit (prévision)",
            "Code standard national": "Standard product code",
            "Coût unitaire moyen (en dollar)": "cout_unitaire_moyen_qat", 
            "Facteur de conversion QAT vers SAGE": "facteur_de_conversion_qat_sage",
            "Acronym": "acronym",
        },
        inplace=True,
    )

    df_map_prod = df_map_prod.drop_duplicates()

    df_plan_approv = df_plan_approv.merge(
        df_map_prod[
            [
                "ID de produit QAT / Identifiant de produit (prévision)",
                "Standard product code",
                "cout_unitaire_moyen_qat",
                "facteur_de_conversion_qat_sage",
                "acronym",
            ]
        ],
        on="ID de produit QAT / Identifiant de produit (prévision)",
        how="left",
    )
    
    df_plan_approv.rename(
        columns={
            "ID de produit QAT / Identifiant de produit (prévision)": "ID de produit QAT",
            "Produit (planification) / Produit (prévision)": "Produits",
            "État": "Status",
            "Quantité": "Quantite",
            "date de réception": "DATE",
            "Coût unitaire de produit (USD)": "Cout des Produits",
            "Coût du fret (USD)": "Couts du fret",
            "Coût total (USD)": "Couts totaux",
        },
        inplace=True,
    )

    def _get_code_and_date_concate(row):
        try:
            if not pd.isna(row["Standard product code"]):
                return (
                    str(int(row["Standard product code"]))
                    + "_"
                    + str(row["DATE"]).replace(" 00:00:00", "")
                )
            elif pd.isna(row["Standard product code"]) and not pd.isna(row["DATE"]):
                return "_" + str(row["DATE"]).replace(" 00:00:00", "")
            else:
                return np.nan
        except Exception:
            try:
                return "_" + str(row["DATE"]).replace(" 00:00:00", "")
            except Exception:
                return np.nan

    df_plan_approv["code_and_date_concate"] = df_plan_approv.apply(
        lambda row: _get_code_and_date_concate(row), axis=1
    )

    return df_plan_approv


def process_etat_stock_npsp(
    df_etat_stock_npsp: pd.DataFrame, date_report: str, programme: str
) -> pd.DataFrame:
    """
    Traite le DataFrame de l'état des stocks NPSP en renommant les colonnes,
    en ajoutant la date du rapport et le programme, et en sélectionnant les colonnes pertinentes.
    Args:
        df_etat_stock_npsp (pd.DataFrame): Le DataFrame contenant les données de l'état des stocks NPSP.
        date_report (str): La date du rapport au format 'YYYY-MM-DD'.
        programme (str): Le programme associé aux données.
    Returns:
        pd.DataFrame: Le DataFrame traité avec les colonnes renommées, la date du rapport et le programme ajoutés.
    """

    COLUMN_MAPPING = {
        r"Nouveau code": "code_produit",
        r"Nouvelle désignation": "designation",
        r"Contenance": "contenance",
        r"DMM": "dmm",
        r"Traceurs": "traceurs",
        r"MSD": "msd",
        r"Statut du stock": "statut_stock",
        r"Stock théorique BOUAKE": "stock_theorique_bke",
        r"Stock théorique ABIDJAN": "stock_theorique_abj",
        r"Stock théorique CENTRALE": "stock_theorique_central",
        r"Stock théorique fin": "stock_theorique_fin_mois",
        r"Nombre de jour de rupture": "nb_jour_rupture",
    }

    df_etat_stock_npsp.rename(
        columns=lambda col: next(
            (v for k, v in COLUMN_MAPPING.items() if re.search(k, col, re.I)), col
        ),
        inplace=True,
    )

    df_etat_stock_npsp["date_report"] = pd.to_datetime(date_report, format="%Y-%m-%d")

    cols = [
        col
        for col in [
            "date_report",
            "code_produit",
            "designation",
            "contenance",
            "dmm",
            "traceurs",
            "stock_theorique_bke",
            "stock_theorique_abj",
            "stock_theorique_central",
            "stock_theorique_fin_mois",
            "msd",
            "statut_stock",
        ]
        if col in df_etat_stock_npsp.columns
    ]
    df_etat_stock_npsp = df_etat_stock_npsp[cols]

    df_etat_stock_npsp["programme"] = programme

    return df_etat_stock_npsp
