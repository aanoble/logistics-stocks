import re
import unicodedata
from pathlib import Path
from typing import Set

import pandas as pd


def standardize_text(text: str) -> str:
    """
    Standardise une chaîne de caractères en supprimant les accents, 
    en retirant les caractères indésirables, en remplaçant les espaces par des underscores et en passant en majuscules.

    Args:
        text (str): La chaîne de caractères à standardiser.

    Returns:
        str: La chaîne standardisée.
    """
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"[^\w\s-]", "", text).strip().replace(" ", "_").upper()
    return text


def load_expected_sites_from_excel(excel_path: Path, required_cols: Set[str]) -> pd.DataFrame:
    """
    Charge la première feuille d'un fichier Excel contenant l'ensemble des colonnes requises pour les sites attendus.

    Args:
        excel_path (Path): Chemin vers le fichier Excel contenant les données.
        required_cols (Set[str]): Ensemble des noms de colonnes obligatoires.

    Returns:
        pd.DataFrame: DataFrame contenant les données de la première feuille valide avec toutes les colonnes requises.

    Raises:
        ValueError: Si aucune feuille ne contient toutes les colonnes requises.
    """
    with pd.ExcelFile(excel_path) as xls:
        for sheet_name in xls.sheet_names:
            try:
                df = xls.parse(
                    sheet_name=sheet_name,
                    usecols=lambda col: col in required_cols,
                    nrows=0,
                )
                if required_cols.issubset(df.columns):
                    full_df = xls.parse(sheet_name=sheet_name, usecols=list(required_cols))
                    full_df = full_df.loc[full_df["Code"].notna()]
                    return full_df.replace(0, "NA").fillna("NA")
            except (KeyError, ValueError) as e:
                print(f"Erreur dans la feuille {sheet_name}: {str(e)}")
                continue

    raise ValueError("Aucune feuille valide trouvée avec toutes les colonnes requises")


def load_traceable_products_from_excel(excel_path: str) -> pd.DataFrame:
    """
    Charge et transforme les colonnes pertinentes depuis la première feuille valide d'un fichier Excel 
    pour les produits traceurs.

    Args:
        excel_path (str): Chemin vers le fichier Excel à traiter.

    Returns:
        pd.DataFrame: DataFrame contenant les colonnes transformées et les colonnes additionnelles.
    
    Raises:
        ValueError: Si aucune feuille valide avec les colonnes requises n'est trouvée dans le fichier Excel.
    """
    COLUMN_MAPPING = {
        r"PROGRAMME": "PROGRAMME",
        r"CODE": "CODE PRODUIT",
        r"PRODUIT": "PRODUIT",
        r"DESIGNATION": "PRODUIT",
    }
    with pd.ExcelFile(excel_path) as xls:
        pattern = re.compile(r"|".join(COLUMN_MAPPING.keys()), flags=re.IGNORECASE)
        for sheet_name in xls.sheet_names:
            try:
                df = pd.read_excel(
                    xls,
                    sheet_name=sheet_name,
                    usecols=lambda col: bool(pattern.search(col)),
                    dtype="string",
                )
                if df.empty:
                    continue

                df.columns = [standardize_text(col) for col in df.columns]
                df.rename(
                    columns=lambda col: next(
                        (v for k, v in COLUMN_MAPPING.items() if re.search(k, col, re.I)), col
                    ),
                    inplace=True,
                )
                df["CODE COMBINE"] = df["CODE PRODUIT"] + "_" + df["PROGRAMME"]
                df["CODE PRODUIT"] = df["CODE PRODUIT"].astype("Int64")
                df["CATEGORIE PRODUIT"] = "Produit traceur"
                return df

            except Exception as e:
                print(f"Erreur dans la feuille {sheet_name}: {str(e)}")
                continue

    raise ValueError(f"Aucune feuille valide avec les colonnes requises trouvée dans {excel_path}")
