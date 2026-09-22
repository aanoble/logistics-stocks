from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from dateutil import parser as dtparser
from fuzzywuzzy import fuzz, process
from openhexa.sdk import workspace


def format_date(month_report: str, year_report: int):
    """Formatage du champs date_report sur base du mois défini."""
    month_number = {
        "Janvier": "01-",
        "Février": "02-",
        "Mars": "03-",
        "Avril": "04-",
        "Mai": "05-",
        "Juin": "06-",
        "Juillet": "07-",
        "Août": "08-",
        "Septembre": "09-",
        "Octobre": "10-",
        "Novembre": "11-",
        "Décembre": "12-",
    }
    date_report = (
        "01-" + month_number[month_report] + str(year_report)
    )  # str(datetime.today().year)
    return pd.to_datetime(date_report, format="%d-%m-%Y").strftime("%Y-%m-%d")


def format_file_path(file_path):
    return workspace.files_path + "/" + file_path.split("workspace/")[-1].lstrip("/")


BORNE = datetime(2262, 1, 1)
PLAFOND = pd.Timestamp(f"{pd.Timestamp.max.year - 1}-12-31")
FORMATS = ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%y", "%d/%m/%Y %H:%M:%S")


def normaliser_dlc(x: object) -> pd.Timestamp:
    """Normalise une date de péremption.

    Returns:
        pd.Timestamp: La date normalisée, ou ``pd.NaT`` si la valeur est invalide.
    """
    if pd.isna(x) or (isinstance(x, str) and not x.strip()):  # type: ignore
        return pd.NaT  # type: ignore

    if isinstance(x, (pd.Timestamp, datetime, date)):
        dt = datetime(x.year, x.month, x.day)

    elif isinstance(x, (int, float, np.integer, np.floating)):
        dt = datetime(1899, 12, 30) + timedelta(days=float(x))

    else:
        texte = str(x).strip()
        dt = None
        for fmt in FORMATS:
            try:
                dt = datetime.strptime(texte, fmt)
                break
            except ValueError:
                continue
        if dt is None:
            try:
                dt = dtparser.parse(texte, dayfirst=True)
            except (ValueError, OverflowError):
                return pd.NaT  # type: ignore

    return PLAFOND if dt >= BORNE else pd.Timestamp(dt)


def check_if_sheet_name_in_file(
    sheet_name: str, sheet_names: list, threshold: int = 95
) -> str | None:
    """Vérifie si une feuille est présente dans le fichier fourni.

    Dans le cas contraire, une recherche par correspondance floue est effectuée
    pour trouver un nom similaire qui pourrait être mal orthographié.

    Returns:
        str | None: Le nom de la feuille correspondante, ou ``None``.
    """
    try:
        if sheet_name in sheet_names:
            return sheet_name

        best_match = process.extractOne(sheet_name, sheet_names, scorer=fuzz.token_set_ratio)
        if best_match[1] >= threshold:  # type: ignore
            return best_match[0]  # type: ignore
        return None
    except Exception:
        return None
