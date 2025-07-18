import pandas as pd
from openhexa.sdk import workspace


def format_date(month_report: str, year_report: int):
    from datetime import datetime

    """
    Formatage du champs date_report sur base du mois défini.
    """
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
    date_report = "01-" + month_number[month_report] + str(year_report) # str(datetime.today().year)
    return pd.to_datetime(date_report, format="%d-%m-%Y").strftime("%Y-%m-%d")


def format_file_path(file_path):
    return workspace.files_path + "/" + file_path.split("workspace/")[-1].lstrip("/")


def check_if_sheet_name_in_file(sheet_name, sheet_names, threshold=95):
    """
    Permet de vérifier si la feuille est présente dans la liste des classeurs du fichier qui a été fourni.
    Dans le cas contraire, un recherche par correspondance floue est effectuée pour voir s'il existe un semblable de nom qui se peut être mal orthographié pour le fournir en sortie.
    """
    from fuzzywuzzy import fuzz, process

    try:
        if sheet_name in sheet_names:
            return sheet_name

        best_match = process.extractOne(sheet_name, sheet_names, scorer=fuzz.token_set_ratio)
        if best_match[1] >= threshold:
            return best_match[0]
        return None
    except Exception:
        return None
