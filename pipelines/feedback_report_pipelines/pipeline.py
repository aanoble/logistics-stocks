from datetime import datetime
from pathlib import Path

import pandas as pd
import papermill as pm
import requests
from openhexa.sdk import current_run, pipeline, workspace


# ============================================================================
# Correspondance entre le numéro du mois et son nom en français
# ============================================================================
MONTHS_FR = {
    1: "Janvier",
    2: "Février",
    3: "Mars",
    4: "Avril",
    5: "Mai",
    6: "Juin",
    7: "Juillet",
    8: "Août",
    9: "Septembre",
    10: "Octobre",
    11: "Novembre",
    12: "Décembre",
}


# ============================================================================
# Détermination automatique du mois et de l'année du rapport
#
# Exemple :
# - Exécution le 12 août 2026  -> Juillet 2026
# - Exécution le 12 janvier 2027 -> Décembre 2026
# ============================================================================
def get_reporting_period():

    # Date du jour
    today = datetime.today()

    # Dernier jour du mois précédent
    previous_month = (
        pd.Timestamp(today).replace(day=1)
        - pd.Timedelta(days=1)
    )

    month_report = MONTHS_FR[previous_month.month]
    report_year = previous_month.year

    return month_report, report_year


# ============================================================================
# Recherche automatique du fichier "Sites attendus"
# ============================================================================
def get_site_attendus_file(month_report, report_year):

    file_path = (
        Path(workspace.files_path)
        / "Rapport Feedback/data/Sites attendus"
        / f"Sites attendus {month_report.lower()} {report_year}.xlsx"
    )

    if not file_path.exists():
        raise FileNotFoundError(
            f"Le fichier '{file_path.name}' est introuvable."
        )

    current_run.log_info(
        f"Fichier Sites attendus trouvé : {file_path.name}"
    )

    return file_path.as_posix()


# ============================================================================
# Recherche automatique du fichier "Liste des Produits Traceurs"
# ============================================================================
def get_prod_traceurs_file(month_report, report_year):

    file_path = (
        Path(workspace.files_path)
        / "Rapport Feedback/data/Produits Traceurs"
        / f"Liste des Produits Traceurs {month_report} {report_year}.xlsx"
    )

    if not file_path.exists():
        raise FileNotFoundError(
            f"Le fichier '{file_path.name}' est introuvable."
        )

    current_run.log_info(
        f"Fichier Produits Traceurs trouvé : {file_path.name}"
    )

    return file_path.as_posix()


# ============================================================================
# Pipeline principal
# ============================================================================
@pipeline("feedback-report-pipelines")
def feedback_report_pipelines():
    """
    Pipeline autonome de génération du rapport Feedback.

    Le pipeline :
        - détermine automatiquement le mois du rapport ;
        - retrouve automatiquement les fichiers nécessaires ;
        - exécute le notebook principal ;
        - rafraîchit le rapport Power BI.
    """

    # Détermination automatique du mois et de l'année
    month_report, report_year = get_reporting_period()

    current_run.log_info(
        f"Rapport à générer : {month_report} {report_year}"
    )

    # Recherche automatique des fichiers nécessaires
    fp_site_attendus = get_site_attendus_file(
        month_report,
        report_year,
    )

    fp_prod_traceurs = get_prod_traceurs_file(
        month_report,
        report_year,
    )

    # Exécution du notebook
    run_notebook(
        month_report,
        fp_site_attendus,
        fp_prod_traceurs,
    )


# ============================================================================
# Exécution du notebook principal
# ============================================================================
@feedback_report_pipelines.task
def run_notebook(
    month_report,
    fp_site_attendus,
    fp_prod_traceurs,
):
    """
    Exécute le notebook principal avec Papermill.
    """

    current_run.log_info("Run jupyter notebook Main Program")

    timestamp = datetime.now().strftime("%Y-%m-%d")

    pm.execute_notebook(
        input_path=workspace.files_path
        + "/Rapport Feedback/code/pipelines/main_program.ipynb",

        output_path=workspace.files_path
        + f"/Rapport Feedback/code/pipelines/output_notebook_execution/output_{timestamp}.ipynb",

        parameters={
            "month_report": month_report,
            "fp_site_attendus": fp_site_attendus,
            "fp_prod_traceurs": fp_prod_traceurs,
        },
    )

    refresh_pbi_report()

    current_run.log_info("Done for Execution!")