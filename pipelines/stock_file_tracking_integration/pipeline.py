"""Template for newly generated pipelines."""

from datetime import datetime
from pathlib import Path

import papermill as pm
import requests
from openhexa.sdk import File, current_run, parameter, pipeline, workspace


@pipeline("stock-file-tracking-integration")
@parameter(
    "month_report",
    name="Mois de conception du rapport",
    type=str,
    required=True,
    help="Mois de conception du Fichier Suivi des Stocks",
    choices=[
        "Janvier",
        "Février",
        "Mars",
        "Avril",
        "Mai",
        "Juin",
        "Juillet",
        "Août",
        "Septembre",
        "Octobre",
        "Novembre",
        "Décembre",
    ],
)
@parameter(
    "year_report",
    name="Année de conception du rapport",
    type=int,
    default=2025,
    required=True,
    help="Année de conception du Fichier Suivi des Stocks",
)
@parameter(
    "programme",
    name="Programme",
    type=str,
    required=True,
    help="Programme pour lequel on concoit le rapport",
    choices=["PNLP", "PNLS", "PNLT", "PNN", "PNSME"],
)
@parameter(
    "fp_etat_mensuel",
    name="Fichier etat du stock et de distribution",
    type=File,
    required=True,
    help="Ce fichier doit être chargé dans le dossier `Fichier Suivi de Stock/data/<programme>/Etat de Stock Mensuel`",
)
@parameter(
    "fp_plan_approv",
    name="Fichier du plan d'appro",
    type=str,
    required=True,
    help="Fichier ou dossier doit être chargé dans le dossier `Fichier Suivi de Stock/data/<programme>/Plan d'Approvisionnement`",
)
@parameter(
    "fp_map_prod",
    name="Fichier de mapping des produits QAT en SAGEX3",
    type=File,
    required=True,
    default="Fichier Suivi de Stock/data/Mapping QAT_SAGEX3_AOUT_2025.xlsx",
    help="Ce fichier doit être chargé dans le dossier `Fichier Suivi de Stock/data/`",
)
@parameter(
    "auto_computed_dmm",
    name="DMM calculé automatiquement",
    type=bool,
    required=False,
    default=False,
    help="Si coché, le choix des distributions des mois sont sélectionnées automatiquement. "
    "Si décoché, les valeurs du mois précédent sont utilisées.",
)
@parameter(
    "auto_computed_cmm",
    name="CMM calculé automatiquement",
    type=bool,
    required=False,
    default=True,
    help="Si coché, le choix de consommations des mois sont sélectionnées automatiquement. "
    "Si décoché, les valeurs du mois précédent sont utilisées.",
)
def stock_file_tracking_integration(
    month_report,
    year_report,
    programme,
    fp_etat_mensuel,
    fp_plan_approv,
    fp_map_prod,
    auto_computed_dmm,
    auto_computed_cmm,
):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    run_notebook(
        month_report,
        year_report,
        programme,
        fp_etat_mensuel,
        fp_plan_approv,
        fp_map_prod,
        auto_computed_dmm,
        auto_computed_cmm,
    )


@stock_file_tracking_integration.task
def run_notebook(
    month_report,
    year_report,
    programme,
    fp_etat_mensuel,
    fp_plan_approv,
    fp_map_prod,
    auto_computed_dmm,
    auto_computed_cmm,
):
    """Put some data processing code here."""
    current_run.log_info("Run jupyter notebook Main Program Fichier Suivi des Stocks Integration")
    timestamp = datetime.now().strftime("%Y-%m-%d")
    input_path = (
        Path(workspace.files_path)
        / "Fichier Suivi de Stock/code/pipelines/mise a jour fichier suivi stock.ipynb"
    )
    output_path = (
        Path(workspace.files_path)
        / f"Fichier Suivi de Stock/code/pipelines/output_notebook_execution/maj fichier suivi stock/{programme}"
    )
    output_path.mkdir(parents=True, exist_ok=True)
    output_path = output_path / f"output_{timestamp}.ipynb"
    pm.execute_notebook(
        input_path=input_path.as_posix(),
        output_path=output_path.as_posix(),
        parameters={
            "month_report": month_report,
            "year_report": year_report,
            "programme": programme,
            "fp_etat_mensuel": fp_etat_mensuel,
            "fp_plan_approv": fp_plan_approv,
            "fp_map_prod": fp_map_prod,
            "auto_computed_dmm": auto_computed_dmm,
            "auto_computed_cmm": auto_computed_cmm,
        },
    )
    refresh_pbi_report()
    current_run.log_info("Exécution terminée avec succès !")


def refresh_pbi_report(
    connection_name: str = "credentials-power-bi-api",
    report_name: str = "Suivi de Stock",
):
    """
    Déclenche le rafraîchissement du rapport Power BI avec gestion d'erreurs ciblée

    Args:
        connection_name: Nom de la connexion OpenHexa
        report_name: Nom exact du dataset Power BI
    """
    try:
        current_run.log_info("Initialisation du rafraîchissement des données du rapport Power BI")

        # Initialisation connexion
        conn = workspace.custom_connection(connection_name)
        credentials = eval(conn.credentials)
        group_id = conn.group_id

        # Récupération token
        token_url = (
            f"https://login.microsoftonline.com/{credentials['tenant_id']}/oauth2/v2.0/token"
        )
        token_data = {
            "client_id": credentials["client_id"],
            "client_secret": credentials["client_secret"],
            "scope": "https://analysis.windows.net/powerbi/api/.default",
            "grant_type": "client_credentials",
        }
        token_response = requests.post(token_url, data=token_data)
        token_response.raise_for_status()
        access_token = token_response.json()["access_token"]

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
        }

        # Recherche dataset
        datasets_response = requests.get(
            f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets",
            headers=headers,
            params={"filter": f"name eq '{report_name}'"},
        )
        datasets_response.raise_for_status()

        dataset = [
            dataset
            for dataset in datasets_response.json().get("value", [{}])
            if dataset.get("name") == report_name
        ][0]
        if not dataset.get("id"):
            current_run.log_error(f"❌ Dataset '{report_name}' non trouvé dans l'espace de travail")

        # Déclenchement rafraîchissement
        refresh_url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset['id']}/refreshes"
        refresh_response = requests.post(refresh_url, headers=headers)

        refresh_response.raise_for_status()

        # Récupération historique si succès
        history_url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset['id']}/refreshes"
        history_response = requests.get(history_url, headers=headers)
        history_response.raise_for_status()

        current_run.log_info("Rafraîchissement du rapport Power BI déclenché avec succès")
        # return pd.DataFrame(history_response.json()['value'])

    except requests.HTTPError as e:
        if e.response.status_code == 429:
            msg_critical = (
                "⚠️ Limite de rafraîchissements atteinte (erreur 429) "
                f"Message d'erreur : {e.response.json().get('error', {}).get('message', 'Pas de message d erreur')}"
            )
            current_run.log_critical(msg_critical)

        else:
            msg_error = (
                f"❌ Erreur HTTP {e.response.status_code}  "
                f"Détails : {e.response.json().get('error', {}).get('message', 'Pas de message d erreur')}"
            )
            current_run.log_error(msg_error)

    except Exception as e:
        current_run.log_error(f"❌ Erreur inattendue : {str(e)}")


if __name__ == "__main__":
    stock_file_tracking_integration()
