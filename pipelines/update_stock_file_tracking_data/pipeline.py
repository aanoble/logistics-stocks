"""Template for newly generated pipelines."""

from datetime import datetime
from pathlib import Path
from openhexa.sdk import workspace, current_run, pipeline, parameter
import papermill as pm
import requests


@pipeline(
    "update-stock-file-tracking-data",
)
@parameter(
    "fp_suivi_stock",
    name="Fichier Suivi de Stock Mis à Jour à la suite de la réunion mensuelle",
    type=str,
    required=True,
    help="Ce fichier doit être chargé dans le dossier `Fichier Suivi de Stock/data/<programme>/Fichier Suivi de Stock`",
)
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
def update_stock_file_tracking_data(fp_suivi_stock, month_report, year_report, programme):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    current_run.log_info(
        f"Exécution du Jupyter Notebook pour la mise à jour des données du rapport PBI du programme {programme}"
    )
    run_notebook(
        fp_suivi_stock,
        month_report,
        year_report,
        programme,
    )


@update_stock_file_tracking_data.task
def run_notebook(fp_suivi_stock, month_report, year_report, programme):
    """Put some data processing code here."""
    timestamp = datetime.now().strftime("%Y-%m-%d")
    input_path = (
        Path(workspace.files_path)
        / "Fichier Suivi de Stock/code/pipelines/actualisation fichier suivi stock.ipynb"
    )
    output_path = (
        Path(workspace.files_path)
        / f"Fichier Suivi de Stock/code/pipelines/output_notebook_execution/actualisation fichier suivi stock/{programme}"
    )
    output_path.mkdir(parents=True, exist_ok=True)
    output_path = output_path / f"output_actualisation_fst_{timestamp}.ipynb"
    pm.execute_notebook(
        input_path=input_path.as_posix(),
        output_path=output_path.as_posix(),
        parameters={
            "fp_suivi_stock": fp_suivi_stock,
            "month_report": month_report,
            "year_report": year_report,
            "programme": programme,
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
    update_stock_file_tracking_data()
