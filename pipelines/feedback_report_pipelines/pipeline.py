from datetime import datetime

import papermill as pm
import requests
from openhexa.sdk import current_run, parameter, pipeline, workspace


@pipeline("feedback-report-pipelines")
@parameter(
    "month_report",
    name="Mois de conception du Rapport Feedback",
    type=str,
    required=True,
    help="Mois de conception du Rapport Feedback",
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
    "fp_site_attendus",
    name="Fichier de la liste des sites attendus",
    type=str,
    required=True,
    help="Fichier de la liste des sites Attendus chargé au préalable sur OpenHexa dans le dossier `Rapport Feedback/data/Sites attendus`.",
)
@parameter(
    "fp_prod_traceurs",
    name="Fichier de la liste des produits traceurs",
    type=str,
    required=True,
    help="Fichier de la liste des produits traceurs chargé au préalable sur OpenHexa dans le dossier `Rapport Feedback/data/Produits Traceurs`",
)
def feedback_report_pipelines(
    month_report,
    fp_site_attendus,
    fp_prod_traceurs,
):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    run_notebook(
        month_report,
        fp_site_attendus,
        fp_prod_traceurs,
    )


@feedback_report_pipelines.task
def run_notebook(
    month_report,
    fp_site_attendus,
    fp_prod_traceurs,
):
    """
    Run Notebook papermill
    """
    current_run.log_info("Run jupyter notebook Main Program")
    timestamp = datetime.now().strftime("%Y-%m-%d")
    pm.execute_notebook(
        input_path=workspace.files_path + "/Rapport Feedback/code/pipelines/main_program.ipynb",
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


def refresh_pbi_report(
    connection_name: str = "credentials-power-bi-api",
    report_name: str = "Rapport Feedback",
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
    feedback_report_pipelines()
