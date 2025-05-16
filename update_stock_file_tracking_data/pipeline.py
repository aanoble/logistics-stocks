"""Template for newly generated pipelines."""

from openhexa.sdk import workspace, current_run, pipeline, parameter
import papermill as pm


@pipeline("update-stock-file-tracking-data", name="Pipeline de Mise a Jour des donnes Fichier Suivi de Stocks")

@parameter(
    "file_path_suivi_stock",
    name="Chemin d'acces au fichier Suivi de Stock Mis a Jour",
    type=str, required=True,
    help="Chemin d'acces au fichier Suivi de Stock Mis a Jour uploader au prealable sur OpenHexa.",
)


@parameter(
    "date_report",
    name="Date de conception du rapport",
    type=str, required=True,
    help="Mois de conception du Rapport",
    choices=[
        "Janvier", "Fevrier", "Mars",
        "Avril", "Mai", "Juin", "Juillet",
        "Aout", "Septembre", "Octobre",
        "Novembre", "Decembre"]
)

@parameter(
    "programme",
    name="Programme",
    type=str, required=True,
    help="Programme pour lequel on concoit le rapport",
    choices=["PNLP", "PNLS", "PNLT", "PNN", "PNSME"]
)

def update_stock_file_tracking_data(file_path_suivi_stock, date_report, programme):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    current_run.log_info("Execute Jupyter Notebook for Fichier Suivi de Stock Data Updating")
    run_notebook(file_path_suivi_stock, date_report, programme, '/automating_dap_tools/code/stock_tracking_file/pipeline_refresh_data')
    current_run.log_info("Done for Execution!")
    
    current_run.log_info("Execute Jupyter Notebook for Visuel Data Updating")
    run_notebook(file_path_suivi_stock, date_report, programme, '/automating_dap_tools/code/pipeline_refresh_visuel')
    current_run.log_info("Done for Execution!")
    
@update_stock_file_tracking_data.task
def run_notebook(file_path_suivi_stock, date_report, programme, file_path_notebook):
    """Put some data processing code here."""
    pm.execute_notebook(workspace.files_path + file_path_notebook + '/main_program.ipynb', 
                        workspace.files_path + file_path_notebook + '/output_main_program.ipynb',
                        parameters={
                            "file_path": file_path_suivi_stock,
                            "date_report": date_report,
                            "programme": programme
                        }
    )
    

if __name__ == "__main__":
    update_stock_file_tracking_data()