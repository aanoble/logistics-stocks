from datetime import datetime

import papermill as pm
from pathlib import Path
from openhexa.sdk import current_run, pipeline, workspace


@pipeline("backup_schema_database")
def backup_schema_database():
    """Backs up the schema of the database by executing a notebook.

    This function triggers the execution of a notebook that handles the backup process
    for the database schema. It does not take any parameters and does not return any value.

    Returns:
        None

    Raises:
        Any exception raised by the run_notebook function.
    """
    run_notebook()


@backup_schema_database.task
def run_notebook():
    """Run Notebook papermill"""
    current_run.log_info("Run Jupyter Notebook Backup Schema Database")
    timestamp = datetime.now().strftime("%Y-%m-%d")
    papermill_output = Path(workspace.files_path, ".backups/output_notebook_execution")
    papermill_output.mkdir(parents=True, exist_ok=True)
    papermill_output = papermill_output / f"output_backup_schema_database_{timestamp}.ipynb"

    pm.execute_notebook(
        input_path=workspace.files_path + "/.backups/backup_schema_database.ipynb",
        output_path=papermill_output.as_posix(),
    )
    current_run.add_file_output(papermill_output.as_posix())
    current_run.log_info(f"Notebook executed successfully: {papermill_output.as_posix()}")


if __name__ == "__main__":
    backup_schema_database()
