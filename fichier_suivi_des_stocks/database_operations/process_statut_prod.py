from typing import Any

import pandas as pd


def process_statut_prod(df: pd.DataFrame, schema_name: str, stock_sync_manager: Any):
    """
    Traite un DataFrame de produits selon leur statut (Ajout/Suppression/Modification)
    et synchronise avec une table de base de données.

    Args:
        df (pd.DataFrame): DataFrame contenant les données produits brutes
        schema_name (str): Nom du schéma SQL cible
        stock_sync_manager (module): Module de gestion des synchronisations
    """

    # Renommage des colonnes
    col_rename = {
        "CODE": "code_produit",
        "Ancien code": "ancien_code",
        "CATEGORIE": "categorie",
        "DESIGNATION DU PRODUIT": "designation",
        "Type": "type_produit",
        "Unité niv Central": "unit_niveau_central",
        "Unité niv Périphérique": "unit_niveau_peripherique",
        "Facteur de conversion (De la centrale à la périphérie)": "facteur_de_conversion",
        "Statut Produit": "statut_produit",
    }
    df = df.rename(columns=col_rename)

    if df.empty:
        return

    # Gestion des différents statuts
    def process_status(status, operation):
        filtered_df = df[df["statut_produit"] == status]
        if not filtered_df.empty:
            operation(filtered_df)

    # Opération d'ajout
    def add_operation(df_ajout):
        stock_sync_manager.insert_dataframe_to_table(
            df=df_ajout.drop(columns="statut_produit"), table_name="dim_produit_stock_track"
        )

    # Opération de suppression
    def delete_operation(df_suppression):
        delete_query = f"""
        DELETE FROM {schema_name}.dim_produit_stock_track
        WHERE code_produit = %s AND programme = %s
        """
        params = [
            [
                stock_sync_manager.convert_numpy_types(row["code_produit"]),
                stock_sync_manager.convert_numpy_types(row["programme"]),
            ]
            for _, row in df_suppression.iterrows()
        ]
        stock_sync_manager.civ_cursor.executemany(delete_query, params)
        stock_sync_manager.conn.commit()

    # Opération de modification
    def update_operation(df_modification):
        update_query = f"""
        UPDATE {schema_name}.dim_produit_stock_track 
        SET ancien_code = %s,
            categorie = %s,
            designation = %s,
            type_produit = %s,
            unit_niveau_central = %s,
            unit_niveau_peripherique = %s,
            facteur_de_conversion = %s
        WHERE code_produit = %s AND programme = %s
        """
        keys = [
            "ancien_code",
            "categorie",
            "designation",
            "type_produit",
            "unit_niveau_central",
            "unit_niveau_peripherique",
            "facteur_de_conversion",
            "code_produit",
            "programme",
        ]

        params = [
            [stock_sync_manager.convert_numpy_types(row[k]) for k in keys]
            for _, row in df_modification.iterrows()
        ]

        stock_sync_manager.civ_cursor.executemany(update_query, params)
        stock_sync_manager.conn.commit()

    # Execution des opérations
    process_status("Ajout", add_operation)
    process_status("Suppression", delete_operation)
    process_status("Modification", update_operation)
