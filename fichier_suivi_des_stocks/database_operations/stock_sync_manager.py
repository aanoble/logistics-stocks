"""
Database Interaction Module

Provides optimized utilities for PostgreSQL database operations including:
- Connection management
- Data retrieval/insertion
- Type conversion handling
- Data synchronization
"""

from typing import Any, List, Optional

import numpy as np
import pandas as pd
import psycopg2

# from IPython.display import display
from openhexa.sdk import workspace
from sqlalchemy import Engine, MetaData, Table, create_engine, inspect
from sqlalchemy.dialects.postgresql import insert

# Database connection objects
civ_engine: Optional[Engine] = None
conn: Optional[psycopg2.extensions.connection] = None
civ_cursor: Optional[psycopg2.extensions.cursor] = None


def initialize_database_connection() -> None:
    """Initialize or refresh database connections using workspace credentials."""
    global civ_engine, conn, civ_cursor

    try:
        # Close existing connections
        if conn and not conn.closed:
            conn.close()
        if civ_engine:
            civ_engine.dispose()
    except Exception as e:
        print(f"Erreur de nettoyage de la connexion: {str(e)}")

    try:
        civ_engine = create_engine(workspace.database_url)
        conn = psycopg2.connect(
            dbname=workspace.database_name,
            user=workspace.database_username,
            password=workspace.database_password,
            host=workspace.database_host,
            port=workspace.database_port,
        )
        conn.autocommit = False
        civ_cursor = conn.cursor()
        print("Connexion à la base de données établie avec succès")
    except Exception as e:
        print(f"Tentative de connexion à la base de données échouée: {str(e)}")
        raise


def get_table_data(
    table_name: str = None, schema_name: str = "suivi_stock", query: Optional[str] = None
) -> pd.DataFrame:
    """
    Retrieve data from database table with schema validation.

    Args:
        table_name: Target table name
        schema_name: Database schema (default: suivi_stock)
        query: Custom SQL query (optional)

    Returns:
        pd.DataFrame: Resultset as DataFrame

    Raises:
        ValueError: On invalid query execution
    """
    try:
        if not civ_engine:
            initialize_database_connection()

        final_query = query or f"SELECT * FROM {schema_name}.{table_name}"
        return pd.read_sql(final_query, civ_engine)

    except Exception as e:
        print(f"Erreur d'exécution de la réquête d'accès aux données: {str(e)}")
        raise ValueError("Opération de base de données échouée") from e


def insert_dataframe_to_table(
    df: pd.DataFrame, table_name: str, schema_name: str = "suivi_stock", chunk_size: int = 1000
) -> str:
    """
    Insert DataFrame into database table with schema validation.

    Args:
        df: Input DataFrame
        table_name: Target table name
        schema_name: Database schema (default: suivi_stock)
        chunk_size: Batch insert size (default: 1000)

    Returns:
        str: Operation status

    Raises:
        ValueError: On schema mismatch
    """
    try:
        if not civ_engine:
            initialize_database_connection()

        # Get schema metadata
        table_columns = pd.read_sql(
            f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = '{schema_name}' 
            AND table_name = '{table_name}'
            """,
            civ_engine,
        )

        # Schema validation
        missing_columns = set(df.columns) - set(table_columns.column_name)
        if missing_columns:
            raise ValueError(f"Colonnes invalides: {missing_columns}")

        # Type conversion mapping
        type_mapping = {
            "integer": np.int32,
            "bigint": np.int64,
            "real": np.float64,
            "text": str,
            "character varying": str,
            "numeric": np.float64,
            "date": "datetime64[ns]",
        }

        # Column type enforcement
        dtype_mapping = {
            row["column_name"]: type_mapping.get(row["data_type"], str)
            for _, row in table_columns.iterrows()
        }

        df = df.astype({col: dtype for col, dtype in dtype_mapping.items() if col in df.columns})

        dtype_str = {
            col: dtype for col, dtype in dtype_mapping.items() if dtype is str and col in df.columns
        }
        for col in dtype_str:
            df[col] = df[col].replace({np.nan: ""})

        # Batch insert
        df.to_sql(
            name=table_name,
            con=civ_engine,
            schema=schema_name,
            if_exists="append",
            index=False,
            chunksize=chunk_size,
            method="multi",
        )

        return f"Insertion de {len(df)} enrégistrements réussie"

    except Exception as e:
        conn.rollback()
        print(f"Insertion d'insertion échouée: {str(e)}")
        raise


def convert_numpy_types(value: Any) -> Any:
    """Convert numpy datatypes to native Python types."""
    if isinstance(value, (np.integer)):
        return int(value)
    if isinstance(value, (np.floating)):
        return float(value)
    if isinstance(value, (np.bool_)):
        return bool(value)
    if isinstance(value, (np.datetime64)) or isinstance(
        value, pd._libs.tslibs.timestamps.Timestamp
    ):
        return pd.Timestamp(value).to_pydatetime()
    return value


def synchronize_product_metadata(
    source_df: pd.DataFrame, programme: str, schema_name: str = "suivi_stock"
) -> str:
    """
    Synchronize product metadata between source DataFrame and database.

    Args:
        source_df: Source data containing product updates
        programme: Programme filter
        schema_name: Database schema (default: suivi_stock)

    Returns:
        str: Operation status
    """
    try:
        # Validate input structure
        required_columns = {
            "Standard product code",
            "acronym",
            "facteur_de_conversion_qat_sage",
        }
        if not required_columns.issubset(source_df.columns):
            missing = required_columns - set(source_df.columns)
            raise ValueError(f"Missing columns: {missing}")

        # Get current database state
        db_products = get_table_data(
            schema_name=schema_name,
            query=f"SELECT * FROM {schema_name}.dim_produit_stock_track WHERE programme = '{programme}'",
        )
        source_df = (
            source_df[list(required_columns)]
            .rename(columns={"acronym": "designation_acronym"})
            .drop_duplicates()
        )
        source_df.astype(
            {col: db_products[col].dtype for col in db_products if col in source_df.columns}
        )

        for col in source_df.select_dtypes("O").columns:
            source_df[col] = source_df[col].replace({pd.NaT: None})

        # Merge and update logic
        merged = source_df.merge(
            db_products,
            left_on="Standard product code",
            right_on="code_produit",
            how="inner",
            suffixes=("_new", "_current"),
        ).round(2)

        mask = False  # masque initial
        for col in [c for c in source_df.columns if c not in ["Standard product code"]]:
            mask |= ~merged[f"{col}_new"].eq(merged[f"{col}_current"])

        merged = merged.loc[mask]
        merged = merged.loc[
            merged["designation_acronym_new"].notna()
        ]  # Afin de garantir l'intégrité des données

        if merged.empty:
            return "Aucune mise à jour des données à effectuer sur la table dim_produit"

        # Prepare updates
        updates = merged[
            [
                "id_dim_produit_stock_track_pk",
                "designation_acronym_new",
                "facteur_de_conversion_qat_sage_new",
            ]
        ].rename(
            columns={
                "designation_acronym_new": "designation_acronym",
                "facteur_de_conversion_qat_sage_new": "facteur_de_conversion_qat_sage",
            }
        )

        # Batch update
        update_query = f"""
        UPDATE {schema_name}.dim_produit_stock_track 
        SET 
            designation_acronym = %s,
            facteur_de_conversion_qat_sage = %s
        WHERE id_dim_produit_stock_track_pk = %s
        """

        params = [
            (
                convert_numpy_types(row["designation_acronym"]),
                convert_numpy_types(row["facteur_de_conversion_qat_sage"]),
                convert_numpy_types(row["id_dim_produit_stock_track_pk"]),
            )
            for _, row in updates.iterrows()
        ]

        civ_cursor.executemany(update_query, params)
        conn.commit()

        return f"Mise à jour réussie pour {len(params)} enregistrements"

    except Exception as e:
        conn.rollback()
        print(f"Erreur lors de la mise à jour: {str(e)}")
        raise


def synchronize_table_data(
    source_df: pd.DataFrame,
    table_name: str,
    merge_keys: List[str],
    programme: str,
    schema_name: str = "suivi_stock",
) -> Optional[pd.DataFrame]:
    """
    Full synchronization workflow for table data.

    Args:
        source_df: Source data for synchronization
        table_name: Target table name
        merge_keys: Columns for merge operations
        programme: Programme filter
        schema_name: Database schema (default: suivi_stock)

    Returns:
        Optional[pd.DataFrame]: Current table state if requested
    """
    try:
        # Schema validation
        db_columns = get_table_data(
            schema_name=schema_name,
            query=f"SELECT * FROM {schema_name}.{table_name} LIMIT 1",
        )

        if not set(source_df.columns).issubset(db_columns.columns.tolist()):
            invalid_cols = set(source_df.columns) - set(db_columns.columns.tolist())
            raise ValueError(f"Colonne invalide: {invalid_cols}")

        # Type consistency
        for col in source_df.columns:
            if col in db_columns and source_df[col].dtype != db_columns[col].dtype:
                source_df[col] = source_df[col].astype(db_columns[col].dtype)

        for col in source_df.select_dtypes("O").columns:
            source_df[col] = source_df[col].replace({pd.NaT: None})

        # Data synchronization workflow
        query = (
            f"SELECT * FROM {schema_name}.{table_name} WHERE programme = '{programme}'"
            if table_name
            in ["dim_produit_stock_track", "plan_approv", "share_link", "stock_track_npsp"]
            else f"""SELECT st.* 
                     FROM {schema_name}.{table_name} st 
                     INNER JOIN {schema_name}.dim_produit_stock_track dps 
                         ON st.id_dim_produit_stock_track_fk = dps.id_dim_produit_stock_track_pk 
                     WHERE dps.programme = '{programme}'"""
        )

        current_data = get_table_data(
            schema_name=schema_name,
            query=query,
        )
        for col in [col_date for col_date in current_data.columns if col_date.startswith("date_")]:
            source_df[col] = pd.to_datetime(
                source_df[col].astype(str).str[:10], format="%Y-%m-%d", errors="coerce"
            )
            current_data[col] = pd.to_datetime(
                current_data[col].astype(str).str[:10], format="%Y-%m-%d", errors="coerce"
            )

        # Identify new records
        new_records = (
            source_df.merge(current_data[merge_keys], on=merge_keys, how="left", indicator=True)
            .query("_merge == 'left_only'")
            .drop("_merge", axis=1)
        )

        if not new_records.empty:
            print(f"Insertion de {len(new_records)} enrégistrements réussie")
            insert_dataframe_to_table(new_records, table_name, schema_name)

        # Identify updates
        updates = source_df.merge(current_data, on=merge_keys, suffixes=("_new", "_current"))

        # display(updates)
        if updates.empty:
            return "Aucune mise à jour des données à effectuer"

        mask = False  # masque booléen de base

        for col in [c for c in source_df.columns if c not in merge_keys]:
            mask |= ~updates[f"{col}_new"].round(2).eq(updates[f"{col}_current"].round(2))

        updates = updates.loc[mask]

        # display(updates)

        # Execute batch updates
        if not updates.empty:
            # Generate dynamic update statement
            set_clause = ", ".join(
                f"{col} = %s" for col in source_df.columns if col not in merge_keys
            )

            update_query = f"""
            UPDATE {schema_name}.{table_name}
            SET {set_clause}
            WHERE {" AND ".join(f"{key} = %s" for key in merge_keys)}
            """

            params = [
                [
                    convert_numpy_types(row[f"{col}_new"])
                    for col in source_df.columns
                    if col not in merge_keys
                ]
                + [convert_numpy_types(row[key]) for key in merge_keys]
                for _, row in updates.iterrows()
            ]

            civ_cursor.executemany(update_query, params)
            conn.commit()

        return get_table_data(table_name, schema_name) if current_data.shape[0] else None

    except Exception as e:
        conn.rollback()
        print(f"Synchronization error: {str(e)}")
        raise


def get_table_info(table_name: str, schema_name: str, engine) -> tuple:
    """Récupère les métadonnées de la table"""
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name, schema=schema_name)
    pk = inspector.get_pk_constraint(table_name, schema=schema_name)["constrained_columns"]
    return columns, pk


def upsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    schema_name: str = "suivi_stock",
    conflict_columns: list = [],
    engine=civ_engine,
):
    """
    Effectue un upsert en ignorant les colonnes auto-incrémentées

    Args:
        df (list[dict]): Données utilisateur
        table_name (str): Nom de la table
        schema (str): Schéma de la table
        engine (sqlalchemy.engine): Connexion
        conflict_columns (list): Colonnes pour la détection de conflit
    """

    if df.empty:
        return "Aucune insertion ou mise à jour effectuée"

    columns, pk = get_table_info(table_name, schema_name, engine)

    auto_cols = [col["name"] for col in columns if col.get("autoincrement", False)]
    df = df.drop(columns=[c for c in auto_cols if c in df.columns], errors="ignore")

    metadata = MetaData(schema=schema_name)
    table = Table(table_name, metadata, autoload_with=engine)

    conflict_columns = pk if not conflict_columns else conflict_columns

    stmt = insert(table).values(df.to_dict("records"))

    # Generate the update dictionary
    # Exclude the primary key and conflict columns from the update
    update_dict = {c.key: c for c in stmt.excluded if c.key not in [*conflict_columns, *pk]}

    # Generate the WHERE clause for the update statement
    where_clause = None
    for col in update_dict.keys():
        condition = table.c[col].is_distinct_from(stmt.excluded[col])
        where_clause = condition if where_clause is None else where_clause | condition

    update_stmt = stmt.on_conflict_do_update(
        index_elements=conflict_columns, set_=update_dict, where=where_clause
    )

    with engine.begin() as conn:
        conn.execute(update_stmt)

    return f"Upsert de {len(df)} enrégistrements réussie"
