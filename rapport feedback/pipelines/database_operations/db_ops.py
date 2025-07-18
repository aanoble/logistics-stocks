import pandas as pd
from IPython.display import display
from typing import List

def reload_connection():
    from sqlalchemy import create_engine
    from openhexa.sdk import workspace
    import psycopg2
    global civ_engine, conn, civ_cursor

    try:
        if conn:
            conn.close()
    except Exception as e:
        pass
    try:
        civ_engine = create_engine(workspace.database_url)
        conn = psycopg2.connect(
            dbname=workspace.database_name,
            user=workspace.database_username,
            password=workspace.database_password,
            host=workspace.database_host,
            port=workspace.database_port
            )
        civ_cursor = conn.cursor()
    except Exception as e:
        print(f"Error while connecting to database: {e}")

    # return civ_engine, conn, civ_cursor

reload_connection()

def convert_numpy_to_native(value):
    import numpy as np
    
    if isinstance(value, (np.int64, np.int32)):
        return int(value)
    elif isinstance(value, (np.float64, np.float32)):
        return float(value)
    else:
        return value

def get_data_from_database(table_name, schema_name="dap_tools") -> pd.DataFrame:
    """
    Renvoie les données d'une table spécifique présent dans la base de données
    """
    if civ_engine is None and civ_cursor is None and conn is None:
        reload_connection()

    return pd.read_sql(f"select * from {schema_name}.{table_name}", civ_engine)

def get_full_table(df_preprocess: pd.DataFrame, table_name, schema_name="dap_tools") -> pd.DataFrame:
    if civ_engine is None and civ_cursor is None and conn is None:
        reload_connection()
        
    df_from_db = pd.read_sql(f"select * from {schema_name}.{table_name}", civ_engine)
    
    if table_name == "dim_produit":
        merged = df_preprocess.merge(df_from_db.drop(columns="id_produit_pk"), indicator=True, how="left")
    elif table_name == "dim_structure":
        merged = df_preprocess.merge(df_from_db.drop(columns="type_structure"), indicator=True, how="left")
    else:
        merged = df_preprocess.merge(df_from_db, indicator=True, how="left")

    result = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    
    if not result.empty:
        if table_name == "dim_produit":
            civ_cursor.execute(f"SELECT last_value FROM {schema_name}.dim_produit_seq")
            dim_produit_seq = civ_cursor.fetchone()[0]
            result["id_produit_pk"] = [i for i in range(dim_produit_seq, dim_produit_seq + result.shape[0])]
            result.to_sql(table_name, civ_engine, if_exists="append", schema=schema_name, index=False)
            civ_cursor.execute(f"""ALTER SEQUENCE {schema_name}.dim_produit_seq RESTART WITH {result['id_produit_pk'].max()+1};""")
            conn.commit()
        else:
            result.to_sql(table_name, civ_engine, if_exists="append", schema=schema_name, index=False)
            civ_engine.dispose()
        return pd.read_sql(f"select * from {schema_name}.{table_name}", civ_engine)
    else:
        return pd.read_sql(f"select * from {schema_name}.{table_name}", civ_engine)

def check_update_data_from_db(df_new: pd.DataFrame, table_name:str, schema_name:str, programme:str, merge_columns:List, bool_df_need=True) -> pd.DataFrame:
    """
    Cette fonction est principalement utilisée pour faire la mise à jour de certaines tables par rapport à des colonnes principales (merge_columns)
    """
    df_from_db = pd.read_sql(f"select * from {schema_name}.{table_name}", civ_engine)

    assert len(df_from_db.columns) >= len(df_new.columns), print(f"Il faut que le dataframe ait la même structure et au format de colonne suivant {list(df_from_db.columns)}")
    
    cols_not_in_db = [col for col in df_new.columns if col not in df_from_db.columns]
    assert len(cols_not_in_db)==0, print(f"Les colonnes suivantes: {cols_not_in_db} ne respecte pas le format de données attendus parmi la liste des colonnes de la table: {list(df_from_db.columns)}")
    
    for col in df_from_db.columns:
        if col in df_new.columns:
            df_new[col] = df_new[col].astype(df_from_db[col].dtype)
        
    merged = df_new.merge(
        df_from_db,
        on=merge_columns, suffixes=('_new', '_past'), indicator=True, how="left")

    result = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    result = result[merge_columns + [col for col in result.columns if '_new' in col]].rename(columns=lambda x: x.replace('_new', ''))
    result = result.drop_duplicates()
    
    # Ajout de nouveaux enrégistrements
    if not result.empty:
        print(f"Ajout de ces nouvelles informations dans la table: {table_name}")        
        display(result)
        result.drop_duplicates().to_sql(table_name, con = civ_engine, 
                                        schema=schema_name, index=False, 
                                        if_exists='append')
        civ_engine.dispose()
        conn.commit()

    # Mise à jour des informations en fonction des nouveaux inputs
    df_from_db = pd.read_sql(f"select * from {schema_name}.{table_name}", civ_engine)
    merged = df_from_db.merge(
        df_new,
        on=merge_columns, suffixes=('_past', '_new'), how='outer')
    
    indicator_columns = [col for col in df_new.columns if col not in merge_columns]
    
    condition = ''
    for col in [col.replace('_past', '') for col in merged.columns if '_past' in col]:
        condition+=f"(merged.{col}_new != merged.{col}_past) & "
    condition = condition.rstrip('& ')
    
    merged = merged.loc[eval(condition)]

    for col in indicator_columns:
        merged[col] = merged[col + '_new'].combine_first(merged[col + '_past'])

    result = merged[df_from_db.columns].reset_index(drop=True).drop_duplicates()
    if not result.empty:
        print(f"Mise à jour des informations rélatives suite aux mise à jour apportées sur la table: {table_name}")
        display(result)
        for i in range(len(result)):
            row = result.iloc[i]
            update_stmt = (f"UPDATE {schema_name}.{table_name} SET " + \
                            ", ".join([f"{col}=%s" for col in result.columns if col!=col_id_pk]) + \
                            f" WHERE {col_id_pk}=%s;")
            params = [convert_numpy_to_native(row[col]) for col in result.columns if col!=col_id_pk] + [convert_numpy_to_native(row[col_id_pk])]
            civ_cursor.execute(update_stmt, params)
                
        conn.commit()
        # civ_engine.dispose()

    return pd.read_sql(f"""SELECT * FROM {schema_name}.{table_name} where programme='{programme}'""", civ_engine) if bool_df_need else None