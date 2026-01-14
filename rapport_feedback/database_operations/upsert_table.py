from sqlalchemy import MetaData, Table, inspect
from sqlalchemy.dialects.postgresql import insert
import pandas as pd

def get_table_info(table_name: str, schema_name:str, engine) -> tuple:
    """Récupère les métadonnées de la table"""
    inspector = inspect(engine)
    columns = inspector.get_columns(table_name, schema=schema_name)
    pk = inspector.get_pk_constraint(table_name, schema=schema_name)['constrained_columns']
    return columns, pk

def upsert_table(df: pd.DataFrame, table_name: str, schema_name:str, engine, conflict_columns: list=[]):
    """UPSERT pour tables avec PK auto-générée"""

    if df.empty:
        return "Aucune insertion ou mise à jour effectuée"

    columns, pk = get_table_info(table_name, schema_name, engine)
    
    auto_cols = [col['name'] for col in columns if col.get('autoincrement', False)]
    df = df.drop(columns=[c for c in auto_cols if c in df.columns], errors='ignore')
    
    metadata = MetaData(schema=schema_name)
    table = Table(table_name, metadata, autoload_with=engine)
    
    stmt = insert(table).values(df.to_dict('records'))
    
    update_dict = {
        c.key: c for c in stmt.excluded 
        if c.key not in [*conflict_columns, *pk]
    }
    
    conflict_columns = pk if len(conflict_columns)==0 else conflict_columns
    
    update_stmt = stmt.on_conflict_do_update(
        index_elements=conflict_columns,
        set_=update_dict
    )
    
    with engine.begin() as conn:
        conn.execute(update_stmt)