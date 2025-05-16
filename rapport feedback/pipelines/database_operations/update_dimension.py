from typing import List, Optional, Callable
import pandas as pd
from .db_ops import get_data_from_database

def update_dimension_table(
    dimension_name: str,
    source_dfs: List[pd.DataFrame],
    merge_on: List[str],
    change_columns: List[str],
    code_generation: Optional[Callable] = None,
    schema_name: str="dap_tools",
):
    """
    Met à jour une table de dimension de manière générique
    
    Args:
        dimension_name: Nom de la table de dimension
        source_dfs: Liste des DataFrames sources
        merge_on: Colonnes pour la jointure avec les données existantes
        change_columns: Colonnes à vérifier pour détecter les changements
        code_generation: Fonction de génération de codes (optionnel)
        schema_name: Nom du schéma
    """
    # 1. Fusion des sources et déduplication
    combined_df = pd.concat(source_dfs, ignore_index=True).drop_duplicates()

    # 2. Récupération des données existantes
    existing_data = get_data_from_database(dimension_name, schema_name=schema_name)
    
    # 3. Détection des modifications
    # existing_data = existing_data.astype({col: combined_df[col].dtype for col in merge_on})
    existing_data = existing_data.astype({
        col: safe_dtype(combined_df[col].dtype)
        for col in merge_on
    })
    merged = combined_df.merge(
        existing_data,
        on=merge_on,
        how="left",
        suffixes=("", "_existing")
    )
    
    # 4. Filtrage des lignes modifiées
    change_conditions = ""
    
    for col in change_columns:
        change_conditions += f"(merged['{col}'] != merged['{col}_existing']) |"
    
    change_conditions = change_conditions.strip('|')
        
    updated_data = merged.loc[eval(change_conditions)]

    if not updated_data.empty:            
        # 5. Génération des codes si nécessaire
        if code_generation:
            updated_data = code_generation(updated_data, existing_data)
            
        # 6. Alignement du format de sortie
        final_columns = existing_data.columns.tolist()
        updated_data = updated_data[final_columns].copy()
            
    return updated_data

def region_code_generation(df, existing, metabase):
    """Génère les informations restante de la table"""
    df_code = metabase.get_data_from_sql_query("""
        SELECT code as code_region, id AS id_region_esigl
        FROM geographic_zones WHERE levelid = 2
    """)
    
    df = df.merge(df_code, on="id_region_esigl", how="left")
    df["Code_region"] = df["Code_region"].fillna(df["code_region"])
    
    max_order = existing["region_order"].max() + 1
    df.loc[df["region_order"].isna(), "region_order"] = range(max_order, max_order + len(df))
    
    return df.drop(columns=["code_region"])

def district_code_generation(df, existing, tb_region, schema_name):
    """Génère les informations sur les code de district"""
    # Recherche des codes régionaux
    df = df.drop(columns="Code_region")
    df_region = get_data_from_database(tb_region, schema_name)[["id_region_esigl", "Code_region"]]
    df = df.merge(
        df_region,
        on="id_region_esigl",
        how="left"
    )
    max_code = existing["Code_district"].str.extract(r'DIST-(\d+)').astype(float).max()
    missing = df["Code_district"].isna()
    df.loc[missing, "Code_district"] = [f"DIST-{i}" for i in range(int(max_code)+1, int(max_code)+1+missing.sum())]
    return df
    
def safe_dtype(dtype):
    # Convertit les types nulles pandas en une forme acceptable pandas
    if pd.api.types.is_integer_dtype(dtype):
        return 'Int64'  # Pandas nullable integer
    return dtype
    
# def product_code_generation(df, existing):
#     """Génèration de l'id_produit_pk"""
#     max_val = existing["id_produit_pk"].max() + 1
#     missing = df["id_produit_pk"].isna()
#     df.loc[missing,"id_produit_pk"] = range(max_val, max_val + missing.sum())
#     return df