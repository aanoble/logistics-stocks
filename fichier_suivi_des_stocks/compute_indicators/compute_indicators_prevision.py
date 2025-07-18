import pandas as pd
import numpy as np
from IPython.display import display


def format_date_updated_plan_approv(
    date_str: str,
    max_date_year: int = pd.Timestamp.max.year,
    dico_mois_map: dict = {
        "janv": "01",
        "Jan": "01",
        "févr": "02",
        "Feb": "02",
        "mars": "03",
        "Mar": "03",
        "avr": "04",
        "Apr": "04",
        "mai": "05",
        "May": "05",
        "juin": "06",
        "Jun": "06",
        "juil": "07",
        "Jul": "07",
        "août": "08",
        "Aug": "08",
        "sept": "09",
        "Sep": "09",
        "oct": "10",
        "Oct": "10",
        "nov": "11",
        "Nov": "11",
        "déc": "12",
        "Dec": "12",
    },
):
    """
    Fonction utilisée pour le formattage du champ `Date updated` dans le fichier plan d'approvisionnement
    """

    try:
        mois, annee = date_str.split("-")
        mois_num = dico_mois_map[mois]
        annee = annee if int(annee) < max_date_year else str(max_date_year - 1)
        return f"{annee}-{mois_num}-01"
    except Exception:
        return date_str


def update_stocks(df_prevision_other_month, df_plan_approv):
    """
    Cette fonction permet d'avoir les prévisions
    """
    for i in range(len(df_prevision_other_month)):
        row = df_prevision_other_month.iloc[i]
        if row["PERIOD"] != row["date_report"]:
            code_produit = row["code_produit"]
            id_dim_produit_stock_track_pk = row["id_dim_produit_stock_track_pk"]
            period = row["PERIOD"]

            prev_period = period - pd.DateOffset(months=1)

            df_plan_filtered = df_plan_approv.loc[
                (df_plan_approv["Standard product code"] == code_produit)
                & (df_plan_approv["Date updated"] == period)
            ]

            df_prev_filtered = df_prevision_other_month.loc[
                (df_prevision_other_month.PERIOD == prev_period)
                & (
                    df_prevision_other_month.id_dim_produit_stock_track_pk
                    == id_dim_produit_stock_track_pk
                )
            ]

            if not df_prev_filtered.empty:
                stock_prev_central = df_prev_filtered["stock_prev_central"].iloc[0]
                stock_prev_national = df_prev_filtered["stock_prev_national"].iloc[0]
            else:
                stock_prev_central = "ND"
                stock_prev_national = "ND"

            quantite = (
                df_plan_filtered["Quantité harmonisée (SAGE)"].sum()
                if not df_plan_filtered.empty
                else 0
            )
            row["stock_prev_central"] = (
                "ND"
                if stock_prev_central == "ND"
                else round(max(0, stock_prev_central - 1) + quantite / row["dmm_central_annexe_2"])
                if row["dmm_central_annexe_2"] != 0
                else "ND"
            )
            row["stock_prev_national"] = (
                "ND"
                if stock_prev_national == "ND"
                else round(
                    max(0, stock_prev_national - 1) + quantite / row["cmm_national_annexe_2"]
                )
                if row["cmm_national_annexe_2"] != 0
                else "ND"
            )

            df_prevision_other_month.iloc[i] = row
    return df_prevision_other_month


def get_prevision_current_month(
    df_plan_approv: pd.DataFrame,
    date_report: str,
    programme: str,
    engine,
    schema_name: str = "suivi_stock",
) -> pd.DataFrame:
    df_plan_approv["Date updated"] = df_plan_approv["Date updated"].apply(
        lambda x: pd.to_datetime(format_date_updated_plan_approv(x), format="%Y-%m-%d")
    )

    df_stock_track = pd.read_sql(
        f"""SELECT prod.*, st.*
            FROM {schema_name}.stock_track st
            INNER JOIN {schema_name}.dim_produit_stock_track prod ON st.id_dim_produit_stock_track_fk = prod.id_dim_produit_stock_track_pk
            WHERE prod.programme='{programme}' AND date_report='{date_report}' 
            -- AND prod.designation_acronym IS NOT NULL
        """,
        engine,
    )

    df_prevision = df_stock_track[
        [
            "id_dim_produit_stock_track_pk",
            "code_produit",
            "ancien_code",
            "categorie",
            "sdu_central_annexe_2",
            "dmm_central_annexe_2",
            "sdu_national_annexe_2",
            "cmm_national_annexe_2",
        ]
    ].sort_values("code_produit")

    df_prevision["stock_prev_central"] = df_prevision.apply(
        lambda row: 0
        if row.sdu_central_annexe_2 == 0
        else round(row.sdu_central_annexe_2 / row.dmm_central_annexe_2)
        if row.dmm_central_annexe_2 != 0
        else "ND",
        axis=1,
    )

    df_prevision["stock_prev_national"] = df_prevision.apply(
        lambda row: 0
        if row.sdu_national_annexe_2 == 0
        else round(row.sdu_national_annexe_2 / row.cmm_national_annexe_2)
        if row.cmm_national_annexe_2 != 0
        else "ND",
        axis=1,
    )

    df_period = pd.DataFrame(
        {"PERIOD": pd.date_range(start=pd.to_datetime(date_report), periods=13, freq="MS")}
    )

    df_prevision_other_month = df_prevision[
        [
            "id_dim_produit_stock_track_pk",
            "code_produit",
            "ancien_code",
            "sdu_central_annexe_2",
            "dmm_central_annexe_2",
            "sdu_national_annexe_2",
            "cmm_national_annexe_2",
            "stock_prev_central",
            "stock_prev_national",
        ]
    ].merge(df_period, how="cross")

    df_prevision_other_month["date_report"] = pd.to_datetime(date_report)

    df_prevision_other_month = update_stocks(df_prevision_other_month, df_plan_approv)

    cols = [
        "sdu_central_annexe_2",
        "dmm_central_annexe_2",
        "sdu_national_annexe_2",
        "cmm_national_annexe_2",
    ]
    for col in cols:
        df_prevision_other_month[col] = df_prevision_other_month.apply(
            lambda row: row[col] if row.PERIOD == pd.to_datetime(date_report) else np.nan, axis=1
        )

    df_prevision_other_month.rename(
        columns={
            "id_dim_produit_stock_track_pk": "id_dim_produit_stock_track_fk",
            "sdu_central_annexe_2": "stock_central",
            "dmm_central_annexe_2": "dmm_central",
            "sdu_national_annexe_2": "stock_national",
            "cmm_national_annexe_2": "cmm_national",
            "PERIOD": "period_prev",
        },
        inplace=True,
    )

    display(df_prevision_other_month.head(3))

    return df_prevision_other_month.round(2)
