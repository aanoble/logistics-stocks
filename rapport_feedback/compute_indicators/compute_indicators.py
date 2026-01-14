import calendar

import numpy as np
import pandas as pd
from openhexa.sdk import workspace
# from IPython.display import display

def generate_month_end_report_date(date_report: str):
    """Génère la date de fin de mois pour un rapport à partir d'un nom de mois localisé."""
    from datetime import datetime

    month_number = {
        "Janvier": "01-",
        "Février": "02-",
        "Mars": "03-",
        "Avril": "04-",
        "Mai": "05-",
        "Juin": "06-",
        "Juillet": "07-",
        "Août": "08-",
        "Septembre": "09-",
        "Octobre": "10-",
        "Novembre": "11-",
        "Décembre": "12-",
    }
    date_report = (
        "01-" + month_number[date_report] + str(datetime.today().year)
        if date_report != "Décembre"
        else "01-" + month_number[date_report] + str(datetime.today().year - 1)
    )
    date_report = pd.to_datetime(date_report, format="%d-%m-%Y").strftime("%Y-%m-%d")
    return (pd.to_datetime(date_report) + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")


def construct_absolute_file_path(file_path):
    """Construit un chemin de fichier absolu à partir d'un chemin relatif du workspace."""
    return workspace.files_path + "/" + file_path.split("workspace/")[-1].lstrip("/")


def find_best_matching_sheet_name(sheet_name, sheet_names, threshold=95):
    """
    Permet de vérifier si la feuille est présente dans la liste des classeurs du fichier qui a été fourni.
    Dans le cas contraire, un recherche par correspondance floue est effectuée pour voir s'il existe un semblable de nom qui se peut être mal orthographié pour le fournir en sortie.
    """
    from fuzzywuzzy import fuzz, process

    try:
        if sheet_name in sheet_names:
            return sheet_name

        best_match = process.extractOne(sheet_name, sheet_names, scorer=fuzz.token_set_ratio)
        if best_match[1] >= threshold:
            return best_match[0]
        return None
    except Exception:
        return None


def convert_reporting_period_to_cutoff_date(period_name: str, facility: str):
    """Convertit une période de reporting en date limite de soumission."""
    from datetime import datetime

    month_number = {
        "janvier": 1,
        "fevrier": 2,
        "mars": 3,
        "avril": 4,
        "mai": 5,
        "juin": 6,
        "juillet": 7,
        "aout": 8,
        "septembre": 9,
        "octobre": 10,
        "novembre": 11,
        "decembre": 12,
        "janvier mars": 3,
        "avril juin": 6,
        "juillet septembre": 9,
        "octobre decembre": 12,
    }

    mois = month_number.get(period_name[:-5].lower(), None) + 1
    annee = int(period_name[-4:])
    if mois > 12:
        annee += 1
        mois = 1
    if "DISTRICT SANITAIRE" in facility.upper():
        return datetime.strptime(str(mois) + "/" + str(10) + "/" + str(annee), "%m/%d/%Y")

    return datetime.strptime(str(mois) + "/" + str(7) + "/" + str(annee), "%m/%d/%Y")


def count_valid_submissions_by_criteria(extract_transmission, element_col, code, program, column_type_table, value_type_table):
    """Compte les soumissions valides selon des critères."""
    if element_col == "NA":
        return "NA"
    if "PNLT" in program:
        if extract_transmission.loc[extract_transmission.program == program].shape[0] == 0:
            return "NA"
        else:
            return extract_transmission.loc[
                (extract_transmission.code == code)
                & (extract_transmission.program == program)
                & (extract_transmission[column_type_table] == value_type_table)
            ].shape[0]

    return extract_transmission.loc[
        (extract_transmission.code == code)
        & (extract_transmission.program == program)
        & (extract_transmission[column_type_table] == value_type_table)
    ].shape[0]


def calculate_completeness_promptness_metrics(
    extract_transmission: pd.DataFrame, expected_site: pd.DataFrame, type_table: str = "completude"
) -> pd.DataFrame:
    """Calcule les indicateurs de complétude et de ponctualité des rapports"""
    
    df = expected_site.copy().sort_values(by=["Region", "Code"])
    df = df.drop(columns="District")
    if type_table == "completude":
        column_type_table, value_type_table = "Transmis", "OUI"
    else:
        column_type_table, value_type_table = "Promptitude", 1

    columns_to_process = {
        "ARV": "PNLS/ANTIRETROVIRAUX ET IO",
        "TRC": "PNLS/TESTS RAPIDES ET CONSOMMABLES",
        "LAB": "PNLS/PRODUITS DE LABORATOIRE",
        "CHARGE VIRALE": "PNLS/CHARGES VIRALES",
        "PNLP": "PNLP/MEDICAMENTS ET INTRANTS",
        "PNSME": "PNSME/MEDICAMENTS ET INTRANTS",
        "PNSME-GRAT": "PNSME_GRATUITE:MEDICAMENTS ET INTRANTS",
        "PNN": "PNN/MEDICAMENTS ET INTRANTS",
        "PNLT": "PNLT/SENSIBLE MEDICAMENTS ET INTRANTS",
        "TBS": "PNLT/SENSIBLE MEDICAMENTS ET INTRANTS",
        "TBMR": "PNLT/SENSIBLE MEDICAMENTS ET INTRANTS",
        "TBLAB": "PNLT/SENSIBLE MEDICAMENTS ET INTRANTS",
    }
    dico_cols = columns_to_process.copy()

    dico_cols_pnlt = {
        "TBS": "PNLT/SENSIBLE MEDICAMENTS ET INTRANTS",
        "TBMR": "PNLT/SENSIBLE MEDICAMENTS ET INTRANTS",
        "TBLAB": "PNLT/SENSIBLE MEDICAMENTS ET INTRANTS",
    }

    for column, program in dico_cols.items():
        try:
            df[column] = df[[column, "Code"]].apply(
                lambda x: count_valid_submissions_by_criteria(
                    extract_transmission, x[0], x["Code"], program, column_type_table, value_type_table
                ),
                axis=1,
            )
        except KeyError:
            columns_to_process.pop(column)
            if column in ("TBS", "TBMR", "TBLAB"):
                dico_cols_pnlt.pop(column)
            continue

    df = df[["Code", "Site", "Region"] + list(columns_to_process)]

    # --> Calcul des colonnes aditionnelles
    columns_to_process = {
        "ARV": "Taux par Région ARV",
        "TRC": "Taux par Région TRC",
        "LAB": "Taux par Région LAB",
        "CHARGE VIRALE": "Taux par Région Charges virales",
        "PNLP": "Taux par Région PNLP",
        "PNSME-GRAT": "Taux par Région PNSME",
        "PNN": "Taux par Région PNN",
        "PNLT": "Taux par Région PNLT",
    }
    # if "PNSME-GRAT" in df.columns:
    #     columns_to_process.pop("PNSME")

    for column, new_column in columns_to_process.items():
        try:
            df_group = df[df[column] != "NA"].groupby(["Region"])["Code"].count().reset_index()

            df[new_column] = df.apply(
                lambda x: 0
                if x[column] == "NA" or x[column] == 0
                else (x[column] / df_group.loc[df_group.Region == x["Region"], "Code"].iloc[0])
                if not df_group.loc[df_group.Region == x["Region"], "Code"].empty
                else 0,
                axis=1,
            )
            del df_group
        except KeyError:
            continue

    # if "PNSME" not in list(columns_to_process):
    #     df_melt = pd.melt(
    #         df.drop(columns="Code"),
    #         id_vars="Region",
    #         value_vars=["PNSME", "PNSME-GRAT"],
    #         value_name="Code",
    #     )
    #     df_group = (
    #         df_melt.loc[df_melt.Code.ne("NA"), ["Region", "Code"]]
    #         .groupby("Region")["Code"]
    #         .count()
    #         .reset_index()
    #     )
    #     df["Taux par Région PNSME"] = df.apply(
    #         lambda x: sum([e for e in (x["PNSME"], x["PNSME-GRAT"]) if e != "NA"])
    #         / df_group.loc[df_group.Region == x["Region"], "Code"].iloc[0]
    #         if not df_group.loc[df_group.Region == x["Region"], "Code"].empty
    #         else 0,
    #         axis=1,
    #     )
    #     del df_melt, df_group

    # Le taux par Région PNLT à changer dans la nouvelle version de Mars 2024
    if "PNLT" not in df.columns:
        cols_pnlt = list(dico_cols_pnlt)
        df_melt = pd.melt(
            df.drop(columns="Code"), id_vars="Region", value_vars=cols_pnlt, value_name="Code"
        )
        df_group = (
            df_melt.loc[df_melt.Code.ne("NA"), ["Region", "Code"]]
            .groupby("Region")["Code"]
            .count()
            .reset_index()
        )
        df["Taux par Région PNLT"] = df.apply(
            lambda x: sum([x[e] for e in cols_pnlt if x[e] != "NA"])
            / df_group.loc[df_group.Region == x["Region"], "Code"].iloc[0]
            if not df_group.loc[df_group.Region == x["Region"], "Code"].empty
            else 0,
            axis=1,
        )
        del df_melt, df_group
    # --> Taux par Région PNLS
    df_melt = pd.melt(
        df,
        id_vars=["Region"],
        value_vars=["ARV", "TRC", "LAB", "CHARGE VIRALE"],
        value_name="value",
    )

    df_melt = df_melt.groupby("Region")["value"].value_counts().reset_index()

    df["Taux par Région PNLS"] = df[["Region", "ARV", "TRC", "LAB", "CHARGE VIRALE"]].apply(
        lambda x: len(
            [
                element
                for element in [x["ARV"], x["TRC"], x["LAB"], x["CHARGE VIRALE"]]
                if element == 1
            ]
        )
        / df_melt[(df_melt.Region == x["Region"]) & (df_melt.value != "NA")]["count"].sum(),
        axis=1,
    )

    df["PNLS recu"] = df[["ARV", "TRC", "LAB", "CHARGE VIRALE"]].apply(
        lambda x: sum(
            [
                element
                for element in [x["ARV"], x["TRC"], x["LAB"], x["CHARGE VIRALE"]]
                if element != "NA"
            ]
        ),
        axis=1,
    )

    df["PNLS attendu"] = df[["ARV", "TRC", "LAB", "CHARGE VIRALE"]].apply(
        lambda x: len(
            [
                element
                for element in [x["ARV"], x["TRC"], x["LAB"], x["CHARGE VIRALE"]]
                if element != "NA"
            ]
        ),
        axis=1,
    )
    cols = [
        col
        for col in [
            "ARV",
            "TRC",
            "LAB",
            "CHARGE VIRALE",
            "PNLP",
            "PNSME-GRAT",
            # "PNSME",
            "TBS",
            "TBMR",
            "TBLAB",
            "PNN",
            "PNLT",
        ]
        if col in df.columns
    ]
    df["Attendu"] = df[cols].apply(
        lambda row: len([element for element in row if element != "NA"]), axis=1
    )

    df["Recu"] = df[cols].apply(
        lambda row: len([element for element in row if element == 1]), axis=1
    )
    return df


def compute_indicators_completeness_and_promptness(
    expected_site, extract_transmission, date_report
):
    """ """
    date_report = pd.to_datetime(date_report)

    extract_transmission = extract_transmission.copy()

    extract_transmission = extract_transmission[
        [
            "region",
            "period",
            "code",
            "facility",
            "program",
            "statut",
            "date_soumission",
            "date_autorisation",
        ]
    ]

    try:
        extract_transmission["date_soumission"] = pd.to_datetime(
            extract_transmission["date_soumission"], format="%Y-%m-%d"
        )
        extract_transmission["date_soumission"] = pd.to_datetime(
            extract_transmission["date_soumission"], format="%d/%m/%Y"
        )

        extract_transmission["date_autorisation"] = pd.to_datetime(
            extract_transmission["date_autorisation"], format="%Y-%m-%d"
        )
        extract_transmission["date_autorisation"] = pd.to_datetime(
            extract_transmission["date_autorisation"], format="%d/%m/%Y"
        )

    except Exception:
        extract_transmission["date_soumission"] = extract_transmission["date_soumission"].str.replace(
            "T00:00:00Z", ""
        )

        extract_transmission["date_autorisation"] = extract_transmission["date_autorisation"].str.replace(
            "T00:00:00Z", ""
        )

        extract_transmission["date_soumission"] = pd.to_datetime(
            extract_transmission["date_soumission"], format="%Y-%m-%d"
        )
        extract_transmission["date_soumission"] = pd.to_datetime(
            extract_transmission["date_soumission"], format="%d/%m/%Y"
        )

        extract_transmission["date_autorisation"] = pd.to_datetime(
            extract_transmission["date_autorisation"], format="%Y-%m-%d"
        )
        extract_transmission["date_autorisation"] = pd.to_datetime(
            extract_transmission["date_autorisation"], format="%d/%m/%Y"
        )

    extract_transmission["facility"] = (
        extract_transmission["facility"]
        .str.replace(r'\b(PNSME|PHG|PCS|CMU)\b', '', regex=True)
        .str.strip()
    )

    extract_transmission["program"] = extract_transmission["program"].str.replace("-", "/")

    extract_transmission["Transmis"] = extract_transmission["statut"].apply(
        lambda x: "NON" if x in ("SUBMITTED", "INITIATED") else x
    )

    extract_transmission["Transmis"] = extract_transmission["Transmis"].apply(
        lambda x: "OUI" if x != "" and x != "NON" else x
    )

    extract_transmission["Date limite"] = extract_transmission[["facility", "period"]].apply(
        lambda x: convert_reporting_period_to_cutoff_date(x[1], x[0]), axis=1
    )

    extract_transmission["Promptitude"] = extract_transmission[
        ["Transmis", "Date limite", "date_autorisation"]
    ].apply(lambda x: 1 if x[1] >= x[2] and x[0] == "OUI" else 0, axis=1)

    detail_completness = calculate_completeness_promptness_metrics(extract_transmission, expected_site)
    detail_completness["Indicateur type"] = "Completude"

    detail_promptitude = calculate_completeness_promptness_metrics(extract_transmission, expected_site, "Promptitude")
    detail_promptitude["Indicateur type"] = "Promptitude"

    df_dq = pd.concat([detail_completness, detail_promptitude])

    # completude_promptitude_par_ets
    # df_dq["PNSME"] = df_dq["PNSME-GRAT"] # modification apportée
    
    df_ets = (
        df_dq.drop(
            columns=[e for e in df_dq.columns if "Taux par Région" in e or e in ("Attendu", "Recu")]
        )
        .sort_values(["Region", "Site"])
        .reset_index()
        .drop(columns="index")
    )

    # Et une autre qui fera un recap des indicateurs par régions Taux de complétude ou promptitude par région
    df_region = (
        df_dq.drop(
            columns=[
                e
                for e in df_dq.columns
                if "Taux par Région" not in e and e not in ("Region", "Indicateur type")
            ]
        )
        .groupby(["Region", "Indicateur type"])
        .sum()
        .reset_index()
    )

    df_group = (
        df_dq[
            [
                "Region",
                "Indicateur type",
                "ARV",
                "TRC",
                "LAB",
                "CHARGE VIRALE",
                "PNLP",
                # "PNSME",
                "PNSME-GRAT",
                "PNN",
                "TBS",
                "TBMR",
                "TBLAB",
            ]
        ]
        .replace(0, 1)
        .replace("NA", 0)
        .groupby(["Region", "Indicateur type"])
        .sum()
        .reset_index()
        .rename(columns={"PNSME-GRAT": "PNSME"}) #ajout
    )

    # if "PNSME-GRAT" in df_group.columns:
    #     df_group["PNSME"] = df_group["PNSME-GRAT"] # df_group["PNSME"] + df_group["PNSME-GRAT"]
    #     df_group.drop(columns="PNSME-GRAT", inplace=True)

    df_group["PNLS"] = df_group.apply(
        lambda row: row["ARV"] + row["TRC"] + row["LAB"] + row["CHARGE VIRALE"], axis=1
    )
    if "PNLT" not in df_group.columns:
        col = [e for e in df_group.columns if e in ("TBS", "TBMR", "TBLAB")]
        df_group["PNLT"] = df_group.apply(lambda row: sum([row[e] for e in col]), axis=1)
        df_group.drop(columns=col, inplace=True)

    df_group.rename(
        columns=lambda x: "Total rapports attendus " + x
        if x not in ("Region", "Indicateur type")
        else x,
        inplace=True,
    )

    df_region = df_region.merge(df_group, on=["Region", "Indicateur type"], how="left")

    df_region = df_region[
        [
            "Region",
            "Indicateur type",
            "Total rapports attendus ARV",
            "Taux par Région ARV",
            "Total rapports attendus TRC",
            "Taux par Région TRC",
            "Total rapports attendus LAB",
            "Taux par Région LAB",
            "Total rapports attendus CHARGE VIRALE",
            "Taux par Région Charges virales",
            "Total rapports attendus PNLP",
            "Taux par Région PNLP",
            "Total rapports attendus PNN",
            "Taux par Région PNN",
            "Total rapports attendus PNSME",
            "Taux par Région PNSME",
            "Total rapports attendus PNLT",
            "Taux par Région PNLT",
            "Total rapports attendus PNLS",
            "Taux par Région PNLS",
        ]
    ]

    del df_group

    programme = ["PNLP", "PNSME", "PNN", "PNLT"]  # , 'PNLS']
    col_pnsme = ["PNSME-GRAT"] # [col for col in df_ets.columns if "PNSME" in col.upper()]
    col_pnlt = (
        [col for col in df_ets.columns if col in ("TBS", "TBMR", "TBLAB")]
        if "PNLT" not in df_ets.columns
        else ["PNLT"]
    )

    for col in programme:
        if col in ("PNSME", "PNLT"):
            cols = col_pnsme if col == "PNSME" else col_pnlt

            df_melt = pd.melt(
                df_ets.drop(columns="Code"),
                id_vars="Indicateur type",
                value_vars=cols,
                value_name="Code",
            )
            df_melt["taux_indicateur_" + col.lower()] = df_melt.groupby(["Indicateur type"])[
                "Code"
            ].transform(lambda x: x.eq(1).sum() / (x.eq(0).sum() + x.eq(1).sum()))
            df_melt = df_melt[
                ["Indicateur type", "taux_indicateur_" + col.lower()]
            ].drop_duplicates()
            df_ets = df_ets.merge(df_melt, how="left", on="Indicateur type")
            del df_melt
        else:
            df_ets["taux_indicateur_" + col.lower()] = df_ets.groupby(["Indicateur type"])[
                col
            ].transform(lambda x: x.eq(1).sum() / (x.eq(0).sum() + x.eq(1).sum()))

    # Calcul pour le PNLS
    df_pnls_group = (
        df_ets.groupby(["Indicateur type"])[["PNLS recu", "PNLS attendu"]]
        .apply(lambda x: x["PNLS recu"].sum() / x["PNLS attendu"].sum())
        .reset_index()
    )
    df_pnls_group.rename(columns={0: "taux_indicateur_pnls"}, inplace=True)
    df_ets = df_ets.merge(df_pnls_group, how="left", on="Indicateur type")

    del df_pnls_group, col_pnlt, col_pnsme

    cols_calculate = (
        [
            "ARV",
            "TRC",
            "LAB",
            "CHARGE VIRALE",
            "PNLP",
            # "PNSME",
            "PNSME-GRAT",
            "PNN",
            "TBS",
            "TBMR",
            "TBLAB",
        ]
        if date_report.month in (3, 6, 9, 12)
        else ["ARV", "TRC", "LAB", "CHARGE VIRALE", "PNLP", "PNSME-GRAT", "PNN"]
    ) # "PNSME"

    df_ets["sum_produit_inline"] = df_ets.apply(
        lambda row: sum(
            [element for element in [row[i] for i in cols_calculate] if element != "NA"]
        ),
        axis=1,
    )
    df_ets["count_produit_inline"] = df_ets.apply(
        lambda row: len(
            [element for element in [row[i] for i in cols_calculate] if element != "NA"]
        ),
        axis=1,
    )

    df_ind_region = (
        df_dq.groupby(["Region", "Indicateur type"])[["Recu", "Attendu"]]
        .apply(lambda x: x["Recu"].sum() / x["Attendu"].sum())
        .reset_index()
    )

    df_ind_region.rename(columns={0: "taux_indicateur_region"}, inplace=True)

    df_region = df_region.merge(df_ind_region, how="left", on=["Region", "Indicateur type"])

    df_app = (
        df_dq.groupby(["Indicateur type"])[["Recu", "Attendu"]]
        .apply(lambda x: x["Recu"].sum() / x["Attendu"].sum())
        .reset_index()
    )
    df_app.rename(columns={0: "taux_indicateur_region"}, inplace=True)

    df_app["Region"] = "NATIONAL"

    df_region = pd.concat([df_region, df_app]).reset_index().drop(columns="index")

    del df_app, df_dq, df_ind_region

    if date_report.month not in (3, 6, 9, 12):
        df_ets["taux_indicateur_pnlt"] = np.nan
        df_region["Taux par Région PNLT"] = np.nan

    return df_ets, df_region

def analyze_product_stock_status_indicators(df_prod_traceurs, df_etat_stock, date_report):
    """Analyse les indicateurs clés de gestion des stocks produits."""
    
    df_prod_traceurs = df_prod_traceurs[
        ["CODE PRODUIT", "PRODUIT", "PROGRAMME", "CODE COMBINE", "CATEGORIE PRODUIT"]
    ]
    # Pour le code combiné il est possible de l'avoir

    df_etat_stock["abrv_programme"] = df_etat_stock["programme"].apply(
        lambda x: x.split("-")[0].split("_")[0]
    )

    df_etat_stock["code_produit"] = df_etat_stock["code_produit"].astype("Int64")

    df_etat_stock["CATEGORIE PRODUIT"] = pd.merge(
        df_etat_stock,
        df_prod_traceurs,
        how="left",
        left_on=["code_produit", "abrv_programme"],
        right_on=["CODE PRODUIT", "PROGRAMME"],
    )["CATEGORIE PRODUIT"]

    df_etat_stock["CATEGORIE PRODUIT"] = df_etat_stock["CATEGORIE PRODUIT"].apply(
        lambda x: x.capitalize()
        if isinstance(x, str)
        else "Produit non traceur"
        if pd.isna(x)
        else x
    )

    def get_cmm_gestionnaire(row):
        if pd.isna(row.periode):
            return np.nan
        else:
            if pd.isna(row.quantite_commandee):
                return row.cmm
            elif row.abrv_programme == "PNLT" and row.quantite_commandee > 0:
                return (row.quantite_commandee + row.sdu) / 6
            elif row.quantite_commandee > 0:
                return (row.quantite_commandee + row.sdu) / 4
            else:
                return row.cmm

    df_etat_stock["CMM gestionnaire"] = df_etat_stock.apply(get_cmm_gestionnaire, axis=1)

    df_etat_stock["MSD"] = df_etat_stock[["sdu", "CMM gestionnaire"]].apply(
        lambda x: np.nan if x[1] == 0 or pd.isna(x[1]) else x[0] / x[1], axis=1
    )
    # [("VITAMINE A 200 000 UI caps UN  -", 3150050),
    #  ("VITAMINE A 100 000 UI caps UN  -", 3150049),
    #  ("ALBENDAZOLE 400 mg comp. UN  -", 3050002)]
    # Les id correspondent aux id de certains districts dans eSIGL (district_id) pour faire des vérifications
    # ou étendre la liste voir le script suivant dans metabase """select * from vw_districts vw join geographic_zones gz on vw.district_id = gz.id where gz.levelid=3 """
    
    dds_routine_pnn = [
        24, 25, 34, 101, 102, 28, 49, 152, 71, 98, 30,
        87, 93, 92, 55, 94, 97, 100, 133, 138, 74, 47,
        129, 77, 131, 65, 59, 112, 80, 103, 113, 130,
        76, 60, 85, 51, 63, 67, 70, 50, 68, 78, 52,
        66, 99, 118, 54, 75, 82, 43, 88, 104, 121,
        42, 120, 40, 56, 132, 107, 64, 108, 89, 125,
        69, 126, 115, 116, 84, 90, 26, 27, 62, 110
    ]

    df_etat_stock["check_prod_pnn"] = df_etat_stock.apply(
        lambda row: 0
        if row.abrv_programme == "PNN"
        and row.code_produit
        in (3050002, 3150049, 3050002)
        and row.id_district_esigl not in dds_routine_pnn
        else 1,
        axis=1,
    )

    df_etat_stock = df_etat_stock.loc[df_etat_stock.check_prod_pnn == 1].drop(
        columns="check_prod_pnn"
    )

    date_report = pd.to_datetime(date_report)
    nombre_de_jours = calendar.monthrange(date_report.year, date_report.month)[1]

    def determine_statut_stock(row):
        if pd.isna(row["CMM gestionnaire"]) and pd.isna(row["sdu"]):
            return "NA"
        elif (
            row["nbrejrsrupture"] >= nombre_de_jours or row["sdu"] == 0
        ):  # new_update dans le calcul de l'etat de stock
            return "RUPTURE"
        elif row["sdu"] > 0 and row["CMM gestionnaire"] == 0:
            return "STOCK DORMANT"  # Good Here
        elif row.abrv_programme == "PNLT":  # row['programme'].lstrip()[:4] ==
            if row["MSD"] > 0 and row["MSD"] <= 1.5:
                return "EN BAS DU PCU"
            elif row["MSD"] > 1.5 and row["MSD"] < 3:
                return "ENTRE PCU et MIN"
            elif row["MSD"] >= 3 and row["MSD"] <= 6:
                return "BIEN STOCKE"
            elif row["MSD"] > 6:
                return "SURSTOCK"
            elif row["sdu"] == 0 and row["CMM gestionnaire"] > 0:
                return "RUPTURE"
        elif row["MSD"] > 4:
            return "SURSTOCK"
        elif row["MSD"] >= 2 and row["MSD"] <= 4:
            return "BIEN STOCKE"
        elif row["MSD"] > 1 and row["MSD"] < 2:
            return "ENTRE PCU et MIN"
        elif row["MSD"] > 0 and row["MSD"] <= 1:
            return "EN BAS DU PCU"
        elif row["sdu"] == 0 and row["CMM gestionnaire"] > 0:
            return "RUPTURE"  # Good here
        else:
            return "NA"

    df_etat_stock["ETAT DU STOCK"] = df_etat_stock.apply(determine_statut_stock, axis=1)

    df_etat_stock["ETAT DU STOCK"] = df_etat_stock["ETAT DU STOCK"].fillna("NA")

    def calcul_quantite(row):
        if row.abrv_programme == "PNLT" and (
            row["ETAT DU STOCK"] == "EN BAS DU PCU" or row["sdu"] == 0
        ):  # (row['ETAT DU STOCK'] in ('EN BAS DU PCU', 'RUPTURE')):
            return 6 * row["CMM gestionnaire"] - row["sdu"]
        elif (
            row["ETAT DU STOCK"] == "EN BAS DU PCU" or row["sdu"] == 0
        ):  # in ('EN BAS DU PCU', 'RUPTURE'):
            return 4 * row["CMM gestionnaire"] - row["sdu"]
        else:
            return np.nan

    df_etat_stock["BESOIN CMMMANDE URGENTE"] = df_etat_stock.apply(calcul_quantite, axis=1)

    def calcul_quantite(row):
        if row.abrv_programme == "PNLT" and (
            row["ETAT DU STOCK"] or row["sdu"] == 0
        ):  # in ('EN BAS DU PCU', 'RUPTURE')):
            return 3 * row["CMM gestionnaire"] - row["sdu"]
        elif (
            row["ETAT DU STOCK"] == "EN BAS DU PCU" or row["sdu"] == 0
        ):  # in ('EN BAS DU PCU', 'RUPTURE'):
            return row["CMM gestionnaire"] - row["sdu"]
        else:
            return np.nan

    df_etat_stock["BESOIN TRANSFERT IN"] = df_etat_stock.apply(calcul_quantite, axis=1)

    def calcul_quantite(row):
        if row["ETAT DU STOCK"] == "ND":
            return np.nan
        elif row["ETAT DU STOCK"] in ("STOCK DORMANT", "SURSTOCK"):
            if row.abrv_programme == "PNLT":
                return row["sdu"] - 6 * row["CMM gestionnaire"]
            else:
                return row["sdu"] - 4 * row["CMM gestionnaire"]
        else:
            return np.nan

    df_etat_stock["QUANTITE A TRANSFERER OUT"] = df_etat_stock.apply(calcul_quantite, axis=1)

    del calcul_quantite

    df_etat_stock.rename(
        columns={
            "code_produit": "CODE",
            "programme": "SOUS-PROGRAMME",
            "abrv_programme": "PROGRAMME",
            "periode": "PERIODE",
            "region": "REGION",
            "district": "DISTRICT",
            "code": "CODE ETS",
            "etablissement": "STRUCTURE",
            "type_structure": "TYPE DE STRUCTURE",
            "designation": "PRODUIT",
            "unite": "UNITE DE RAPPORTAGE",
            "stock_initial": "STOCK INITIAL",
            "quantite_recue": "QUANTITE RECUE",
            "quantite_distribuee": "QUANTITE UTILISEE",
            "perte_ajustement": "PERTES ET AJUSTEMENT",
            "nbrejrsrupture": "JOURS DE RUPTURE",
            "sdu": "SDU",
            "cmm": "CMM ESIGL",
            "quantite_proposee": "QUANTITE PROPOSEE",
            "quantite_commandee": "QUANTITE COMMANDEE",
            "quantite_approuvee": "QUANTITE APPROUVEE",
            "categorie_produit": "CATEGORIE_DU_PRODUIT",
        },
        inplace=True,
    )

    df_etat_stock = df_etat_stock[
        [
            "CODE",
            "PROGRAMME",
            "SOUS-PROGRAMME",
            "PERIODE",
            "REGION",
            "id_region_esigl",
            "DISTRICT",
            "id_district_esigl",
            "CODE ETS",
            "STRUCTURE",
            "TYPE DE STRUCTURE",
            "CATEGORIE PRODUIT",
            "PRODUIT",
            "UNITE DE RAPPORTAGE",
            "STOCK INITIAL",
            "QUANTITE RECUE",
            "QUANTITE UTILISEE",
            "PERTES ET AJUSTEMENT",
            "JOURS DE RUPTURE",
            "SDU",
            "CMM ESIGL",
            "CMM gestionnaire",
            "QUANTITE PROPOSEE",
            "QUANTITE COMMANDEE",
            "QUANTITE APPROUVEE",
            "MSD",
            "ETAT DU STOCK",
            "BESOIN CMMMANDE URGENTE",
            "BESOIN TRANSFERT IN",
            "QUANTITE A TRANSFERER OUT",
            "CATEGORIE_DU_PRODUIT",
        ]
    ]

    stock_lvl_decent = (
        df_etat_stock[["CODE", "PROGRAMME"]]
        .drop_duplicates()
        .rename(columns={"CODE": "Code", "PROGRAMME": "Programme"})
    )

    stock_lvl_decent = stock_lvl_decent.drop_duplicates()

    column_to_caculate = {
        "Designation": "PRODUIT",
        "Categorie": "CATEGORIE_DU_PRODUIT",
        "Unite": "UNITE DE RAPPORTAGE",
        "lvl_decent_conso": "QUANTITE UTILISEE",
        "lvl_decent_sdu": "SDU",
        "lvl_decent_cmm": "CMM gestionnaire",
        "Categorie_produit": "CATEGORIE PRODUIT",
    }

    for column, col_in_extract_stock in column_to_caculate.items():
        if column in ("lvl_decent_conso", "lvl_decent_sdu", "lvl_decent_cmm"):
            stock_lvl_decent[column] = stock_lvl_decent.apply(
                lambda row: df_etat_stock.loc[
                    (df_etat_stock.CODE == row.Code)
                    & (df_etat_stock["PROGRAMME"] == row.Programme),
                    col_in_extract_stock,
                ].sum()
                if not df_etat_stock.loc[
                    (df_etat_stock.CODE == row.Code)
                    & (df_etat_stock["PROGRAMME"] == row.Programme),
                    col_in_extract_stock,
                ].empty
                else "",
                axis=1,
            )
        else:
            stock_lvl_decent[column] = stock_lvl_decent.apply(
                lambda row: df_etat_stock.loc[
                    (df_etat_stock.CODE == row.Code)
                    & (df_etat_stock["PROGRAMME"] == row.Programme),
                    col_in_extract_stock,
                ].iloc[0]
                if not df_etat_stock.loc[
                    (df_etat_stock.CODE == row.Code)
                    & (df_etat_stock["PROGRAMME"] == row.Programme),
                    col_in_extract_stock,
                ].empty
                else "",
                axis=1,
            )

    def calculate_msd(row):
        if sum([row.lvl_decent_conso, row.lvl_decent_sdu, row.lvl_decent_cmm]) == 0:
            return "NA"
        else:
            if row.lvl_decent_sdu == 0 and row.lvl_decent_cmm == 0:
                return "NA"
            else:
                if row.lvl_decent_sdu > 0 and row.lvl_decent_cmm == 0:
                    return row.lvl_decent_sdu
                else:
                    return row.lvl_decent_sdu / row.lvl_decent_cmm

    stock_lvl_decent["lvl_decent_msd"] = stock_lvl_decent.apply(calculate_msd, axis=1)

    def get_satut(row):
        if row.lvl_decent_msd == "NA":
            if (
                df_etat_stock.loc[
                    (df_etat_stock.CODE == row.Code) & (df_etat_stock.PROGRAMME == row.Programme),
                    "SDU",
                ].sum()
                > 0
            ):
                return "STOCK DORMANT"
            else:
                return "NA"
        elif row.lvl_decent_msd < 0.05:
            return "RUPTURE"
        elif row.lvl_decent_msd > 0 and row.lvl_decent_msd < 2:
            return "SOUS-STOCK"
        elif row.lvl_decent_msd >= 2 and row.lvl_decent_msd <= 4:
            return "BIEN STOCKE"
        elif row.lvl_decent_msd > 4:
            return "SURSTOCK"
        else:
            return "ND"

    stock_lvl_decent["lvl_decent_statut"] = stock_lvl_decent.apply(get_satut, axis=1)

    df_count_prog = stock_lvl_decent["Programme"].value_counts().reset_index()
    df_count_prog["dispo_globale_cible"] = 0.85 / df_count_prog["count"]
    df_count_prog["dispo_traceur_cible"] = 0.95 / df_count_prog["count"]

    stock_lvl_decent["dispo_globale"] = stock_lvl_decent["Programme"].apply(
        lambda prog: df_etat_stock.loc[
            (df_etat_stock.PROGRAMME == prog) & (df_etat_stock["ETAT DU STOCK"] != "RUPTURE")
        ].shape[0]
        / df_count_prog.loc[df_count_prog["Programme"] == prog, "count"].values[0]
    )
    df_ = (
        df_etat_stock["PROGRAMME"]
        .value_counts()
        .reset_index()
        .rename(columns={"PROGRAMME": "Programme"})
    )
    stock_lvl_decent["dispo_globale"] = stock_lvl_decent.apply(
        lambda row: row.dispo_globale
        / df_.loc[df_["Programme"] == row.Programme, "count"].values[0],
        axis=1,
    )

    stock_lvl_decent["dispo_traceur"] = stock_lvl_decent["Programme"].apply(
        lambda prog: df_etat_stock.loc[
            (df_etat_stock.PROGRAMME == prog)
            & (df_etat_stock["CATEGORIE PRODUIT"].str.upper() == "PRODUIT TRACEUR")
            & (df_etat_stock["ETAT DU STOCK"] != "RUPTURE")
        ].shape[0]
        / df_count_prog.loc[df_count_prog["Programme"] == prog, "count"].values[0]
    )  # Bien mais pour l'heure pas optimale
    
    df_ = (
        df_etat_stock.loc[
            (df_etat_stock["CATEGORIE PRODUIT"].str.upper() == "PRODUIT TRACEUR"), "PROGRAMME"
        ]
        .value_counts()
        .reset_index()
        .rename(columns={"PROGRAMME": "Programme"})
    )
    
    stock_lvl_decent["dispo_traceur"] = stock_lvl_decent.apply(
        lambda row: row.dispo_traceur
        / df_.loc[df_["Programme"] == row.Programme, "count"].values[0],
        axis=1,
    )

    stock_lvl_decent = stock_lvl_decent.merge(
        df_count_prog.drop(columns="count"), on="Programme", how="left"
    )

    cols = [
        "dispo_globale",
        "dispo_globale_cible",
        "dispo_traceur",
        "dispo_traceur_cible",
        "Categorie_produit",
    ]

    stock_lvl_decent = stock_lvl_decent[
        [col for col in stock_lvl_decent.columns if col not in cols] + cols
    ]

    del df_count_prog, df_, cols

    stock_region = (
        df_etat_stock[["CODE", "PROGRAMME", "REGION"]]
        .drop_duplicates()
        .rename(columns={"CODE": "Code", "REGION": "Region", "PROGRAMME": "Programme"})
    )

    df_ = (
        df_etat_stock.groupby(["CODE", "PROGRAMME", "REGION"])[["CMM gestionnaire", "SDU"]]
        .sum()
        .reset_index()
    )

    def get_msd_region(row):
        try:
            filtered_data = df_[
                (df_.CODE == row.Code)
                & (df_.REGION == row.Region)
                & (df_["PROGRAMME"] == row.Programme)
            ]
            cmm_sum = filtered_data["CMM gestionnaire"].sum()
            if cmm_sum == 0:
                return "NA"
            else:
                sdu_sum = filtered_data["SDU"].sum()
                return sdu_sum / cmm_sum
        except Exception as e:
            print(e)
            return ""

    stock_region["MSD"] = stock_region.apply(get_msd_region, axis=1)

    def get_statut_stock_region(row):
        try:
            if row.MSD == "NA":
                if (
                    df_[
                        (df_.CODE == row.Code)
                        & (df_.REGION == row.Region)
                        & (df_["PROGRAMME"] == row.Programme)
                    ]["SDU"].sum()
                    > 0
                ):
                    return "STOCK DORMANT"
                else:
                    return "NA"
            elif row.MSD < 0.05:
                return "RUPTURE"
            elif row.MSD > 0 and row.MSD < 2:
                return "SOUS-STOCK"
            elif row.MSD >= 2 and row.MSD <= 4:
                return "BIEN STOCKE"
            elif row.MSD > 4:
                return "SURSTOCK"
            else:
                return "ND"
        except:
            return "ND"

    stock_region["STATUT"] = stock_region.apply(get_statut_stock_region, axis=1)

    return df_etat_stock, stock_lvl_decent, stock_region


def aggregate_regional_stock_availability_metrics(stock_lvl_decent, stock_region):
    """Agrège les métriques de disponibilité stock au niveau régional et national."""
    stock_lvl_decent["Region"] = "NATIONAL"

    stock_national = stock_lvl_decent[
        [
            "Code",
            "Programme",
            "Region",
            "lvl_decent_msd",
            "lvl_decent_statut",
            "lvl_decent_conso",
            "lvl_decent_sdu",
            "lvl_decent_cmm",
            "dispo_globale",
            "dispo_globale_cible",
            "dispo_traceur",
            "dispo_traceur_cible",
        ]
    ].rename(
        columns={
            "lvl_decent_msd": "MSD",
            "lvl_decent_statut": "STATUT",
            "lvl_decent_conso": "conso_lvl_national",
            "lvl_decent_sdu": "sdu_lvl_national",
            "lvl_decent_cmm": "cmm_lvl_national",
        }
    )

    stock_region_with_central = pd.concat([stock_region, stock_national])

    df_sheet_two = pd.merge(
        stock_lvl_decent[["Code", "Programme"]], stock_region[["Code", "Region"]], how="inner"
    )[["Region", "Programme"]].drop_duplicates()

    df_sheet_two = df_sheet_two.sort_values(by=["Region"])

    df_sheet_two = pd.concat(
        [
            df_sheet_two,
            pd.DataFrame(
                {"Region": "NATIONAL", "Programme": stock_lvl_decent["Programme"].unique()}
            ),
        ]
    ).reset_index()[["Region", "Programme"]]

    df_sheet_two = pd.concat(
        [
            df_sheet_two,
            pd.DataFrame({"Region": df_sheet_two["Region"].unique(), "Programme": "TOUS"}),
        ]
    ).sort_values(["Region", "Programme"])

    df = pd.merge(
        stock_lvl_decent[["Code", "Categorie_produit", "Programme", "lvl_decent_statut"]],
        stock_region[["Code", "Region", "STATUT"]],
        how="inner",
    )

    def get_dispo_global(row):
        if row.Region == "NATIONAL":
            total_codes_programme = (
                df.loc[df.Programme == row.Programme, "Code"].unique().shape[0] - 1
                if row.Programme != "TOUS"
                else df["Code"].unique().shape[0] - 1
            )
            codes_en_rupture = (
                df.loc[
                    (df.lvl_decent_statut == "RUPTURE") & (df.Programme == row.Programme), "Code"
                ]
                .unique()
                .shape[0]
                if row.Programme != "TOUS"
                else df.loc[(df.lvl_decent_statut == "RUPTURE"), "Code"].unique().shape[0]
            )
        else:
            total_codes_programme = (
                df.loc[df.Programme == row.Programme, "Code"].unique().shape[0] - 1
                if row.Programme != "TOUS"
                else df["Code"].unique().shape[0] - 1
            )
            codes_en_rupture = (
                df.loc[
                    (df.Region == row.Region)
                    & (df.STATUT == "RUPTURE")
                    & (df.Programme == row.Programme),
                    "Code",
                ]
                .unique()
                .shape[0]
                if row.Programme != "TOUS"
                else df.loc[(df.Region == row.Region) & (df.STATUT == "RUPTURE"), "Code"]
                .unique()
                .shape[0]
            )

        return 1 - (codes_en_rupture / total_codes_programme)

    def get_dispo_traceur(row):
        if row.Region == "NATIONAL":
            total_codes_traceur = (
                stock_lvl_decent.loc[
                    (stock_lvl_decent.Programme == row.Programme)
                    & (stock_lvl_decent.Categorie_produit == "Produit traceur"),
                    "Code",
                ]
                .unique()
                .shape[0]
                if row.Programme != "TOUS"
                else stock_lvl_decent.loc[
                    (stock_lvl_decent.Categorie_produit == "Produit traceur"), "Code"
                ]
                .unique()
                .shape[0]
            )
            codes_traceur_en_rupture = (
                stock_lvl_decent.loc[
                    (stock_lvl_decent.lvl_decent_statut == "RUPTURE")
                    & (stock_lvl_decent.Programme == row.Programme)
                    & (stock_lvl_decent.Categorie_produit == "Produit traceur"),
                    "Code",
                ]
                .unique()
                .shape[0]
                if row.Programme != "TOUS"
                else stock_lvl_decent.loc[
                    (stock_lvl_decent.lvl_decent_statut == "RUPTURE")
                    & (stock_lvl_decent.Categorie_produit == "Produit traceur"),
                    "Code",
                ]
                .unique()
                .shape[0]
            )
        else:
            total_codes_traceur = (
                df.loc[
                    (df.Programme == row.Programme) & (df.Categorie_produit == "Produit traceur"),
                    "Code",
                ]
                .unique()
                .shape[0]
                if row.Programme != "TOUS"
                else df.loc[(df.Categorie_produit == "Produit traceur"), "Code"].unique().shape[0]
            )
            codes_traceur_en_rupture = (
                df.loc[
                    (df.Region == row.Region)
                    & (df.STATUT == "RUPTURE")
                    & (df.Programme == row.Programme)
                    & (df.Categorie_produit == "Produit traceur"),
                    "Code",
                ]
                .unique()
                .shape[0]
                if row.Programme != "TOUS"
                else df.loc[
                    (df.Region == row.Region)
                    & (df.STATUT == "RUPTURE")
                    & (df.Categorie_produit == "Produit traceur"),
                    "Code",
                ]
                .unique()
                .shape[0]
            )

        return 1 - (codes_traceur_en_rupture / total_codes_traceur)

    df_sheet_two["dispo_globale"] = df_sheet_two.apply(get_dispo_global, axis=1)

    df_sheet_two["dispo_traceur"] = df_sheet_two.apply(get_dispo_traceur, axis=1)

    return stock_lvl_decent, stock_region, df_sheet_two, stock_region_with_central