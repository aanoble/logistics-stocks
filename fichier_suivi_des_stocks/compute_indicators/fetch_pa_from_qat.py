import json
from datetime import datetime
from typing import Iterable

import pandas as pd
import polars as pl
import requests

AUTH_URL = "https://api.quantificationanalytics.org/authenticate"
VERSION_URL = (
    "https://api.quantificationanalytics.org/api/dropdown/version/filter/sp/programId/{program_id}"
)
SHIPMENT_DETAILS_URL = "https://api.quantificationanalytics.org/api/report/shipmentDetails"

PROGRAM_MAPPING = {
    "PNLP": [2101],
    "PNLS": [2948, 2949, 2951, 2950, 2952, 2954, 2953, 2955],
    "PNLT": [2957],
    "PNN": [2956],
    "PNSME": [2947],
}

DEFAULT_HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.quantificationanalytics.org",
}

COLS = [
    "shipmentId",
    "shipmentQty",
    "expectedDeliveryDate",
    "productCost",
    "freightCost",
    "totalCost",
    "orderNo",
    "emergencyOrder",
    "localProcurement",
    "erpFlag",
    "notes",
    "planningUnit.id",
    "planningUnit.label.label_en",
    "planningUnit.label.label_fr",
    "program.id",
    "procurementAgent.id",
    "procurementAgent.label.label_en",
    "procurementAgent.label.label_fr",
    "procurementAgent.code",
    "fundingSource.label.label_en",
    "fundingSource.label.label_fr",
    "fundingSource.code",
    "budget.label.label_en",
    "budget.label.label_fr",
    "budget.code",
    "shipmentStatus.id",
    "shipmentStatus.label.label_en",
    "shipmentStatus.label.label_fr",
]


def get_auth_headers(credentials: dict[str, str]) -> dict[str, str]:
    response = requests.post(
        AUTH_URL,
        headers=DEFAULT_HEADERS,
        data=json.dumps(credentials),
        timeout=60,
    )
    response.raise_for_status()
    auth_token = response.json().get("token")
    if not auth_token:
        raise ValueError("Token d'authentification introuvable dans la réponse API.")

    return {
        **DEFAULT_HEADERS,
        "Authorization": f"Bearer {auth_token}",
    }


def resolve_program_ids(programme_id: str | int | Iterable[int]) -> list[int]:
    if isinstance(programme_id, int):
        return [programme_id]

    if isinstance(programme_id, str):
        code = programme_id.upper().strip()
        if code in PROGRAM_MAPPING:
            return PROGRAM_MAPPING[code]
        if code.isdigit():
            return [int(code)]
        raise ValueError(
            f"Programme inconnu: {programme_id}. Utiliser un code {list(PROGRAM_MAPPING)} ou un ID numérique."
        )

    return [int(program_id) for program_id in programme_id]


def get_latest_version_metadata(program_id: int, headers: dict[str, str]) -> tuple[int, str]:
    version_response = requests.get(
        VERSION_URL.format(program_id=program_id),
        headers=headers,
        timeout=60,
    )
    version_response.raise_for_status()

    df_version = pd.json_normalize(version_response.json())
    if df_version.empty:
        raise ValueError(f"Aucune version disponible pour programId={program_id}")

    version_id = int(df_version["versionId"].max())
    created_date = str(df_version["createdDate"].iloc[-1]).split()[0]
    return version_id, created_date


def fetch_shipment_details(
    program_id: int,
    version_id: int,
    headers: dict[str, str],
    start_date: str,
    stop_date: str,
) -> pd.DataFrame:
    payload = {
        "programIds": [str(program_id)],
        "versionId": str(version_id),
        "startDate": start_date,
        "stopDate": stop_date,
        "reportView": "1",
    }
    shipment_response = requests.post(
        SHIPMENT_DETAILS_URL,
        headers=headers,
        data=json.dumps(payload),
        timeout=120,
    )
    shipment_response.raise_for_status()

    details = shipment_response.json().get("shipmentDetailsList", [])
    return pd.json_normalize(details)


def transform_pa_dataframe(
    shipment_df: pd.DataFrame,
    version_id: int,
    created_date: str,
    program_id: int,
) -> pl.DataFrame:
    if shipment_df.empty:
        return pl.DataFrame(
            {
                "program_id": [],
                "ID de produit QAT / Identifiant de produit (prévision)": [],
                "Produit (planification) / Produit (prévision)": [],
                "ID de l`envoi QAT": [],
                "Commande d`urgence": [],
                "Commande PGI": [],
                "Approvisionnement local": [],
                "N° de commande de l`agent d`approvisionnement": [],
                "Agent d`approvisionnement": [],
                "Source de financement": [],
                "Budget": [],
                "État": [],
                "Quantité": [],
                "date de réception": [],
                "Coût unitaire de produit (USD)": [],
                "Coût du fret (USD)": [],
                "Coût total (USD)": [],
                "Notes": [],
                "version_pa": [],
                "date_extraction_pa": [],
            }
        )

    for column in COLS:
        if column not in shipment_df.columns:
            shipment_df[column] = None

    df_pa = pl.DataFrame(shipment_df[COLS]).select(
        pl.lit(program_id).alias("program_id"),
        pl.col("planningUnit.id").alias("ID de produit QAT / Identifiant de produit (prévision)"),
        pl.when(pl.col("planningUnit.label.label_fr").is_null())
        .then(pl.col("planningUnit.label.label_en"))
        .otherwise(pl.col("planningUnit.label.label_fr"))
        .alias("Produit (planification) / Produit (prévision)"),
        pl.col("shipmentId").alias("ID de l`envoi QAT"),
        pl.col("emergencyOrder").alias("Commande d`urgence"),
        pl.col("erpFlag").alias("Commande PGI"),
        pl.col("localProcurement").alias("Approvisionnement local"),
        pl.col("procurementAgent.id").alias("N° de commande de l`agent d`approvisionnement"),
        pl.col("procurementAgent.code").alias("Agent d`approvisionnement"),
        pl.col("fundingSource.code").alias("Source de financement"),
        pl.col("budget.code").alias("Budget"),
        pl.when(pl.col("shipmentStatus.label.label_fr").is_null())
        .then(pl.col("shipmentStatus.label.label_en"))
        .otherwise(pl.col("shipmentStatus.label.label_fr"))
        .alias("État"),
        pl.col("shipmentQty").alias("Quantité"),
        pl.col("expectedDeliveryDate").alias("date de réception"),
        pl.col("productCost").alias("Coût unitaire de produit (USD)"),
        pl.col("freightCost").alias("Coût du fret (USD)"),
        pl.col("totalCost").alias("Coût total (USD)"),
        pl.col("notes").alias("Notes"),
        pl.lit(version_id).alias("version_pa"),
        pl.lit(created_date).alias("date_extraction_pa"),
    )
    return df_pa


def extract_pa(
    programme_id: str | int | Iterable[int],
    credentials: dict[str, str],
    date_report: str,
) -> pd.DataFrame:
    headers = get_auth_headers(credentials)
    program_ids = resolve_program_ids(programme_id)

    year = datetime.strptime(date_report, "%Y-%m-%d").year
    start_date: str = f"{year}-01-01"
    stop_date: str = f"{year + 1}-12-31"

    frames: list[pl.DataFrame] = []
    for program_id in program_ids:
        version_id, created_date = get_latest_version_metadata(program_id, headers)
        shipment_df = fetch_shipment_details(
            program_id=program_id,
            version_id=version_id,
            headers=headers,
            start_date=start_date,
            stop_date=stop_date,
        )
        frames.append(
            transform_pa_dataframe(
                shipment_df=shipment_df,
                version_id=version_id,
                created_date=created_date,
                program_id=program_id,
            )
        )

    if not frames:
        return pl.DataFrame().to_pandas()
    return pl.concat(frames, how="vertical").drop("program_id").to_pandas()
