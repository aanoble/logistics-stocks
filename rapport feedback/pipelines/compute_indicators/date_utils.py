import pandas as pd

FRENCH_MONTHS = [
    "",
    "JANVIER",
    "FEVRIER",
    "MARS",
    "AVRIL",
    "MAI",
    "JUIN",
    "JUILLET",
    "AOUT",
    "SEPTEMBRE",
    "OCTOBRE",
    "NOVEMBRE",
    "DECEMBRE",
]
QUARTER_MONTHS = {3, 6, 9, 12}


def get_date_report(date_str: str) -> tuple | str:
    """Transforme une date en format de rapport français avec logique trimestrielle."""
    try:
        dt = pd.to_datetime(date_str, dayfirst=True)
    except pd.errors.ParserError:
        try:
            dt = pd.to_datetime(date_str, yearfirst=True)
        except pd.errors.ParserError as e:
            raise ValueError(f"Format de date invalide: {date_str}") from e

    month, year = dt.month, dt.year
    current_period = f"{FRENCH_MONTHS[month]} {year}"

    if month in QUARTER_MONTHS:
        prev_month = (month - 2) % 12 or 12  # Gestion du cycle annuel
        return (f"{FRENCH_MONTHS[prev_month]} {current_period}", current_period)
    return f"('{current_period}')"
