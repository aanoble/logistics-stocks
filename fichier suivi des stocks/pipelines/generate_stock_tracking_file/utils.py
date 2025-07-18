import pandas as pd

from .constants import DICO_MOIS_FR


def find_best_match(element, values, threshold=95):
    """
    Finds the best match for a given element within a list of values using fuzzy matching.
    This function searches for the exact position of an element in a list of values. If the exact element is not found,
    it uses fuzzy matching to find the closest match based on a specified threshold.
    Args:
        element (str): The element to search for in the list of values.
        values (list of str): The list of values to search within.
        threshold (int, optional): The minimum score for a fuzzy match to be considered valid. Defaults to 95.
    Returns:
        int or None: The 1-based index of the best match in the list of values, or None if no match is found.
    """
    from fuzzywuzzy import fuzz, process

    try:
        if element in values:
            return values.index(element) + 1
        best_match = process.extractOne(element, values, scorer=fuzz.token_set_ratio)
        if best_match[1] >= threshold:
            return values.index(best_match[0]) + 1
        return None
    except Exception:
        return None


def has_formula(cell):
    """
    Cette fonction permet d'évaluer si une cellule contient une formule
    """
    return isinstance(cell.value, str) and cell.value.startswith("=")


def get_current_variable(date_report: str):
    """
    Converts a given date string into specific date formats and returns them.
    Args:
        date_report (str): The date string in the format 'dd/mm/yyyy'.
    Returns:
        tuple: A tuple containing:
            - date_format (Timestamp): The converted date in pandas Timestamp format.
            - month_year_str (str): The month and year of the given date in uppercase French.
            - prev_month_year_str (str): The month and year of the previous day in uppercase French.
    """

    import calendar

    date_format = pd.to_datetime(date_report, format="%d/%m/%Y")

    month_year_str = (
        DICO_MOIS_FR[calendar.month_name[date_format.month]].upper() + " " + str(date_format.year)
    )

    prev_date = date_format - pd.Timedelta(days=1)

    prev_month_year_str = (
        DICO_MOIS_FR[calendar.month_name[prev_date.month]].upper() + " " + str(prev_date.year)
    )

    return date_format, month_year_str, prev_month_year_str
