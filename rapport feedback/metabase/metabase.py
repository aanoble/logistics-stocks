from typing import Optional
from urllib.parse import urlparse

import pandas as pd
import requests
from openhexa.sdk import CustomConnection


class MetabaseError(Exception):
    pass


class Metabase:
    def __init__(self, connection: CustomConnection):
        self.api = Api(connection)

    def get_data_from_sql_query(
        self, sql_query: str, database_id: int = 3, chunk_size: int = 2000
    ) -> pd.DataFrame:
        """
        Exécute une requête SQL sur Metabase avec pagination automatique.

        Args:
            sql_query: Requête SQL avec {limit} et {offset} comme paramètres de pagination
            database_id: ID de la base Metabase
            chunk_size: Nombre de lignes par requête (2000 par défaut)

        Returns:
            DataFrame combinant tous les résultats

        Raises:
            ValueError en cas d'erreur
        """
        try:
            sql_query = self._prepare_sql_query(sql_query)
            data_frames = []
            offset = 0
            names = None

            while True:
                df, names = self._fetch_chunk(sql_query, database_id, chunk_size, offset, names)
                if df.empty:
                    break
                data_frames.append(df)
                offset += len(df)
                if len(df) < chunk_size:
                    break

            return pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()
        except Exception as e:
            raise ValueError(f"Erreur lors de la récupération des données: {e}") from e

    def _prepare_sql_query(self, sql_query: str) -> str:
        """Valide et formate la requête SQL avec les paramètres de pagination."""
        sql_query = sql_query.rstrip(";")
        required_params = {"{limit}", "{offset}"}

        if not required_params.issubset(sql_query):
            if "LIMIT" not in sql_query.upper():
                sql_query += "\nLIMIT {limit}"
            if "OFFSET" not in sql_query.upper():
                sql_query += "\nOFFSET {offset}"

        if not all(param in sql_query for param in required_params):
            raise ValueError("La requête SQL doit contenir les paramètres {limit} et {offset}")

        return sql_query

    def _fetch_chunk(
        self, sql_query: str, database_id: int, chunk_size: int, offset: int, names: Optional[list]
    ) -> tuple[pd.DataFrame, list]:
        """Récupère un segment de données et gère les métadonnées."""
        try:
            response = self.api.session.post(
                f"{self.api.url}/dataset",
                headers={"Content-Type": "application/json", "X-Metabase-Session": self.api.token},
                json={
                    "database": database_id,
                    "type": "native",
                    "native": {"query": sql_query.format(limit=chunk_size, offset=offset)},
                },
                # timeout=15
            )
            response.raise_for_status()
            data = response.json()["data"]

            # Extraction des noms de colonnes
            if names is None:
                names = [col["display_name"] for col in data["results_metadata"]["columns"]]

            df = pd.DataFrame(data["rows"])
            if not df.empty:
                df.columns = names

            return df, names

        except requests.exceptions.RequestException as e:
            raise ValueError(f"Erreur réseau: {e}") from e
        except (KeyError, TypeError) as e:
            raise ValueError(f"Structure de réponse invalide: {e}") from e


class Api:
    def __init__(self, connection: CustomConnection):
        self._validate_connection(connection)
        self.url = self.parse_url(connection.url)
        self.token = None
        self.session = self.authenticate(connection.username, connection.password)

    @staticmethod
    def _validate_connection(connection: CustomConnection):
        """Valide les paramètres de connexion."""
        if not connection:
            raise MetabaseError("Connexion requise")
        if not all([connection.url, connection.username, connection.password]):
            raise MetabaseError("URL, utilisateur et mot de passe requis")

    @staticmethod
    def parse_url(url: str) -> str:
        """Formate l'URL de l'API Metabase."""
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            raise MetabaseError(f"URL invalide: {url}")
        return f"{parsed.scheme}://{parsed.netloc}/api"

    def authenticate(self, username: str, password: str) -> requests.Session:
        """Authentification avec gestion robuste des erreurs."""
        session = requests.Session()
        try:
            response = session.post(
                f"{self.url}/session",
                headers={"Content-Type": "application/json"},
                json={"username": username, "password": password},
                timeout=15,
            )
            response.raise_for_status()
            if not (token := response.json().get("id")):
                raise MetabaseError("Token absent de la réponse")
            self.token = token
            return session
        except requests.JSONDecodeError as e:
            raise MetabaseError("Réponse d'authentification invalide") from e