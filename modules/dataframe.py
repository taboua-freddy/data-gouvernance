import re
import phpserialize
import pandas as pd
import numpy as np
import json
from modules.mylogging import MyLogger
class ParseDFTypes:
    """
    Classe pour analyser et déterminer les types de données dans un DataFrame pandas.
    Elle prétraite également les données, notamment en désérialisant les objets PHP en JSON.
    """
    def __init__(self):
        self.__df = None
        self.logger = MyLogger('ParseDFTypes', with_file=False)

    @property
    def df(self) -> pd.DataFrame:
        return self.__df

    @df.setter
    def df(self, value: pd.DataFrame):
        self.__df = value.copy()

    def _is_serialized_php(self, data: any) -> bool:
        """Détecte si une chaîne est un objet PHP sérialisé."""
        if isinstance(data, str):
            return re.match(r'^[aOs]:', data.strip()) is not None
        return False

    def _bytes_to_str(self, data: any):
        """Convertit récursivement les objets bytes en str dans les structures de données."""
        if isinstance(data, dict):
            return {self._bytes_to_str(key): self._bytes_to_str(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._bytes_to_str(element) for element in data]
        elif isinstance(data, tuple):
            return tuple(self._bytes_to_str(element) for element in data)
        elif isinstance(data, bytes):
            return data.decode('utf-8')
        else:
            return data

    def _php_object_hook(self, name, attrs):
        """Fonction pour gérer la désérialisation des objets PHP."""
        result = {'__php_class_name': name.decode('utf-8') if isinstance(name, bytes) else name}
        for key, value in attrs.items():
            result[self._bytes_to_str(key)] = value
        return result

    def _deserialize_php_to_json(self, data: any) -> str | None:
        """Désérialise une chaîne PHP et la convertit en JSON."""
        try:
            deserialized_data = phpserialize.loads(
                data.encode('utf-8'),
                object_hook=self._php_object_hook,
                decode_strings=True
            )
            deserialized_data = self._bytes_to_str(deserialized_data)
            return json.dumps(deserialized_data)
        except Exception as e:
            self.logger.error(f"Erreur de désérialisation : {e}")
            return None

    def get_postgres_type(self, dtype, series: pd.Series = None) -> str:
        """Convertit un dtype de pandas en un type de colonne PostgreSQL plus précis."""
        if pd.api.types.is_integer_dtype(dtype):
            if series is not None and series.max() > np.iinfo(np.int32).max:
                return "BIGINT"
            elif series is not None and series.nunique() == 2 and set(series.unique()) == {0, 1}:
                return "BOOLEAN"
            else:
                return "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            return "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        elif pd.api.types.is_object_dtype(dtype):
            if series is not None and series.apply(self._is_serialized_php).any():
                return "JSONB"
            if series is not None:
                max_length = series.astype(str).str.len().max()
                if pd.isna(max_length):
                    return "TEXT"
                max_length = int(max_length)
                if max_length <= 255:
                    return "VARCHAR"#f"VARCHAR({max_length})"
                else:
                    return "TEXT"
            else:
                return "TEXT"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "TIMESTAMP"
        else:
            return "TEXT"

    def preprocess_data(self, df: pd.DataFrame = None, with_copy: bool = False) -> pd.DataFrame:
        """Préprocesser les colonnes contenant des données sérialisées PHP pour les convertir en JSON."""
        if df is None:
            df = self.df
        _df = df.copy() if with_copy else df
        for col in df.columns:
            if _df[col].apply(self._is_serialized_php).any():
                _df[col] = _df[col].apply(self._deserialize_php_to_json)
        return _df