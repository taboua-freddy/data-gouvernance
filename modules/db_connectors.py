import re
import psycopg2
from modules.dataframe import ParseDFTypes
from modules.mylogging import MyLogger
from modules.utils import read_json_file
from psycopg2 import sql
import pandas as pd


def _split_schema_table(full_table_name: str, default_schema=None) -> tuple[str, str]:
    if '.' in full_table_name:
        schema_name, table_name = full_table_name.split('.', 1)
    else:
        schema_name = default_schema
        table_name = full_table_name

    if default_schema is not None:
        schema_name = default_schema

    return schema_name, table_name

def _drop_schema_query(schema_name:str) -> str:
    return f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE;'

def _create_schema_query(schema_name:str) -> str:
    return f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";'

class PostgresConnection:
    """
    Classe pour gérer la connexion à PostgreSQL et effectuer des opérations telles que la création de schémas,
    de tables, le chargement de données, et la création de contraintes.
    """
    def __init__(self, connection_params: dict, parser: ParseDFTypes,schema_name, constraints_file: str=None ):
        self.logger = MyLogger('PostgresConnection', with_file=False)
        self.connection = self.get_postgres_connection(connection_params)
        if self.connection is None:
            raise Exception("Impossible de se connecter à PostgreSQL.")
        self.parser = parser
        self.schema_name = schema_name
        print("Ouverture du fichier des contraintes")
        self.primary_key_constraints, self.foreign_key_constraints, self.unique_constraints = None, None, None
        if constraints_file is not None:
             self.load_constraints(constraints_file)

    def __enter__(self):
        return self.connection.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_postgres_connection()

    def get_postgres_connection(self, connection_params: dict) -> psycopg2.connect:
        try:
            connection = psycopg2.connect(**connection_params)
            self.logger.info("Connexion à PostgreSQL réussie.")
            return connection
        except Exception as e:
            self.logger.error(f"Erreur lors de la connexion à PostgreSQL : {e}")
            return None

    def execute_query(self, query: str):
        """Exécuter une requête SQL."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
            self.connection.commit()
            self.logger.info(f"Requête exécutée avec succès : {query}")
        except Exception as e:
            self.logger.error(f"Erreur lors de l'exécution de la requête : {e}\nRequête : {query}")



    def _drop_table_query(self, table_name: str,schema_name:str=None) -> str:
        _schema_name = self.schema_name if schema_name is None else schema_name
        return f'DROP TABLE IF EXISTS "{_schema_name}"."{table_name}";'

    def generate_table_schema(self, df: pd.DataFrame, table_name: str,schema_name:str=None) -> str:
        """Génère la requête SQL pour créer une table en fonction du DataFrame."""
        columns = []
        for col in df.columns:
            col_type = self.parser.get_postgres_type(df[col].dtype, df[col])
            columns.append(f'"{col}" {col_type}')
        columns_schema = ", ".join(columns)
        _schema_name = self.schema_name if schema_name is None else schema_name
        create_table_query = f'CREATE TABLE "{_schema_name}"."{table_name}" ({columns_schema});'
        return create_table_query

    def drop_schema_in_postgres(self, schema_name: str = None):
        _schema_name = self.schema_name if schema_name is None else schema_name
        query = _drop_schema_query(_schema_name)
        self.execute_query(query)
        self.logger.info(f"Schéma '{_schema_name}' supprimé.")

    def create_schema_in_postgres(self, schema_name: str = None):
        _schema_name = self.schema_name if schema_name is None else schema_name
        query = _create_schema_query(_schema_name)
        self.execute_query(query)
        self.logger.info(f"Schéma '{_schema_name}' créé.")

    def drop_table_if_exists(self, table_name: str, schema_name: str = None):
        _schema_name = self.schema_name if schema_name is None else schema_name
        query = self._drop_table_query(table_name, _schema_name)
        self.execute_query(query)
        self.logger.info(f"Table '{_schema_name}.{table_name}' supprimée si elle existait.")

    def create_table_in_postgres(self, create_table_query: str, with_drop: bool = False):
        if with_drop:
            match = re.search(r'CREATE TABLE "(.+?)"\."(.+?)"', create_table_query)
            if match:
                schema_name = match.group(1)
                table_name = match.group(2)
                self.drop_table_if_exists(table_name, schema_name)
        self.execute_query(create_table_query)
        self.logger.info(f"Table créée avec la requête :\n{create_table_query}")

    def load_csv_to_postgres(self, csv_file: str, table_name: str, schema_name: str = None):
        """Charge les données d'un fichier CSV dans la table spécifiée."""
        _schema_name = self.schema_name if schema_name is None else schema_name
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                with self.connection.cursor() as cursor:
                    cursor.copy_expert(f'COPY "{_schema_name}"."{table_name}" FROM STDIN WITH CSV HEADER', f)
            self.connection.commit()
            self.logger.info(f"Données chargées dans la table '{_schema_name}.{table_name}' depuis le fichier '{csv_file}'.")
        except Exception as e:
            self.logger.error(f"Erreur lors du chargement du fichier CSV : {e}")

    def close_postgres_connection(self):
        if self.connection is not None:
            self.connection.close()
            self.logger.info("Connexion à PostgreSQL fermée.")

    def init_schema(self, schema_name: str=None):
        self.drop_schema_in_postgres(schema_name)
        self.create_schema_in_postgres(schema_name)

    def load_constraints(self, constraints_file: str = "constraints.json"):
        """Charge les contraintes de clés primaires et étrangères."""
        constraints = read_json_file(constraints_file)
        self.primary_key_constraints = constraints.get('primary_keys', [])
        self.foreign_key_constraints = constraints.get('foreign_keys', [])
        self.unique_constraints = constraints.get('unique_keys', [])

    def _generate_primary_key_queries(self, schema: str = None) -> list[str]:
        """Génère les requêtes SQL pour les clés primaires."""
        queries = []
        for constraint in self.primary_key_constraints:
            table_name = constraint['table_name']
            schema_name, pure_table_name =  _split_schema_table(table_name, schema)
            column_name = constraint['column_name']
            constraint_name = f"{pure_table_name}_{column_name}_pk"
            query = f'ALTER TABLE "{schema_name}"."{pure_table_name}" ADD CONSTRAINT "{constraint_name}" PRIMARY KEY ("{column_name}");'
            queries.append(query)
        return queries

    def _generate_foreign_key_queries(self, schema: str = None) -> list[str]:
        """Génère les requêtes SQL pour les clés étrangères."""
        queries = []
        for constraint in self.foreign_key_constraints:
            table_name = constraint['table_name']
            schema_name, pure_table_name = _split_schema_table(table_name, schema)
            column_name = constraint['column_name']
            reference_table = constraint['reference_table']
            ref_schema_name, ref_table_name = _split_schema_table(reference_table, schema)
            reference_column = constraint['reference_column']
            constraint_name = f"{pure_table_name}_{column_name}_fk"
            query = f'ALTER TABLE "{schema_name}"."{pure_table_name}" ADD CONSTRAINT "{constraint_name}" FOREIGN KEY ("{column_name}") REFERENCES "{ref_schema_name}"."{ref_table_name}" ("{reference_column}");'
            queries.append(query)
        return queries

    def create_primary_keys_in_postgres(self,schema_name: str = None):
        """Crée les clés primaires dans la base de données."""
        primary_key_queries = self._generate_primary_key_queries(schema_name)
        for query in primary_key_queries:
            self.execute_query(query)
        self.logger.info(f"Clés primaires créées avec succès.")

    def create_foreign_keys_in_postgres(self,schema_name: str = None):
        """Crée les clés étrangères dans la base de données."""
        foreign_key_queries = self._generate_foreign_key_queries(schema_name)
        for query in foreign_key_queries:
            self.execute_query(query)
        self.logger.info(f"Clés étrangères créées avec succès.")

    def create_unique_constraints_in_postgres(self,schema_name: str = None):
        unique_constraint_queries = self._generate_unique_constraint_queries(schema_name)
        for query in unique_constraint_queries:
            self.execute_query(query)

    def _generate_unique_constraint_queries(self,schema_name: str = None)-> list[str]:
        queries = []
        for constraint in self.unique_constraints:
            table_name = constraint['table_name']
            schema_name, pure_table_name = _split_schema_table(table_name,schema_name)
            column_name = constraint['column_name']
            constraint_name = f"{pure_table_name}_{column_name}_unique"
            query = f'ALTER TABLE "{schema_name}"."{pure_table_name}" ADD CONSTRAINT "{constraint_name}" UNIQUE ("{column_name}");'
            queries.append(query)
        return queries

    def clean_invalid_foreign_keys(self, schema_name:str=None):
        """Nettoie les clés étrangères invalides en les mettant à NULL."""
        for constraint in self.foreign_key_constraints:
            table_name = constraint['table_name']
            column_name = constraint['column_name']
            reference_table = constraint['reference_table']
            reference_column = constraint['reference_column']

            # Extraire le schéma et le nom de la table
            table_schema, table_pure_name = _split_schema_table(table_name, schema_name)
            ref_table_schema, ref_table_pure_name = _split_schema_table(reference_table, schema_name)

            # Construire la requête pour trouver les clés étrangères invalides
            invalid_fk_query = sql.SQL("""
                SELECT {column_name}
                FROM {table_schema}.{table_name}
                WHERE {column_name} IS NOT NULL
                AND {column_name} NOT IN (SELECT {reference_column} FROM {ref_table_schema}.{reference_table});
            """).format(
                column_name=sql.Identifier(column_name),
                table_schema=sql.Identifier(table_schema),
                table_name=sql.Identifier(table_pure_name),
                reference_column=sql.Identifier(reference_column),
                ref_table_schema=sql.Identifier(ref_table_schema),
                reference_table=sql.Identifier(ref_table_pure_name)
            )

            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(invalid_fk_query)
                    invalid_rows = cursor.fetchall()

                if invalid_rows:
                    invalid_ids = [row[0] for row in invalid_rows]

                    # Journaliser les IDs invalides
                    invalid_ids_str = ','.join(str(id) for id in invalid_ids)
                    self.logger.warning(
                        f"Clés étrangères invalides trouvées dans {table_schema}.{table_pure_name}.{column_name}: {invalid_ids_str}")

                    # Préparer la requête de mise à jour en utilisant des paramètres
                    update_query = sql.SQL("""
                        UPDATE {table_schema}.{table_name}
                        SET {column_name} = NULL
                        WHERE {column_name} = ANY(%s);
                    """).format(
                        table_schema=sql.Identifier(table_schema),
                        table_name=sql.Identifier(table_pure_name),
                        column_name=sql.Identifier(column_name)
                    )

                    with self.connection.cursor() as cursor:
                        cursor.execute(update_query, (invalid_ids,))
                    self.connection.commit()
                    self.logger.info(
                        f"Clés étrangères invalides dans {table_schema}.{table_pure_name}.{column_name} mises à NULL.")
                else:
                    self.logger.info(
                        f"Aucune clé étrangère invalide trouvée dans {table_schema}.{table_pure_name}.{column_name}.")
            except Exception as e:
                self.logger.error(
                    f"Erreur lors du nettoyage des clés étrangères invalides dans {table_schema}.{table_pure_name}.{column_name}: {e}")
                self.connection.rollback()

    def perform_constraints(self, schema_name: str = None):
        if None not in (self.primary_key_constraints, self.foreign_key_constraints, self.unique_constraints):
            self.clean_invalid_foreign_keys(schema_name)
            self.create_primary_keys_in_postgres(schema_name)
            self.create_unique_constraints_in_postgres(schema_name)
            self.create_foreign_keys_in_postgres(schema_name)
        else:
            self.logger.error("Les contraintes n'ont pas été chargées.")