import os
import pandas as pd
from time import sleep
from dotenv import load_dotenv
from modules.db_connectors import PostgresConnection
from modules.dataframe import ParseDFTypes
from modules.utils import read_json_file, clone_and_extract_repo
from modules.mylogging import MyLogger


standard_logger = MyLogger("standard_logger", with_file=False)
# Chargement des variables d'environnement
load_dotenv()
connection_params = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DATABASE"),
    "user": os.getenv("POSTGRES_USERNAME"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

repo_url = "https://vcs.management-datascience.org/a.alfocea/Owa_Data_Site_MDS.git"
data_dir = "data"
local_dir = os.path.join(data_dir, "Owa_Data_Site_MDS")
constraints_file = os.path.join(data_dir, "constraints.json")
family_file = os.path.join(data_dir, "family.json")
csv_directory = os.path.join(local_dir, "Data")

def create_schema():
    schema_name = "test"

    # Clone du repository (décommenter si nécessaire)
    # clone_and_extract_repo(repo_url, local_dir)

    if not os.path.exists(csv_directory):
        standard_logger.error(f"Le répertoire {csv_directory} n'existe pas.")
        return

    parser = ParseDFTypes()
    pg_manager = PostgresConnection(
        connection_params=connection_params,
        parser=parser,
        schema_name=schema_name,
        constraints_file=constraints_file
    )
    pg_manager.init_schema(schema_name)

    # Parcours des fichiers CSV.gz dans le répertoire des données
    for csv_file in os.listdir(csv_directory):
        if csv_file.endswith(".csv.gz"):
            filename = csv_file.split(".")[0]
            table_name = filename  # Utilise le nom du fichier comme nom de table
            csv_gz_path = os.path.join(csv_directory, csv_file)

            # Lecture du fichier CSV.gz
            try:
                df = pd.read_csv(csv_gz_path, compression='gzip', low_memory=False)
            except Exception as e:
                standard_logger.error(f"Erreur lors de la lecture du fichier {csv_gz_path} : {e}")
                continue

            # Génération et création de la table
            create_table_query = pg_manager.generate_table_schema(df, table_name)
            pg_manager.create_table_in_postgres(create_table_query, with_drop=False)

            # Exportation du DataFrame en CSV temporaire pour chargement dans PostgreSQL
            temp_csv_path = os.path.join(csv_directory, filename + ".csv")

            # Prétraitement des données
            df = parser.preprocess_data(df)
            df.to_csv(temp_csv_path, index=False)

            # Chargement des données dans PostgreSQL
            pg_manager.load_csv_to_postgres(temp_csv_path, table_name)

            # Suppression du fichier CSV temporaire
            os.remove(temp_csv_path)

            sleep(.5)

    # Création des contraintes
    pg_manager.perform_constraints(schema_name)
    # Fermeture de la connexion à PostgreSQL
    pg_manager.close_postgres_connection()



def make_dictionary():

    if not os.path.exists(csv_directory):
        standard_logger.error(f"Le répertoire {csv_directory} n'existe pas.")
        return

    parser = ParseDFTypes()
    constraints = read_json_file(constraints_file)
    family_dict = read_json_file(family_file)
    def constraints_df(constraints: dict[str,list[dict]]) -> pd.DataFrame:
        df = pd.DataFrame(columns=["Custom_id","Table", "Nom de la colonne", "Type de contrainte", "Table de référence", "Colonne de référence"])
        constraint_type_map = {
            "primary_keys": "Clé primaire",
            "foreign_keys": "Clé étrangère",
            "unique_keys": "Clé unique"
        }

        for constraint_type, constraint_data in constraints.items():
            constraint_type = constraint_type_map.get(constraint_type, "Inconnu")
            for constraint in constraint_data:
                table = constraint.get("table_name", "").split(".")[-1]
                column = constraint.get("column_name", "")
                ref_table = constraint.get("reference_table", "").split(".")[-1]
                ref_column = constraint.get("reference_column", "")
                df = pd.concat([df, pd.DataFrame([{
                    "Custom_id": f"{table}_{column}",
                    "Table": table,
                    "Nom de la colonne": column,
                    "Type de contrainte": constraint_type,
                    "Table de référence": ref_table,
                    "Colonne de référence": ref_column
                }])], ignore_index=True)
        return df

    constraints_df = constraints_df(constraints)

    def get_family(family_dict: dict[str,list[str]], table_name)-> str:
        for family, tables in family_dict.items():
            if table_name in tables:
                return family
        return "Inconnu"

    # Parcours des fichiers CSV.gz dans le répertoire des données
    data = []
    for csv_file in os.listdir(csv_directory):
        if csv_file.endswith(".csv.gz"):
            filename = csv_file.split(".")[0]
            table_name = filename  # Utilise le nom du fichier comme nom de table
            csv_gz_path = os.path.join(csv_directory, csv_file)
            family =  get_family(family_dict,table_name)
            try:
                df = pd.read_csv(csv_gz_path, compression='gzip', low_memory=False)
                standard_logger.info(f"La table {table_name} est entrain d'être analysée.")
            except Exception as e:
                standard_logger.error(f"Erreur lors de la lecture du fichier {csv_gz_path} : {e}")
                continue

            for col in df.columns:
                row = {}
                row["Table"] = table_name
                row["Nom de la colonne"] = col
                data_type = parser.get_postgres_type(df[col].dtype, df[col])
                row["Type de données"] = data_type
                row["Custom_id"] = f"{table_name}_{col}"
                n_samples = 3 if data_type not in ["JSONB", "TEXT"] else 1
                row["Echantillon"] =  ", ".join(df[col].dropna().astype(str).unique()[:n_samples])
                row["Famille"] = family
                data.append(row)
    dictionary_df = pd.DataFrame(data)
    output = pd.merge(dictionary_df, constraints_df.drop(columns=["Table","Nom de la colonne"]), on="Custom_id", how="left")
    output = output[["Table","Famille", "Nom de la colonne", "Type de données", "Type de contrainte", "Table de référence", "Colonne de référence", "Echantillon"]]
    output.to_excel("data/dictionary.xlsx", index=False, header=True)
    standard_logger.info("Le dictionnaire de données a été créé avec succès.")

def add_description():
    def merge_descriptions(dictionary: pd.DataFrame, descriptions: pd.DataFrame) -> pd.DataFrame:
        return pd.merge(dictionary, descriptions, on=["Custom_id"], how="left")

    standard_logger.info("Ajout des descriptions au dictionnaire de données...")
    standard_logger.info("Lecture des fichiers...")
    dictionary = pd.read_excel("data/dictionary.xlsx")
    descriptions = pd.read_excel("data/descriptions.xlsx").rename(columns={"Nom de la Colonne":"Nom de la colonne" })
    dictionary["Custom_id"] = dictionary["Table"] + "_" + dictionary["Nom de la colonne"]
    descriptions["Nom de la colonne"] = descriptions.apply(lambda x: x[["Nom de la colonne"]].str.split(",") , axis=1)

    descriptions = descriptions.explode("Nom de la colonne", ignore_index=True)
    # descriptions.query("Table == 'owa_session'").iloc[27:, :][["Nom de la Colonne", "Description"]])
    descriptions["Custom_id"] = descriptions["Table"] + "_" + descriptions["Nom de la colonne"]
    descriptions = descriptions[["Custom_id", "Description"]]
    output = merge_descriptions(dictionary, descriptions)
    output.drop(columns=["Custom_id"], inplace=True)
    output.to_excel("data/dictionary_with_descriptions.xlsx", index=False, header=True)
    standard_logger.info("Les descriptions ont été ajoutées avec succès.")




if __name__ == "__main__":

    # make_dictionary()
    # add_description()
    create_schema()