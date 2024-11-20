import json
from git import Repo
from modules.mylogging import MyLogger
import os

def read_json_file(file_path: str) -> dict:
    """Lire un fichier JSON et retourner son contenu."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def clone_and_extract_repo(repo_url: str, local_dir: str):
    """
    Clone un repository Git et extrait les fichiers dans le répertoire spécifié.
    """
    logger = MyLogger('clone_and_extract_repo', with_file=False)
    try:
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        logger.info(f"Clonage du repository depuis {repo_url} dans {local_dir}...")
        Repo.clone_from(repo_url, local_dir)
        logger.info(f"Clonage réussi. Les fichiers sont extraits dans {local_dir}.")
    except Exception as e:
        logger.error(f"Une erreur s'est produite lors du clonage du repository : {e}")
