Voici le fichier README que vous avez demandé :

---

# README

## Introduction

Ce script Python est conçu pour automatiser le processus de création d'un schéma de base de données PostgreSQL à partir de fichiers CSV compressés. Il lit les fichiers de données, génère les schémas de tables appropriés, crée les tables dans la base de données, charge les données, et crée les clés primaires et étrangères en fonction des contraintes définies. Le code est structuré de manière orientée objet pour une meilleure maintenabilité et extensibilité.

## Fonctionnalités

- Lecture et prétraitement des fichiers CSV.gz.
- Désérialisation des objets PHP sérialisés en JSON.
- Détermination automatique des types de données PostgreSQL appropriés pour chaque colonne.
- Génération dynamique des requêtes SQL pour créer les tables.
- Chargement des données dans PostgreSQL.
- Création des clés primaires et étrangères selon les contraintes définies.
- Utilisation du module `logging` pour un suivi détaillé du processus.

## Prérequis

- Python 3.7 ou supérieur
- PostgreSQL installé et accessible
- Accès à une base de données PostgreSQL avec les droits nécessaires pour créer des schémas et des tables
- Les fichiers de données CSV.gz à traiter
- Les dépendances Python suivantes :

  - pandas
  - psycopg2
  - GitPython
  - python-dotenv
  - numpy
  - phpserialize

## Installation des dépendances

Il est recommandé d'utiliser un environnement virtuel pour installer les dépendances. Voici comment installer les dépendances requises :

```bash
# Créer un environnement virtuel
python3 -m venv venv

# Activer l'environnement virtuel
# Sur Windows :
venv\Scripts\activate
# Sur macOS/Linux :
source venv/bin/activate

# Mettre à jour pip
pip install --upgrade pip

# Installer les dépendances
pip install pandas psycopg2-binary GitPython python-dotenv numpy phpserialize
```

## Configuration

### Variables d'environnement

Le script utilise le module `dotenv` pour charger les variables d'environnement nécessaires à la connexion à PostgreSQL. Créez un fichier `.env` dans le même répertoire que le script et ajoutez-y les variables suivantes :

```
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=nom_de_votre_base
POSTGRES_USERNAME=votre_nom_d_utilisateur
POSTGRES_PASSWORD=votre_mot_de_passe
```

Assurez-vous de remplacer les valeurs par celles correspondant à votre configuration PostgreSQL.

### Fichiers de données

Les fichiers de données CSV.gz doivent être placés dans un répertoire spécifique pour que le script puisse les traiter. Par défaut, le script recherche les fichiers dans le répertoire `Owa_Data_Site_MDS/Data`. Si vous avez un autre emplacement, assurez-vous de mettre à jour le chemin dans le script ou de placer vos fichiers au bon endroit.

### Clonage du dépôt Git (optionnel)

Le script inclut une fonction pour cloner un dépôt Git contenant les données. Cette partie est commentée par défaut. Si vous souhaitez cloner automatiquement le dépôt, décommentez la ligne correspondante dans la fonction `main()` :

```python
# clone_and_extract_repo(repo_url, local_dir)
```

Assurez-vous que vous avez les droits nécessaires pour cloner le dépôt et que l'URL du dépôt est correcte.

## Utilisation

1. **Configurer les variables d'environnement** : Comme indiqué dans la section précédente, créez un fichier `.env` avec les paramètres de connexion à PostgreSQL.

2. **Placer les fichiers de données** : Assurez-vous que les fichiers CSV.gz sont placés dans le répertoire `Owa_Data_Site_MDS/Data`.

3. **Exécuter le script** :

   ```bash
   python votre_script.py
   ```

   Remplacez `votre_script.py` par le nom du fichier contenant le code.

4. **Surveiller les logs** : Le script utilise le module `logging` pour afficher les informations sur l'exécution. Les messages incluent les étapes de création des schémas, des tables, le chargement des données, et la création des contraintes.

## Personnalisation

### Modification des contraintes

Les contraintes de clés primaires et étrangères sont définies dans la méthode `load_constraints()` de la classe `PostgresConnection`. Si vous souhaitez ajouter, modifier ou supprimer des contraintes, vous pouvez ajuster les listes `primary_key_constraints` et `foreign_key_constraints` dans cette méthode.

### Gestion des fichiers de données

Si vos fichiers de données sont dans un format différent ou nécessitent un prétraitement spécifique, vous pouvez modifier la méthode `preprocess_data()` de la classe `ParseDFTypes` pour adapter le prétraitement à vos besoins.

## Structure du code

- **ParseDFTypes** : Classe responsable de la détermination des types de données appropriés pour PostgreSQL et du prétraitement des données, y compris la désérialisation des objets PHP.

- **PostgresConnection** : Classe pour gérer la connexion à PostgreSQL et effectuer des opérations telles que la création de schémas, de tables, le chargement de données, et la création de contraintes.

- **clone_and_extract_repo** : Fonction utilitaire pour cloner un dépôt Git. Optionnelle et commentée par défaut.

- **main** : Fonction principale qui orchestre l'exécution du script.

## Dépendances

Assurez-vous d'installer les versions appropriées des packages Python :

- pandas
- psycopg2-binary
- GitPython
- python-dotenv
- numpy
- phpserialize

Vous pouvez installer toutes les dépendances en une seule commande :

```bash
pip install -r requirements.txt
```

Si vous utilisez un fichier `requirements.txt` contenant les lignes suivantes :

```
pandas
psycopg2-binary
GitPython
python-dotenv
numpy
phpserialize
```

## Notes

- **Sécurité** : Veillez à protéger vos informations d'identification de la base de données. Ne les partagez pas et ne les ajoutez pas à un dépôt public.

- **Performance** : Pour de grands volumes de données, le processus peut prendre du temps. Assurez-vous que votre machine dispose des ressources nécessaires.

- **Compatibilité** : Le script est compatible avec Python 3.7 et supérieur.

- **Support** : Si vous rencontrez des problèmes, vérifiez les messages d'erreur dans les logs et assurez-vous que toutes les dépendances sont correctement installées.

## Auteurs

- **Votre Nom** : Développeur du script.

## Licence

Ce projet est sous licence MIT - voir le fichier [LICENSE](LICENSE) pour plus de détails.

---

**Note** : Si vous souhaitez inclure un fichier `LICENSE`, assurez-vous de le créer et de le personnaliser selon vos besoins.

---
