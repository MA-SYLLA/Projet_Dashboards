import os
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
from pathlib import Path
from io import StringIO  # Importer StringIO depuis io

class CSVHandler(FileSystemEventHandler):
    def __init__(self, csv1_path, csv2_path, output_path):
        self.csv1_path = Path(csv1_path)
        self.csv2_path = Path(csv2_path)
        self.output_path = Path(output_path)

    def on_created(self, event):
        # Vérifier si l'événement concerne un fichier CSV
        if event.src_path.endswith('.csv'):
            print(f'Fichier détecté : {event.src_path}')
            self.process_files()

    def process_files(self):
        try:
            # Supprimer les lignes 1 à 10 de first_file.csv
            self.remove_lines(self.csv1_path.parent)

            # Lire les deux fichiers CSV
            read_df1 = pd.read_csv(self.csv1_path, encoding='utf-8')
            read_df2 = pd.read_csv(self.csv2_path, encoding='utf-8')

            # Vérifiez que les fichiers ne sont pas vides
            if read_df1.empty or read_df2.empty:
                raise ValueError("L'un des fichiers CSV est vide.")

            print('Les deux fichiers CSV ont été lus avec succès.')

            # Harmoniser les en-têtes des fichiers CSV
            read_df1.columns = [col.strip() for col in read_df1.columns]
            read_df2.columns = [col.strip() for col in read_df2.columns]

            if set(read_df1.columns) != set(read_df2.columns):
                raise ValueError("Les en-têtes des fichiers CSV ne correspondent pas.")

            # Fusionner les fichiers CSV
            merged_df = pd.concat([read_df1, read_df2])

            # Enregistrer les données fusionnées dans un nouveau fichier CSV
            merged_df.to_csv(self.output_path, index=False, encoding='utf-8')
            print(f'Données fusionnées et enregistrées dans {self.output_path}')
        except FileNotFoundError as e:
            print(f'Erreur de fichier non trouvé: {e}')
        except pd.errors.EmptyDataError as e:
            print(f'Erreur de fichier vide: {e}')
        except ValueError as e:
            print(f'Erreur: {e}')
        except Exception as e:
            print(f'Erreur lors de la fusion des fichiers CSV: {e}')

    def remove_lines(self, watch_to_directory):
        try:
            # Construire le chemin complet du fichier à surveiller
            csv_to_watch = watch_to_directory / 'new_data.csv'
            
            
            # Lire le fichier CSV en tant que texte brut
            with open(csv_to_watch, 'r', encoding='latin-1') as file:
                csv_text = file.read()
                
            # Utiliser pandas pour lire le texte brut   
            remove_df = pd.read_csv(StringIO(csv_text)) 
                
            # Supprimer les lignes 1 à 10
            if len(remove_df) > 10:
                remove_df.drop(range(0, 10), inplace=True)
            
            # Enregistrer le fichier modifié
            remove_df.to_csv(self.csv1_path, index=False, encoding='latin-1')
            print('Lignes 1 à 10 supprimées de first_file.csv')
        except pd.errors.EmptyDataError:
            print(f'Erreur: Le fichier {csv_to_watch} est vide ou mal formaté.')
        except Exception as e:
            print(f'Erreur lors de la suppression des lignes : {e}')

def data_transformation(nom_fichier):
    try:
        # Charger le fichier CSV
        df = pd.read_csv(nom_fichier, encoding='utf-8')

        if df.empty:
            raise ValueError("Le fichier CSV est vide.")

        if 'Date' not in df.columns:
            print("La colonne 'Date' est manquante.")
            return df

        # Convertir la colonne 'Date' en type datetime
        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y', errors='coerce')

        # Créer une nouvelle colonne "catégorie" avec des valeurs spécifiques
        df['Catégorie'] = [''] * len(df)

        # Créer des colonnes pour l'analyse temporelle
        df['Jour'] = df['Date'].dt.to_period('D')
        df['Mois'] = df['Date'].dt.to_period('M')
        df['Année'] = df['Date'].dt.to_period('Y')

        # S'assurer que les montants sont de type numérique
        df['Débit en euros'] = pd.to_numeric(df['Débit en euros'].str.replace(',', '.'), errors='coerce')
        df['Crédit en euros'] = pd.to_numeric(df['Crédit en euros'].str.replace(',', '.'), errors='coerce')

        return df
    except ValueError as e:
        print(f'Erreur: {e}')
        return pd.DataFrame()  # Retourne un DataFrame vide en cas d'erreur
    except Exception as e:
        print(f'Erreur lors de la transformation des données : {e}')
        return pd.DataFrame()  # Retourne un DataFrame vide en cas d'erreur

def main():
    # Chemins des répertoires à vérifier et créer si nécessaire
    directories_to_check = [
        'data/path/to_first',
        'data/path/to_second',
        'data/path/to_output'
    ]

    # Vérification et création des répertoires
    for directory in directories_to_check:
        if not os.path.exists(directory):
            print(f"Le répertoire {directory} n'existe pas. Création du répertoire...")
            try:
                os.makedirs(directory)
                print(f"Répertoire {directory} créé avec succès.")
            except Exception as e:
                print(f"Erreur lors de la création du répertoire {directory}: {e}")
                return

    # Chemins vers les fichiers CSV
    csv1_path = 'data/path/to_first/new_data.csv'  # Changez cela avec le chemin correct
    csv2_path = 'data/path/to_second/data_used.csv'  # Changez cela avec le chemin correct
    output_path = 'data/path/to_output/data_merging.csv'  # Changez cela avec le chemin correct
    directory_to_watch = 'data/path/to_watch_directory'  # Changez cela avec le chemin correct

    # Vérifier que le répertoire à surveiller existe, sinon le créer
    watch_directory = Path(directory_to_watch)
    if not watch_directory.exists():
        print(f'Le répertoire {directory_to_watch} n\'existe pas. Création du répertoire.')
        watch_directory.mkdir(parents=True, exist_ok=True)

    # En-têtes pour les fichiers CSV
    headers = ['Date', 'Libellé', 'Débit en euros', 'Crédit en euros']

    # Vérifier que les chemins des fichiers existent, sinon créer des fichiers vides avec en-têtes
    for csv_path, headers in [(csv1_path, headers), (csv2_path, headers)]:
        if not os.path.exists(csv_path):
            print(f'Erreur: Le fichier {csv_path} n\'existe pas. Création du fichier...')
            try:
                with open(csv_path, 'w') as csv_file:
                    if headers:
                        csv_file.write(','.join(headers) + '\n')
                    print(f'Fichier {csv_path} créé avec succès.')
            except Exception as e:
                print(f'Erreur lors de la création du fichier {csv_path}: {e}')
        else:
            print(f'Succès: Le fichier {csv_path} existe.')

    # Terminer l'exécution si un ou plusieurs fichiers n'existent pas
    if not os.path.exists(csv1_path) or not os.path.exists(csv2_path):
        return

    # Créer le gestionnaire d'événements
    event_handler = CSVHandler(csv1_path, csv2_path, output_path)
    observer = Observer()
    observer.schedule(event_handler, path=str(watch_directory), recursive=False)
    observer.start()

    print(f'Surveillance du répertoire : {directory_to_watch}')

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print('Arrêt de la surveillance.')

    observer.join()

if __name__ == "__main__":
    main()