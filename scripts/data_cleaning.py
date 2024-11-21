import pandas as pd
import numpy as np
import os

# Charger les données brutes
def load_data():
    try:
        ltm_data = pd.read_csv('/Users/lisayaici/Desktop/Projet_data_integration/data/raw/LTM_Data_2022_8_1.csv', sep=';', on_bad_lines='skip')
        methods_data = pd.read_csv('/Users/lisayaici/Desktop/Projet_data_integration/data/raw/Methods_2022_8_1.csv', sep=';', on_bad_lines='skip')
        site_info_data = pd.read_csv('/Users/lisayaici/Desktop/Projet_data_integration/data/raw/Site_Information_2022_8_1.csv', sep=';', on_bad_lines='skip')
        print("Données chargées avec succès.")
        return ltm_data, methods_data, site_info_data
    except FileNotFoundError as fnf_error:
        print(f"Fichier non trouvé : {fnf_error}")
        return None, None, None
    except Exception as e:
        print(f"Erreur lors du chargement des données : {e}")
        return None, None, None

# Fonction pour afficher des statistiques de base
def display_overview(df, df_name):
    print(f"\n{df_name} Overview:")
    print(df.info())
    print(f"Valeurs manquantes dans {df_name} :\n", df.isnull().sum())  
    print(f"Statistiques de base pour {df_name} :\n", df.describe())  
    print("\n")

# Nettoyage des données LTM
def clean_ltm_data(ltm_data):
    try:
        if 'DATE_SMP' in ltm_data.columns:
            ltm_data['DATE_SMP'] = pd.to_datetime(ltm_data['DATE_SMP'], errors='coerce')
        else:
            print("Colonne 'DATE_SMP' manquante dans les données LTM.")

        if 'TIME_SMP' in ltm_data.columns:
            ltm_data['TIME_SMP'] = pd.to_datetime(ltm_data['TIME_SMP'], errors='coerce')
        else:
            print("Colonne 'TIME_SMP' manquante dans les données LTM.")
        
        # Imputer les valeurs manquantes numériques
        ltm_data['SAMPLE_DEPTH'].fillna(ltm_data['SAMPLE_DEPTH'].median(), inplace=True)
        ltm_data['CHL_A_UG_L'].fillna(ltm_data['CHL_A_UG_L'].mean(), inplace=True)
        ltm_data['DOC_MG_L'].fillna(ltm_data['DOC_MG_L'].median(), inplace=True)

        # Supprimer les lignes avec trop de valeurs manquantes
        ltm_data.dropna(subset=['SAMPLE_DEPTH', 'TIME_SMP', 'CHL_A_UG_L'], inplace=True)

        # Conversion des colonnes numériques au format numérique (si ce n'est déjà fait)
        if 'ANC_UEQ_L' in ltm_data.columns:
            ltm_data['ANC_UEQ_L'] = pd.to_numeric(ltm_data['ANC_UEQ_L'], errors='coerce')
        if 'CA_UEQ_L' in ltm_data.columns:
            ltm_data['CA_UEQ_L'] = pd.to_numeric(ltm_data['CA_UEQ_L'], errors='coerce')
        if 'COND_UM_CM' in ltm_data.columns:
            ltm_data['COND_UM_CM'] = pd.to_numeric(ltm_data['COND_UM_CM'], errors='coerce')

        print("Nettoyage des données LTM terminé.")
        return ltm_data
    except Exception as e:
        print(f"Erreur lors du nettoyage des données LTM : {e}")
        return ltm_data

# Nettoyage des données Methods
def clean_methods_data(methods_data):
    try:
        if 'PARAMETER' in methods_data.columns:
            methods_data['PARAMETER'].fillna('UNKNOWN', inplace=True)
        else:
            print("Colonne 'PARAMETER' manquante dans les données Methods.")
        
        if 'END_YEAR' in methods_data.columns:
            methods_data['END_YEAR'].fillna(2022, inplace=True)

        print("Nettoyage des données Methods terminé.")
        return methods_data
    except Exception as e:
        print(f"Erreur lors du nettoyage des données Methods : {e}")
        return methods_data

# Nettoyage des données Site Information
def clean_site_info_data(site_info_data):
    try:
        if 'LATDD_CENTROID' in site_info_data.columns:
            site_info_data['LATDD_CENTROID'].fillna(site_info_data['LATDD_CENTROID'].mean(), inplace=True)
        else:
            print("Colonne 'LATDD_CENTROID' manquante dans les données Site Information.")

        if 'LONDD_CENTROID' in site_info_data.columns:
            site_info_data['LONDD_CENTROID'].fillna(site_info_data['LONDD_CENTROID'].mean(), inplace=True)
        else:
            print("Colonne 'LONDD_CENTROID' manquante dans les données Site Information.")

        # Imputer les valeurs manquantes dans 'LAKE_DEPTH_MAX' et 'LAKE_DEPTH_MEAN' avec la moyenne
        site_info_data['LAKE_DEPTH_MAX'].fillna(site_info_data['LAKE_DEPTH_MAX'].mean(), inplace=True)
        site_info_data['LAKE_DEPTH_MEAN'].fillna(site_info_data['LAKE_DEPTH_MEAN'].mean(), inplace=True)

        # Supprimer les colonnes qui sont entièrement vides
        site_info_data.drop(columns=['LAKE_RET'], inplace=True, errors='ignore')

        print("Nettoyage des données Site Information terminé.")
        return site_info_data
    except Exception as e:
        print(f"Erreur lors du nettoyage des données Site Information : {e}")
        return site_info_data

# Sauvegarde des données nettoyées dans le répertoire 'data/processed'
def save_cleaned_data(ltm_data, methods_data, site_info_data):
    try:
        # Définir le répertoire de sauvegarde
        output_dir = 'data/processed'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Sauvegarder les données nettoyées
        ltm_data.to_csv(os.path.join(output_dir, 'LTM_Data_cleaned.csv'), index=False)
        methods_data.to_csv(os.path.join(output_dir, 'Methods_Data_cleaned.csv'), index=False)
        site_info_data.to_csv(os.path.join(output_dir, 'Site_Info_Data_cleaned.csv'), index=False)
        print("\nNettoyage terminé et fichiers sauvegardés dans 'data/processed'.")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde des fichiers : {e}")

# Principal
def main():
    # Charger les données
    ltm_data, methods_data, site_info_data = load_data()

    if ltm_data is not None and methods_data is not None and site_info_data is not None:
        # Afficher l'aperçu des données
        display_overview(ltm_data, "LTM Data")
        display_overview(methods_data, "Methods Data")
        display_overview(site_info_data, "Site Information Data")

        # Nettoyer les données
        ltm_data = clean_ltm_data(ltm_data)
        methods_data = clean_methods_data(methods_data)
        site_info_data = clean_site_info_data(site_info_data)

        # Sauvegarder les données nettoyées dans le répertoire 'data/processed'
        save_cleaned_data(ltm_data, methods_data, site_info_data)
    else:
        print("Impossible de continuer sans charger les données correctement.")

# Exécution du script principal
if __name__ == "__main__":
    main()
