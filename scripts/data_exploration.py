import pandas as pd

# Analyse du fichier LTM_Data_2022_8_1.csv
try:
    ltm_data = pd.read_csv('data/raw/LTM_Data_2022_8_1.csv', delimiter=';')
    print("LTM Data Overview:")
    print(ltm_data.info())
    print(ltm_data.head())
    print("\nValeurs manquantes dans LTM Data:")
    print(ltm_data.isna().sum())
except Exception as e:
    print(f"Erreur lors du chargement du fichier LTM_Data_2022_8_1.csv: {e}")

try:
    methods_data = pd.read_csv('data/raw/Methods_2022_8_1.csv', delimiter=';', error_bad_lines=False)
    print("\nMethods Data Overview:")
    print(methods_data.info())
    print(methods_data.head())
    print("\nValeurs manquantes dans Methods Data:")
    print(methods_data.isna().sum())
except Exception as e:
    print(f"Erreur lors du chargement du fichier Methods_2022_8_1.csv: {e}")

try:
    site_info_data = pd.read_csv('data/raw/Site_Information_2022_8_1.csv', delimiter=';')
    print("\nSite Information Data Overview:")
    print(site_info_data.info())
    print(site_info_data.head())
    print("\nValeurs manquantes dans Site Information Data:")
    print(site_info_data.isna().sum())
except Exception as e:
    print(f"Erreur lors du chargement du fichier Site_Information_2022_8_1.csv: {e}")
