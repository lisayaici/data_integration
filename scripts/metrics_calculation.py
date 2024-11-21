import pandas as pd

# Charger les données avec le séparateur ';'
def load_data():
    try:
        ltm_data = pd.read_csv('data/raw/LTM_Data_2022_8_1.csv', sep=';', on_bad_lines='skip')
        methods_data = pd.read_csv('data/raw/Methods_2022_8_1.csv', sep=';', on_bad_lines='skip')
        site_info_data = pd.read_csv('data/raw/Site_Information_2022_8_1.csv', sep=';', on_bad_lines='skip')
        print("Données chargées avec succès.")
        return ltm_data, methods_data, site_info_data
    except FileNotFoundError as fnf_error:
        print(f"Fichier non trouvé : {fnf_error}")
        return None, None, None
    except Exception as e:
        print(f"Erreur lors du chargement des données : {e}")
        return None, None, None

# Fonction pour calculer les métriques
def calculate_metrics(ltm_data):
    try:
        # Calcul des moyennes, médianes, min, max pour les colonnes pertinentes
        metrics = {}
        metrics['mean_sample_depth'] = ltm_data['SAMPLE_DEPTH'].mean()
        metrics['median_chl_a'] = ltm_data['CHL_A_UG_L'].median()
        metrics['mean_doc'] = ltm_data['DOC_MG_L'].mean()
        metrics['std_ph'] = ltm_data['PH_LAB'].std()
        metrics['count_missing_sample_depth'] = ltm_data['SAMPLE_DEPTH'].isnull().sum()

        # D'autres métriques intéressantes
        metrics['max_anc'] = ltm_data['ANC_UEQ_L'].max()
        metrics['min_cond'] = ltm_data['COND_UM_CM'].min()

        print("Métriques calculées avec succès.")
        return metrics
    except Exception as e:
        print(f"Erreur lors du calcul des métriques : {e}")
        return {}

# Fonction pour sauvegarder les résultats des métriques
def save_metrics(metrics):
    try:
        metrics_df = pd.DataFrame([metrics])
        metrics_df.to_csv('data/processed/data_metrics.csv', index=False)
        print("Métriques sauvegardées avec succès.")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde des métriques : {e}")

# Principal
def main():
    # Charger les données
    ltm_data, methods_data, site_info_data = load_data()

    if ltm_data is not None:
        # Calculer les métriques
        metrics = calculate_metrics(ltm_data)

        if metrics:
            # Sauvegarder les métriques dans un fichier CSV
            save_metrics(metrics)
    else:
        print("Impossible de continuer sans charger les données correctement.")

# Exécution du script principal
if __name__ == "__main__":
    main()
