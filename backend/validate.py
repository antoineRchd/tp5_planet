# Champs attendus
dataset_required_fields = [
    "Name", "Num_Moons", "Minerals", "Gravity", "Sunlight_Hours",
    "Temperature", "Rotation_Time", "Water_Presence", "Colonisable"
]

# Valeurs valides après transformation
valid_binary_values = ["oui", "non"]

def validate_dataset_planet_data(data):
    """
    Valide les données d'une planète du dataset CSV (avec Water_Presence et Colonisable en 0/1).
    """
    for field in dataset_required_fields:
        if field not in data:
            return False, f"Champ manquant : {field}"
        if data[field] is None or str(data[field]).strip() == "":
            return False, f"Le champ '{field}' ne peut pas être vide"

    try:
        # Num_Moons : entier >= 0
        num_moons = int(data["Num_Moons"])
        if num_moons < 0:
            return False, "Le nombre de lunes doit être positif"

        # Valeurs numériques
        float(data["Gravity"])
        float(data["Sunlight_Hours"])
        float(data["Temperature"])
        float(data["Rotation_Time"])

        # Water_Presence : 0/1 → "non"/"oui"
        wp_raw = str(data["Water_Presence"]).strip()
        water_val = "oui" if wp_raw == "1" else "non"
        if water_val not in valid_binary_values:
            return (
                False,
                f"Valeur invalide pour Water_Presence : {wp_raw}. Attendu : 0 ou 1"
            )

        # Colonisable : 0/1 → "non"/"oui"
        col_raw = str(data["Colonisable"]).strip()
        colonisable_val = "oui" if col_raw == "1" else "non"
        if colonisable_val not in valid_binary_values:
            return (
                False,
                f"Valeur invalide pour Colonisable : {col_raw}. Attendu : 0 ou 1"
            )

    except ValueError as e:
        return False, f"Erreur de type numérique : {str(e)}"
    except Exception as e:
        return False, f"Erreur : {str(e)}"

    return True, "Données valides"
