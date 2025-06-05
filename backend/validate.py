# Modèle de découverte de planète selon l'énoncé
required_fields = [
    "id",  # Identifiant unique de la découverte
    "nom",  # Nom de la planète
    "decouvreur",  # Nom du scientifique ou équipe
    "date_decouverte",  # Date de la découverte
    "masse",  # Masse (en multiples de la masse terrestre)
    "rayon",  # Rayon (en multiples du rayon terrestre)
    "distance",  # Distance par rapport à la Terre (en années-lumière)
    "type",  # Type de planète (géante gazeuse, terrestre, naine)
    "statut",  # Statut (confirmée, non confirmée, potentielle)
    "atmosphere",  # Composition atmosphérique
    "temperature_moyenne",  # Température moyenne (en Celsius)
    "periode_orbitale",  # Durée de l'orbite (en jours terrestres)
    "nombre_satellites",  # Nombre de satellites naturels connus
    "presence_eau",  # Présence d'eau liquide (oui/non)
]

# Types valides pour validation
valid_planet_types = [
    "géante gazeuse",
    "terrestre",
    "naine",
    "super-terre",
    "neptune",
    "jupiter chaud",
]
valid_statuts = ["confirmée", "non confirmée", "potentielle"]
valid_presence_eau = ["oui", "non", "inconnue"]


def validate_planet_data(data):
    """
    Valide les données de découverte de planète selon le modèle spécifié
    """
    # Vérification des champs requis
    for field in required_fields:
        if field not in data:
            return False, f"Champ manquant : {field}"

        # Vérification que les champs ne sont pas vides
        if data[field] is None or str(data[field]).strip() == "":
            return False, f"Le champ '{field}' ne peut pas être vide"

    # Validations spécifiques par type de champ
    try:
        # Validation de l'ID (doit être unique et non vide)
        if not str(data["id"]).strip():
            return False, "L'ID ne peut pas être vide"

        # Validation des valeurs numériques
        masse = float(data["masse"])
        if masse <= 0:
            return False, "La masse doit être positive"

        rayon = float(data["rayon"])
        if rayon <= 0:
            return False, "Le rayon doit être positif"

        distance = float(data["distance"])
        if distance < 0:
            return False, "La distance ne peut pas être négative"

        temperature = float(data["temperature_moyenne"])

        periode = float(data["periode_orbitale"])
        if periode <= 0:
            return False, "La période orbitale doit être positive"

        satellites = int(data["nombre_satellites"])
        if satellites < 0:
            return False, "Le nombre de satellites ne peut pas être négatif"

        # Validation des champs avec valeurs prédéfinies
        if data["type"].lower() not in [t.lower() for t in valid_planet_types]:
            return (
                False,
                f"Type de planète invalide. Types valides : {', '.join(valid_planet_types)}",
            )

        if data["statut"].lower() not in [s.lower() for s in valid_statuts]:
            return (
                False,
                f"Statut invalide. Statuts valides : {', '.join(valid_statuts)}",
            )

        if data["presence_eau"].lower() not in [p.lower() for p in valid_presence_eau]:
            return (
                False,
                f"Présence d'eau invalide. Valeurs valides : {', '.join(valid_presence_eau)}",
            )

    except ValueError as e:
        return False, f"Erreur de validation des données numériques : {str(e)}"
    except Exception as e:
        return False, f"Erreur de validation : {str(e)}"

    return True, "Données valides"


def validate_dataset_planet_data(data):
    """
    Fonction de validation pour les données du dataset existant
    (pour compatibilité avec les données du CSV)
    """
    dataset_required_fields = [
        "Name",
        "Num_Moons",
        "Minerals",
        "Gravity",
        "Sunlight_Hours",
        "Temperature",
        "Rotation_Time",
        "Water_Presence",
        "Colonisable",
    ]

    for field in dataset_required_fields:
        if field not in data:
            return False, f"Champ manquant : {field}"
    return True, "Valid"
