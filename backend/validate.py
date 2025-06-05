required_fields = [
    "Name","Num_Moons","Minerals","Gravity","Sunlight_Hours","Temperature","Rotation_Time","Water_Presence","Colonisable"
]

def validate_planet_data(data):
    for field in required_fields:
        if field not in data:
            return False, f"Champ manquant : {field}"
    return True, "Valid"
