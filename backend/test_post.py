import pandas as pd
import requests
import json

# Lire la première ligne du CSV
df = pd.read_csv("../planets_dataset.csv")
first_row = df.iloc[0].to_dict()

# Assurer que tous les champs sont convertis en types JSON-valide (ex. : pas de NaN)
for key, value in first_row.items():
    if pd.isna(value):
        first_row[key] = None
    elif isinstance(value, (int, float)):
        # Ex. : convertir 1.0 en 1 si possible
        first_row[key] = int(value) if value == int(value) else value
    else:
        first_row[key] = str(value)

# Envoyer la requête POST
url = "http://localhost:5000/discoveries"
headers = {"Content-Type": "application/json"}
response = requests.post(url, data=json.dumps(first_row), headers=headers)

# Affichage de la réponse
print("Statut HTTP:", response.status_code)
print("Réponse:", response.json())
