from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime
import re


class PlanetDiscovery(BaseModel):
    """
    Modèle de validation pour les découvertes de planètes
    Basé sur la structure du fichier CSV planets_dataset.csv
    """

    Name: str = Field(..., description="Nom de la planète")
    Num_Moons: int = Field(..., ge=0, description="Nombre de satellites naturels")
    Minerals: int = Field(..., ge=0, description="Quantité de minéraux")
    Gravity: float = Field(
        ..., gt=0, description="Gravité (multiple de la gravité terrestre)"
    )
    Sunlight_Hours: float = Field(
        ..., ge=0, le=24, description="Heures d'exposition au soleil par jour"
    )
    Temperature: float = Field(..., description="Température moyenne (en Celsius)")
    Rotation_Time: float = Field(..., gt=0, description="Temps de rotation (en heures)")
    Water_Presence: int = Field(
        ..., ge=0, le=1, description="Présence d'eau (0=non, 1=oui)"
    )
    Colonisable: Optional[int] = Field(
        0, ge=0, le=1, description="Planète colonisable (0=non, 1=oui)"
    )

    @validator("Name")
    def validate_name(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError("Le nom de la planète ne peut pas être vide")
        if len(v) > 100:
            raise ValueError("Le nom de la planète ne peut pas dépasser 100 caractères")
        return v.strip()

    @validator("Water_Presence")
    def validate_water_presence(cls, v):
        if v not in [0, 1]:
            raise ValueError("La présence d'eau doit être 0 (non) ou 1 (oui)")
        return v

    @validator("Colonisable")
    def validate_colonisable(cls, v):
        if v not in [0, 1]:
            raise ValueError("Colonisable doit être 0 (non) ou 1 (oui)")
        return v

class PlanetDiscoveryResponse(BaseModel):
    """Modèle de réponse pour les découvertes de planètes"""

    message: str
    planet_data: PlanetDiscovery
    status: str = "success"
