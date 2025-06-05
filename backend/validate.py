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
    Colonisable: int = Field(
        ..., ge=0, le=1, description="Planète colonisable (0=non, 1=oui)"
    )

    timestamp_reception: Optional[str] = Field(
        default=None, description="Timestamp de réception"
    )

    @validator("name")
    def validate_name(cls, v):
        if not v or len(v.strip()) == 0:
            raise ValueError("Le nom de la planète ne peut pas être vide")
        if len(v) > 100:
            raise ValueError("Le nom de la planète ne peut pas dépasser 100 caractères")
        return v.strip()

    @validator("water_presence")
    def validate_water_presence(cls, v):
        if v not in [0, 1]:
            raise ValueError("La présence d'eau doit être 0 (non) ou 1 (oui)")
        return v

    @validator("colonisable")
    def validate_colonisable(cls, v):
        if v not in [0, 1]:
            raise ValueError("Colonisable doit être 0 (non) ou 1 (oui)")
        return v

    def model_post_init(self, __context):
        """Définit automatiquement le timestamp si non fourni"""
        if self.timestamp_reception is None:
            self.timestamp_reception = datetime.now().isoformat()


class PlanetDiscoveryResponse(BaseModel):
    """Modèle de réponse pour les découvertes de planètes"""

    message: str
    planet_data: PlanetDiscovery
    status: str = "success"
