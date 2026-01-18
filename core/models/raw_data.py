from sqlalchemy import Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import BaseModel


class RawFactoryData(BaseModel):
    """
     Represents raw factory production and consumption records
     for produced and component materials by plant and period.
    """

    __tablename__ = "raw_factory_data"

    year: Mapped[int] = mapped_column(Integer)
    month: Mapped[int] = mapped_column(Integer)
    plant_id: Mapped[str] = mapped_column(String(50))

    produced_material_id: Mapped[str] = mapped_column(String(50))
    produced_material_production_type: Mapped[str] = mapped_column(
        String(50), nullable=True
    )
    produced_material_release_type: Mapped[str] = mapped_column(
        String(50), nullable=True
    )
    produced_material_quantity: Mapped[float] = mapped_column(Float, nullable=True)

    component_material_id: Mapped[str] = mapped_column(String(50))
    component_material_production_type: Mapped[str] = mapped_column(
        String(50), nullable=True
    )
    component_material_release_type: Mapped[str] = mapped_column(
        String(50), nullable=True
    )
    component_material_quantity: Mapped[float] = mapped_column(Float, nullable=True)
