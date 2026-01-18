from sqlalchemy import Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from core.models.base import BaseModel


class BomReport(BaseModel):
    """
    Stores aggregated bill of materials data for finished, produced, and component materials by plant and year.
    """

    __tablename__ = "bom_reports"

    plant: Mapped[str] = mapped_column(String(50))
    year: Mapped[int] = mapped_column(Integer)

    fin_material_id: Mapped[str] = mapped_column(String(50))
    fin_material_release_type: Mapped[str] = mapped_column(String(50), nullable=True)
    fin_material_production_type: Mapped[str] = mapped_column(String(50), nullable=True)
    fin_production_quantity: Mapped[float] = mapped_column(Float, nullable=True)

    prod_material_id: Mapped[str] = mapped_column(String(50))
    prod_material_release_type: Mapped[str] = mapped_column(String(50), nullable=True)
    prod_material_production_type: Mapped[str] = mapped_column(
        String(50), nullable=True
    )
    prod_material_production_quantity: Mapped[float] = mapped_column(
        Float, nullable=True
    )

    component_id: Mapped[str] = mapped_column(String(50))
    component_material_release_type: Mapped[str] = mapped_column(
        String(50), nullable=True
    )
    component_material_production_type: Mapped[str] = mapped_column(
        String(50), nullable=True
    )
    component_consumption_quantity: Mapped[float] = mapped_column(Float, nullable=True)
