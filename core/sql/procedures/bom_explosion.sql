-- Recursively constructs a bill-of-materials hierarchy, linking finished products to their
-- components across all levels for each plant and year.

WITH RECURSIVE bom_hierarchy AS (

    SELECT
        r.plant_id AS plant,
        r.year,

        r.produced_material_id AS fin_material_id,
        r.produced_material_release_type AS fin_material_release_type,
        r.produced_material_production_type AS fin_material_production_type,
        r.produced_material_quantity AS fin_production_quantity,

        r.produced_material_id AS prod_material_id,
        r.produced_material_release_type AS prod_material_release_type,
        r.produced_material_production_type AS prod_material_production_type,
        r.produced_material_quantity AS prod_material_production_quantity,

        r.component_material_id AS component_id,
        r.component_material_release_type,
        r.component_material_production_type,
        r.component_material_quantity AS component_consumption_quantity,

        1 AS level
    FROM
        raw_factory_data r
    WHERE
        r.produced_material_release_type = 'FIN'

    UNION ALL

    SELECT
        child.plant_id,
        child.year,

        parent.fin_material_id,
        parent.fin_material_release_type,
        parent.fin_material_production_type,
        parent.fin_production_quantity,

        child.produced_material_id,
        child.produced_material_release_type,
        child.produced_material_production_type,
        child.produced_material_quantity,

        child.component_material_id,
        child.component_material_release_type,
        child.component_material_production_type,
        child.component_material_quantity,

        parent.level + 1
    FROM
        raw_factory_data child
    JOIN
        bom_hierarchy parent
        ON child.produced_material_id = parent.component_id
        AND child.plant_id = parent.plant
        AND child.year = parent.year
)
SELECT
    plant,
    fin_material_id,
    fin_material_release_type,
    fin_material_production_type,
    fin_production_quantity,
    prod_material_id,
    prod_material_release_type,
    prod_material_production_type,
    prod_material_production_quantity,
    component_id,
    component_material_release_type,
    component_material_production_type,
    component_consumption_quantity,
    year
FROM bom_hierarchy
ORDER BY plant, year, fin_material_id, level;