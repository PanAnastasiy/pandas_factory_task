
DELETE FROM bom_reports;


INSERT INTO bom_reports (
    plant,
    year,
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
    component_consumption_quantity
)
WITH RECURSIVE aggregated_bom AS (
    SELECT
        plant_id,
        year,
        produced_material_id,
        produced_material_release_type,
        produced_material_production_type,
        SUM(produced_material_quantity) as produced_material_quantity,
        component_material_id,
        component_material_release_type,
        component_material_production_type,
        SUM(component_material_quantity) as component_material_quantity
    FROM
        raw_factory_data
    GROUP BY
        plant_id,
        year,
        produced_material_id,
        produced_material_release_type,
        produced_material_production_type,
        component_material_id,
        component_material_release_type,
        component_material_production_type
),
bom_hierarchy AS (
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
        aggregated_bom r
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
        aggregated_bom child
    JOIN
        bom_hierarchy parent
        ON child.produced_material_id = parent.component_id
        AND child.plant_id = parent.plant
        AND child.year = parent.year
)
SELECT
    plant,
    year,
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
    component_consumption_quantity
FROM bom_hierarchy;