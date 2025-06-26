SELECT * FROM `{{ params.project_id }}.{{ params.dataset }}.table1`
UNION ALL
SELECT * FROM `{{ params.project_id }}.{{ params.dataset }}.table2`
WHERE order_date >= DATE("{{ macros.ds_add(ds, -7) }}")
