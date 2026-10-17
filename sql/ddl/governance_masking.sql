-- =============================================================================
-- Gobierno avanzado (opcional, Unity Catalog): enmascaramiento y filtros de filas
-- Se usa en el módulo 15 de la ruta de aprendizaje. Requiere permisos de owner.
-- =============================================================================

-- Máscara de columna: sólo el grupo `pii_readers` ve el email completo
CREATE OR REPLACE FUNCTION ${gold}.mask_email(email STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('pii_readers') THEN email
    ELSE CONCAT(LEFT(email, 2), '***@', SPLIT_PART(email, '@', 2))
END;

ALTER TABLE ${gold}.dim_customer ALTER COLUMN email SET MASK ${gold}.mask_email;

-- Filtro de filas: cada gerente regional sólo ve las ventas de su región
CREATE OR REPLACE FUNCTION ${gold}.region_filter(store_key BIGINT)
RETURNS BOOLEAN
RETURN is_account_group_member('sales_admins')
    OR EXISTS (
        SELECT 1 FROM ${gold}.dim_store s
        WHERE s.store_key = store_key
          AND is_account_group_member(CONCAT('sales_', LOWER(REPLACE(s.region, ' ', '_'))))
    );

-- ALTER TABLE ${gold}.fact_sales SET ROW FILTER ${gold}.region_filter ON (store_key);

-- Permisos (principio de mínimo privilegio)
-- GRANT USE CATALOG ON CATALOG ${catalog} TO `analistas`;
-- GRANT USE SCHEMA, SELECT ON SCHEMA ${gold} TO `analistas`;
-- GRANT SELECT ON TABLE ${gold}.mart_sales_daily TO `gerencia`;
