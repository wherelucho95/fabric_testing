CREATE   PROCEDURE dbo.CrearYInsertarValores
AS
BEGIN
    -- Crear la tabla si no existe
    IF NOT EXISTS (
        SELECT 1
        FROM sys.tables
        WHERE name = 'Productos' AND schema_id = SCHEMA_ID('dbo')
    )
    BEGIN
        CREATE TABLE dbo.Productos (
            Id INT,
            Nombre VARCHAR(100),
            Precio DECIMAL(10,2)
        );
        PRINT '✅ Tabla dbo.Productos creada correctamente.';
    END
    ELSE
    BEGIN
        PRINT 'ℹ️ La tabla dbo.Productos ya existe.';
    END

    -- Insertar algunos valores
    INSERT INTO dbo.Productos (Nombre, Precio)
    VALUES 
        ('Laptop', 1200.50),
        ('Mouse', 25.75),
        ('Teclado', 45.90);

    PRINT '✅ Registros insertados correctamente.';

    -- Mostrar los valores insertados
    SELECT * FROM dbo.Productos;
END;