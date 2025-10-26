CREATE   FUNCTION dbo.CuadradoDeDigitos (@numero BIGINT)
RETURNS VARCHAR(MAX)
AS
BEGIN
    DECLARE @resultado VARCHAR(MAX) = '';
    DECLARE @i INT = 1;
    DECLARE @longitud INT = LEN(@numero);
    DECLARE @digito INT;

    WHILE @i <= @longitud
    BEGIN
        -- Extraer el dígito como entero
        SET @digito = CAST(SUBSTRING(CAST(@numero AS VARCHAR), @i, 1) AS INT);

        -- Concatenar el cuadrado al resultado
        SET @resultado = @resultado + CAST(@digito * @digito AS VARCHAR);

        SET @i += 1;
    END

    RETURN @resultado;
END;