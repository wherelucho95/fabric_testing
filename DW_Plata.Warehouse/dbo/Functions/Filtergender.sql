CREATE   FUNCTION dbo.Filtergender (@gender varchar(10))
RETURNS TABLE
AS
RETURN
(
    SELECT *
    FROM Salary_Data
    WHERE Gender = @gender
)