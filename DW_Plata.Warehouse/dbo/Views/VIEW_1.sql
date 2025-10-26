-- Auto Generated (Do not modify) BBEFAA5932AC49206BE22C276DBF4E02E226005F20905FDB1E3CBC24F1651524
CREATE VIEW VIEW_1
AS 
select T1.*,
DENSE_RANK() over (PARTITION by T1.[Job Title] order by T1.[Salary] DESC) as ranking,
avg(T1.[Salary]) over(PARTITION BY T1.[Job Title]) as Promedio
from (
SELECT
cast([Age] as FLOAT) as Age,
[Gender],
[Education Level],
[Job Title],
cast([Years of Experience] as FLOAT) as [Years of Experience],
cast([Salary] as FLOAT) as Salary
from Salary_Data) AS T1