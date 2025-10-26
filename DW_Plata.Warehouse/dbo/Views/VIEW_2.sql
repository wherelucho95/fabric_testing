-- Auto Generated (Do not modify) 19F7F650BECA91D3EBDCD500584DF7C84498AED22877E2E95708E926376DB2AB
CREATE VIEW VIEW_2
AS 
SELECT [Job Title] ,
CASE 
    WHEN [Years of Experience] < 3 THEN 'Junior'
    WHEN [Years of Experience] BETWEEN 3 AND 5 THEN 'Mid-level'
    WHEN [Years of Experience] > 5 THEN 'Senior'
END AS [Seniority],
AVG([Salary]) AS AVGSALARY,
MAX([Salary]) AS BESTPAYER,
COUNT(*) AS TOTALCOUNT
from (
SELECT
cast([Age] as FLOAT) as Age,
[Gender],
[Education Level],
[Job Title],
cast([Years of Experience] as FLOAT) as [Years of Experience],
cast([Salary] as FLOAT) as Salary
from Salary_Data) AS T1
GROUP BY [Job Title],
CASE 
    WHEN [Years of Experience] < 3 THEN 'Junior'
    WHEN [Years of Experience] BETWEEN 3 AND 5 THEN 'Mid-level'
    WHEN [Years of Experience] > 5 THEN 'Senior'
END;