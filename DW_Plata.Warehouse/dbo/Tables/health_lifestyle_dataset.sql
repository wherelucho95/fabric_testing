CREATE TABLE [dbo].[health_lifestyle_dataset] (

	[id] int NULL, 
	[age] int NULL, 
	[gender] varchar(10) NULL, 
	[bmi] decimal(4,1) NULL, 
	[daily_steps] int NULL, 
	[sleep_hours] decimal(3,1) NULL, 
	[water_intake_l] decimal(3,1) NULL, 
	[calories_consumed] int NULL, 
	[smoker] bit NULL, 
	[alcohol] bit NULL, 
	[resting_hr] smallint NULL, 
	[systolic_bp] smallint NULL, 
	[diastolic_bp] smallint NULL, 
	[cholesterol] smallint NULL, 
	[family_history] bit NULL, 
	[disease_risk] bit NULL
);