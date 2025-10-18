[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Salary_Data_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Age", DestinationColumnName = "Age"], [SourceColumnName = "Gender", DestinationColumnName = "Gender"], [SourceColumnName = "Education Level", DestinationColumnName = "Education Level"], [SourceColumnName = "Job Title", DestinationColumnName = "Job Title"], [SourceColumnName = "Years of Experience", DestinationColumnName = "Years of Experience"], [SourceColumnName = "Salary", DestinationColumnName = "Salary"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared Salary_Data = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "7368e6d6-9481-4cef-9fd6-3bd07c6b4e77"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "af58ae68-80e5-40e1-9b7b-383b30bf3eb0"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Salary_Data", ItemKind = "Table"]}[Data],
  #"Removed top rows" = Table.Skip(#"Navigation 2", 2)
in
  #"Removed top rows";
shared Salary_Data_DataDestination = let
  Pattern = Fabric.Warehouse([HierarchicalNavigation = null, CreateNavigationProperties = false]),
  Navigation_1 = Pattern{[workspaceId = "bf39c7eb-674e-42a1-a9d4-cc490c8fbec0"]}[Data],
  Navigation_2 = Navigation_1{[warehouseId = "7e0f0412-041d-48e5-9d74-b1d8622e975c"]}[Data],
  TableNavigation = Navigation_2{[Item = "Salary_Data", Schema = "dbo"]}?[Data]?
in
  TableNavigation;
