-- Please use SQL server for this demo
--create target table
CREATE TABLE Products(
  ProductID INT PRIMARY KEY,
  ProductName VARCHAR(100),
  Rate MONEY
)
GO
  
--Insert records into target table
INSERT INTO Products
VALUES
   (1, 'Tea', 10.00),
   (2, 'Coffee', 20.00),
   (3, 'Muffin', 30.00),
   (4, 'Biscuit', 40.00)
GO

--Create source table
CREATE TABLE UpdatedProducts
(
   ProductID INT PRIMARY KEY,
   ProductName VARCHAR(100),
   Rate MONEY
) 
GO

--Insert records into source table
INSERT INTO UpdatedProducts
VALUES
   (1, 'Tea', 10.00),
   (2, 'Coffee', 25.00),
   (3, 'Muffin', 35.00),
   (5, 'Pizza', 60.00)
GO

-MERGE SQL statement - Part 2
--Synchronize the target table with refreshed data from source table

MERGE Products AS TARGET
USING UpdatedProducts AS SOURCE
ON (TARGET.ProductID = SOURCE.ProductID)
--When records are matched, update the records if there is any change
WHEN MATCHED AND TARGET.ProductName <> SOURCE.ProductName
  OR TARGET.Rate <> SOURCE.Rate
  THEN UPDATE SET TARGET.ProductName = SOURCE.ProductName,
  TARGET.Rate = SOURCE.Rate

--When no records are matched, insert the incoming records from source table to target table
WHEN NOT MATCHED BY TARGET 
  THEN INSERT (ProductID, ProductName, Rate) Values (SOURCE.ProductID, SOURCE.ProductName, SOURCE.Rate)
WHEN NOT MATCHED BY SOURCE
  THEN DELETE

--$action specifies a column of type nvarchar(10) in the OUTPUT clause that returns 
--one of three values for each row: 'INSERT', 'UPDATE', or 'DELETE' according to the action that was performed on that row
OUTPUT $action, 
DELETED.ProductID AS TargetProductID, 
DELETED.ProductName AS TargetProductName, 
DELETED.Rate AS TargetRate, 
INSERTED.ProductID AS SourceProductID, 
INSERTED.ProductName AS SourceProductName, 
INSERTED.Rate AS SourceRate; 

SELECT @@ROWCOUNT;
GO

