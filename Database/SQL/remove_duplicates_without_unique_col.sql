/* ##########################################################################
   <<<<>>>> 2. Remove Duplicates Based on All of the Columns <<<<>>>>
   ########################################################################## */

/* Create cars table */
CREATE TABLE cars (
    id INT,
    model VARCHAR(50),
    brand VARCHAR(40),
    color VARCHAR(30),
    make INT
);

/* Insert sample data */
INSERT INTO cars VALUES (1, 'Model S', 'Tesla', 'Blue', 2018);
INSERT INTO cars VALUES (2, 'EQS', 'Mercedes-Benz', 'Black', 2022);
INSERT INTO cars VALUES (3, 'iX', 'BMW', 'Red', 2022);
INSERT INTO cars VALUES (4, 'Ioniq 5', 'Hyundai', 'White', 2021);
INSERT INTO cars VALUES (1, 'Model S', 'Tesla', 'Blue', 2018);
INSERT INTO cars VALUES (4, 'Ioniq 5', 'Hyundai', 'White', 2021);


-- >>> Method 1: Temporary Backup Table (SQL_server) <<< -----

-- Step 1: Create a backup table with distinct rows
SELECT DISTINCT * 
INTO cars_bkp 
FROM cars;

-- Step 2: Truncate the original table
TRUNCATE TABLE cars;

-- Step 3: Re-insert distinct rows into the original table
INSERT INTO cars 
SELECT DISTINCT * FROM cars_bkp;

-- Step 4: Drop the backup table
DROP TABLE cars_bkp;

-- >>> Method 2: creating a temporary unique id column <<< -----

-- Step 1: Add a row number column (optional if a primary key exists)
ALTER TABLE cars ADD row_num INT identity(1,1);

-- Step 2: Delete duplicates based on model and brand, keeping only the first occurrence
DELETE FROM cars
WHERE row_num NOT IN (
    SELECT MIN(row_num)
    FROM cars
    GROUP BY model, brand
);

-- Step 3: Drop the row number column
ALTER TABLE cars DROP COLUMN row_num;

-- >>> Method 3: Create Backup, Drop Original, Rename Backup <<< -----

-- Step 1: Create a backup table with distinct rows
CREATE TABLE cars_bkp AS
SELECT DISTINCT * FROM cars;

-- Step 2: Drop the original table
DROP TABLE cars;

-- Step 3: Rename the backup table to the original table's name
ALTER TABLE cars_bkp RENAME TO cars;

