/* Remove duplicates using different methods
  1. Remove Duplicates for specific columns
  2. Remove Duplicates for entire columns
*/

/* ##########################################################################
   <<<<>>>> 1. Remove Duplicates for Specific Columns <<<<>>>>
   ########################################################################## */




/* Solution 1: Delete using a Unique identifier */
DELETE FROM cars
WHERE id IN (
    SELECT MAX(id)
    FROM cars
    GROUP BY model, brand
    HAVING COUNT(1) > 1
);

/* Solution 2: Using SELF join */
DELETE FROM cars
WHERE id IN (
    SELECT c2.id
    FROM cars c1
    JOIN cars c2 ON c1.model = c2.model AND c1.brand = c2.brand
    WHERE c1.id < c2.id
);

/* Solution 3: Using Window Function */
DELETE FROM cars
WHERE id IN (
    SELECT id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY model, brand ORDER BY id) AS rn
        FROM cars
    ) x
    WHERE x.rn > 1
);

/* Solution 4: Using MIN function. This deletes even multiple duplicate records. */
DELETE FROM cars
WHERE id NOT IN (
    SELECT MIN(id)
    FROM cars
    GROUP BY model, brand
);

/* Solution 5: Using Backup Table */
DROP TABLE IF EXISTS cars_bkp;
CREATE TABLE cars_bkp AS
SELECT * FROM cars WHERE 1 = 0;

INSERT INTO cars_bkp
SELECT * FROM cars
WHERE id IN (
    SELECT MIN(id)
    FROM cars
    GROUP BY model, brand
);

DROP TABLE cars;
ALTER TABLE cars_bkp RENAME TO cars;

/* Solution 6: Use the backup table without dropping the original table */
DROP TABLE IF EXISTS cars_bkp;
CREATE TABLE cars_bkp AS
SELECT * FROM cars WHERE 1 = 0;

INSERT INTO cars_bkp
SELECT * FROM cars
WHERE id IN (
    SELECT MIN(id)
    FROM cars
    GROUP BY model, brand
);

TRUNCATE TABLE cars;

INSERT INTO cars
SELECT * FROM cars_bkp;

DROP TABLE cars_bkp;


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

SELECT * FROM cars;

/* Solution 1: Delete using CTID / ROWID (in Oracle) */
DELETE FROM cars
WHERE ctid IN (
    SELECT MAX(ctid)
    FROM cars
    GROUP BY model, brand
    HAVING COUNT(1) > 1
);

/* Solution 2: By creating a temporary unique id column */
ALTER TABLE cars ADD COLUMN row_num INT GENERATED ALWAYS AS IDENTITY;

DELETE FROM cars
WHERE row_num NOT IN (
    SELECT MIN(row_num)
    FROM cars
    GROUP BY model, brand
);

ALTER TABLE cars DROP COLUMN row_num;

/* Solution 3: By creating a backup table */
CREATE TABLE cars_bkp AS
SELECT DISTINCT * FROM cars;

DROP TABLE cars;
ALTER TABLE cars_bkp RENAME TO cars;

/* Solution 4: Creating a backup table without dropping the original table */
CREATE TABLE cars_bkp AS
SELECT DISTINCT * FROM cars;

TRUNCATE TABLE cars;

INSERT INTO cars
SELECT DISTINCT * FROM cars_bkp;

DROP TABLE cars_bkp;




