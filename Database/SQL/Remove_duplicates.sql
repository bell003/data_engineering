--Remove duplicates using different methods

--Method 1: Using Subquery with EXISTS:
/*
Pros: Relatively simple and avoids the use of CTEs or creating new tables.
Cons: Performance can degrade on very large tables due to the need to perform a self-join and comparison of ctid values.
*/

DELETE FROM employee a
USING employee b
WHERE a.ctid < b.ctid
  AND a.name = b.name
  AND a.dept = b.dept;


--Method 2: Using DELETE with ROW_NUMBER(): Recommanded method
/*
Pros: It’s a straightforward approach that efficiently utilizes window functions to identify duplicates.
Cons: It involves a CTE and might be slower on very large tables due to the need to compute row numbers for all rows.
*/

WITH cte AS (
    SELECT 
        ctid,
        ROW_NUMBER() OVER (PARTITION BY name, dept ORDER BY id) AS rn
    FROM 
        employee
)
DELETE FROM employee
WHERE ctid IN (
    SELECT ctid
    FROM cte
    WHERE rn > 1
);

-- Method 3: Using GROUP BY with MIN or MAX:
/*
Pros: Efficient for large datasets if properly indexed on the grouping columns. Simple and commonly used.
Cons: Might not perform as well without appropriate indexes. Can be slower for tables with a large number of duplicates.
*/

DELETE FROM employee
WHERE id NOT IN (
    SELECT MIN(id)
    FROM employee
    GROUP BY name, dept
);

-- Method 4: Using DISTINCT to Create a New Table:
/*
Pros: Simple and easy to understand. Creating a new table and inserting distinct rows can be efficient.
Cons: Requires sufficient disk space to hold two copies of the table during the operation. 
It also involves dropping and renaming tables which might have additional overhead if there are indexes, foreign keys, or constraints.
*/
CREATE TABLE employee_new AS
SELECT DISTINCT ON (name, dept) id, name, dept
FROM employee;

DROP TABLE employee;

ALTER TABLE employee_new RENAME TO employee;




