WITH most_visited_floor AS (
    SELECT 
        name,
        floor, 
        COUNT(floor) AS most_visited,
        RANK() OVER(PARTITION BY name ORDER BY COUNT(floor) DESC) AS rn
    FROM 
        entries
    GROUP BY 
        name, floor
),
visited_tag AS (
    SELECT 
        name, 
        floor AS most_visted
    FROM 
        most_visited_floor
    WHERE 
        rn = 1
)
SELECT 
    v.name,
    v.most_visted,
    STRING_AGG(e.resources, ',') AS Used_resources
FROM 
    visited_tag v
JOIN 
    entries e
ON 
    v.name = e.name
GROUP BY 
    v.name, v.most_visted;
