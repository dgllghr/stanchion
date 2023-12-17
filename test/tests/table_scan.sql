.import test/datasets/planets.csv planets

SELECT *
FROM (
    SELECT *
    FROM planets
    WHERE sector = 100

    UNION ALL

    SELECT *
    FROM planets
    WHERE name LIKE '%pl%'

    UNION ALL

    SELECT *
    FROM planets
    WHERE gravity < 1000.0
)
ORDER BY quadrant, sector, name