.import test/datasets/planets.csv planets

SELECT *
FROM (
    SELECT *
    FROM planets
    WHERE quadrant = 'Alpha' AND sector = 57

    UNION ALL

    SELECT *
    FROM planets
    WHERE quadrant = 'Beta' AND sector = 201

    UNION ALL

    SELECT *
    FROM planets
    WHERE quadrant = 'Gamma' AND sector = 900
)
ORDER BY quadrant, sector, name
