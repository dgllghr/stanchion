.import test/datasets/planets.csv planets

SELECT *
FROM (
    SELECT *
    FROM planets
    WHERE quadrant = 'Alpha' AND sector > 900

    UNION ALL

    SELECT *
    FROM planets
    WHERE quadrant = 'Beta' AND sector >= 100

    UNION ALL

    SELECT *
    FROM planets
    WHERE quadrant > 'Delta'
)
ORDER BY quadrant, sector, name
