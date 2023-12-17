.import test/datasets/planets.csv planets

SELECT *
FROM (
    SELECT *
    FROM planets
    WHERE quadrant < 'Beta'

    UNION ALL

    SELECT *
    FROM planets
    WHERE quadrant = 'Delta' AND sector <= 100

    UNION ALL

    SELECT *
    FROM planets
    WHERE quadrant = 'Gamma' AND sector < 989
)
ORDER BY quadrant, sector, name
