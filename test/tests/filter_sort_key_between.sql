.import test/datasets/planets.csv planets

SELECT *
FROM planets
WHERE (quadrant > 'Beta' OR (quadrant = 'Beta' AND sector > 57))
    AND (quadrant < 'Delta' OR (quadrant = 'Delta' AND sector < 500))
ORDER BY quadrant, sector, name
