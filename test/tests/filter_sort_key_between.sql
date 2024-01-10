.import test/datasets/planets.csv planets

SELECT *
FROM planets
WHERE quadrant >= 'Beta' AND quadrant < 'Gamma'
ORDER BY quadrant, sector, name;

SELECT *
FROM planets
WHERE quadrant = 'Delta' AND sector > 57 AND sector <= 101
ORDER BY quadrant, sector, name;

SELECT *
FROM planets
WHERE (quadrant > 'Beta' OR (quadrant = 'Beta' AND sector > 57))
    AND (quadrant < 'Delta' OR (quadrant = 'Delta' AND sector < 500))
ORDER BY quadrant, sector, name;

SELECT *
FROM planets
WHERE (quadrant > 'Alpha' OR (quadrant = 'Alpha' AND sector > 900))
    AND (quadrant < 'Beta' OR (quadrant = 'Beta' AND sector <= 100))
ORDER BY quadrant, sector, name;

SELECT *
FROM planets
WHERE (quadrant > 'Alpha' OR (quadrant = 'Alpha' AND sector >= 900))
    AND (quadrant < 'Beta' OR (quadrant = 'Beta' AND sector < 100))
ORDER BY quadrant, sector, name;

SELECT *
FROM planets
WHERE (quadrant > 'Delta' OR (quadrant = 'Delta' AND sector >= 750))
    AND (quadrant < 'Gamma' OR (quadrant = 'Gamma' AND sector <= 900))
ORDER BY quadrant, sector, name;

SELECT *
FROM planets
WHERE (quadrant > 'Beta' OR (quadrant = 'Beta' AND sector > 900))
    AND (quadrant < 'Alpha' OR (quadrant = 'Alpha' AND sector <= 100))
ORDER BY quadrant, sector, name;
