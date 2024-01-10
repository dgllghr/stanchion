.import test/datasets/planets.csv planets

SELECT *
FROM planets
WHERE sector = 100
ORDER BY quadrant, sector, name;

SELECT *
FROM planets
WHERE sector > 270 AND sector < 300
ORDER BY quadrant, sector, name;

SELECT *
FROM planets
WHERE name LIKE '%pl%'
ORDER BY quadrant, sector, name;

SELECT *
FROM planets
WHERE gravity < 1000.0
ORDER BY quadrant, sector, name;

