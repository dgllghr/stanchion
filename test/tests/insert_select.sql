.import test/datasets/planets.csv planets

-- Set the quadrant to Gamma so that rows are being inserted ahead of where the select
-- cursor is at some points during the query
INSERT INTO planets (quadrant, sector, size, gravity, name)
SELECT 'Gamma', sector, size, gravity, name || ' II'
FROM planets
WHERE sector > 100 AND sector <= 500;

SELECT * FROM planets ORDER BY quadrant, sector, name;

SELECT * FROM planets WHERE quadrant = 'Gamma' AND sector = 286 ORDER BY quadrant, sector, name;
