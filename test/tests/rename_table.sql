.import test/datasets/planets.csv planets

ALTER TABLE planets RENAME TO planets2;

.import test/datasets/planets2.csv planets2

SELECT *
FROM planets2
WHERE quadrant = 'Gamma'
ORDER BY quadrant, sector, name;
