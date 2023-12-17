CREATE TABLE planets (
    quadrant TEXT NOT NULL,
    sector INTEGER NOT NULL,
    size INTEGER NULL,
    gravity REAL NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (quadrant, sector, name)
) STRICT, WITHOUT ROWID;
