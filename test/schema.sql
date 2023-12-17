CREATE VIRTUAL TABLE planets USING stanchion (
    quadrant TEXT NOT NULL,
    sector INTEGER NOT NULL,
    size INTEGER NULL,
    gravity FLOAT NULL,
    name TEXT NOT NULL,
    SORT KEY (quadrant, sector)
);
