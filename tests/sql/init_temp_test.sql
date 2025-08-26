DROP TABLE IF EXISTS temp_test;

CREATE TABLE temp_test (
    id SERIAL PRIMARY KEY,
    nom TEXT,
    valeur NUMERIC,
    date_insertion TEXT  -- volontairement en TEXT pour simuler des mauvais formats
);

INSERT INTO temp_test (nom, valeur, date_insertion) VALUES
('A', 10.5, '2024-01-01'),
('B', NULL, '2024-01-02'),
('C', 7.3, 'erreur_date'),
('D', NULL, NULL);
