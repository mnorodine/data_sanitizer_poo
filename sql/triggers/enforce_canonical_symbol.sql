-- =============================================
-- File: sql/triggers/enforce_canonical_symbol.sql
-- Purpose: Empêcher l'insertion/mise à jour d'un symbole non canonique pour un ISIN
-- =============================================
CREATE OR REPLACE FUNCTION enforce_canonical_symbol()
RETURNS trigger AS $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM symbol_canonical_map m
    WHERE m.isin = NEW.isin AND NEW.symbol <> m.canonical_symbol
  ) THEN
    RAISE EXCEPTION 'Symbol % is not canonical for ISIN % (expected %)',
      NEW.symbol, NEW.isin,
      (SELECT canonical_symbol FROM symbol_canonical_map WHERE isin=NEW.isin);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_enforce_canonical_symbol ON equities_prices;
CREATE TRIGGER trg_enforce_canonical_symbol
BEFORE INSERT OR UPDATE OF symbol ON equities_prices
FOR EACH ROW EXECUTE FUNCTION enforce_canonical_symbol();