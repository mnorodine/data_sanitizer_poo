-- =============================================
-- File: sql/views/v_prices_compat.sql
-- Purpose: Exposer une vue stable pour Analyse_pea (colonnes: date, ticker, open, high, low, close, adj_close, volume)
-- =============================================
CREATE OR REPLACE VIEW v_prices_compat AS
SELECT
  p.price_date::date AS date,
  COALESCE(e.ticker, m.canonical_symbol) AS ticker,
  p.open_price  AS open,
  p.high_price  AS high,
  p.low_price   AS low,
  p.close_price AS close,
  p.adj_close   AS adj_close,
  p.volume      AS volume
FROM equities_prices p
JOIN symbol_canonical_map m USING(isin)
JOIN equities e USING(isin, symbol)
WHERE p.symbol = m.canonical_symbol;

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