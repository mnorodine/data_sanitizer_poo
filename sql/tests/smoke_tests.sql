-- =============================================
-- File: sql/tests/smoke_tests.sql
-- Purpose: Tests de qualité bloquants (à exécuter après migration)
--          Utilisation recommandée: psql -v ON_ERROR_STOP=1 -f sql/tests/smoke_tests.sql "$DB_URL"
-- =============================================

-- Test 1: ticker non NULL dans equities
DO $$
DECLARE cnt INT;
BEGIN
  SELECT COUNT(*) INTO cnt FROM equities WHERE ticker IS NULL;
  IF cnt > 0 THEN
    RAISE EXCEPTION 'SmokeTest FAIL: equities.ticker NULL count = %', cnt;
  END IF;
END $$;

-- Test 2: tous les ISIN de equities_prices doivent être mappés
DO $$
DECLARE cnt INT;
BEGIN
  SELECT COUNT(DISTINCT ep.isin) INTO cnt
  FROM equities_prices ep
  LEFT JOIN symbol_canonical_map m USING(isin)
  WHERE m.isin IS NULL;
  IF cnt > 0 THEN
    RAISE EXCEPTION 'SmokeTest FAIL: % ISIN non mappés dans symbol_canonical_map', cnt;
  END IF;
END $$;

-- Test 3: adj_close sans NULL (après fallback)
DO $$
DECLARE cnt INT;
BEGIN
  SELECT COUNT(*) INTO cnt FROM equities_prices WHERE adj_close IS NULL;
  IF cnt > 0 THEN
    RAISE EXCEPTION 'SmokeTest FAIL: adj_close contient encore % NULL', cnt;
  END IF;
END $$;

-- Test 4: vue v_prices_compat existe et non vide
DO $$
DECLARE cnt INT;
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.views
    WHERE table_schema='public' AND table_name='v_prices_compat'
  ) THEN
    RAISE EXCEPTION 'SmokeTest FAIL: vue v_prices_compat absente';
  END IF;
  EXECUTE 'SELECT COUNT(*) FROM v_prices_compat' INTO cnt;
  IF cnt = 0 THEN
    RAISE EXCEPTION 'SmokeTest FAIL: v_prices_compat est vide';
  END IF;
END $$;

-- Test 5: trigger présent et activé
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger t
    JOIN pg_class c ON c.oid = t.tgrelid
    WHERE t.tgname = 'trg_enforce_canonical_symbol'
      AND c.relname = 'equities_prices'
      AND t.tgenabled = 'O'
  ) THEN
    RAISE EXCEPTION 'SmokeTest FAIL: trigger trg_enforce_canonical_symbol manquant ou désactivé';
  END IF;
END $$;

-- Test 6: aucune valeur de prix négative ou nulle
DO $$
DECLARE cnt INT;
BEGIN
  SELECT COUNT(*) INTO cnt FROM equities_prices
  WHERE (open_price IS NOT NULL AND open_price <= 0)
     OR (high_price IS NOT NULL AND high_price <= 0)
     OR (low_price  IS NOT NULL AND low_price  <= 0)
     OR (close_price IS NOT NULL AND close_price <= 0)
     OR (adj_close   IS NOT NULL AND adj_close   <= 0);
  IF cnt > 0 THEN
    RAISE EXCEPTION 'SmokeTest FAIL: % prix non positifs détectés', cnt;
  END IF;
END $$;

-- OK
SELECT 'Smoke tests OK' AS status;
