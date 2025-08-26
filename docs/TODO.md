
Parfait — voici ta checklist de ménage pour la refonte POO, prête à suivre au prochain run. J’ai mis des commandes concrètes et des décisions “à valider”.
1) Git & branches

Confirmer l’état de travail: main = legacy figée, feat/poo-core = POO active (worktree data_sanitizer_poo).

Ajouter un .gitignore minimal à la racine POO (si pas déjà) :

.venv/
__pycache__/
*.pyc
.ruff_cache/
.pytest_cache/
.env
reports/
logs/
outputs/

    Commit initial “cleanup POO baseline”.

2) Environnement & variables

Nettoyer .env (uniquement ce qui est utilisé par la POO) :

DATABASE_URL=postgresql://pea_user:pea_password@127.0.0.1:5432/pea_db
DS_PRICE_READ_VIEW=v_prices_compat
DS_PRICE_WRITE_TABLE=equity_prices
DS_PRICE_DATE_COL=price_date
LOG_LEVEL=INFO
REQUEST_PAUSE_S=0.6
YF_TIMEOUT_S=10

    Vérifier que get_settings() lit bien ces clés (OK).

3) Base de données
3.1 Cibles à garder

    equities (table principale).

    equity_prices (nouvelle table cible).

    Vues utiles: v_equities_active, v_equities_activity, etc. (si encore consommées).

    v_prices_compat (vue de compatibilité → source de lecture POO).

Script idempotent :

        DROP VIEW IF EXISTS public.v_prices_compat;
        CREATE VIEW public.v_prices_compat (
          isin, symbol, price_date, open_price, close_price, high_price, low_price, volume, adj_close
        ) AS
        SELECT isin, symbol, price_date, open_price, close_price, high_price, low_price, volume, adj_close
        FROM public.equity_prices;

3.2 Cibles à déprécier/supprimer (après dernier besoin legacy)

equities_prices_old (table legacy de test) → DROP TABLE quand plus utilisée.

equities_prices (vue legacy) → à supprimer une fois la bascule confirmée côté legacy.

    Vues/objets Temp/ de tests si non utilisés.

3.3 Index/contraintes

Vérifier index equity_prices (tu as déjà des PK + index par (isin, price_date) et (symbol, price_date) — ça semble OK).

    (Optionnel) Ajouter des index sur equities: (is_valid, is_active), (ticker) — déjà présents.

4) Arborescence code (POO only)

À garder

data_sanitizer/
  __init__.py
  config.py
  domain/        # models, entités
  ports/         # interfaces (EquitiesRepo, PricesRepo, MarketClient, Resolver…)
  services/      # cas d’usage (UpdatePricesService)
  adapters/
    db/          # EquitiesRepoPg, PricesRepoPg, common.get_pg
    providers/   # YFinanceClient, DefaultTickerResolver
  cli/
    __init__.py
    __main__.py  # Typer app (update-prices)

À déplacer ou supprimer

scripts/ legacy (garder uniquement si vraiment utile, sinon déplacer dans legacy_scripts/ hors package).

Temp/ (tout ce qui n’est pas utile à la POO → archiver ou supprimer).

providers/ (dossier racine) si doublon obsolète → supprimer si migré sous adapters/providers.

    reports/, reports (déjà nettoyés).

5) Packaging & pyproject

Confirmer pyproject.toml (entry point & deps) :

[project]
name = "data-sanitizer"
version = "3.0.0a0"
dependencies = [
  "psycopg[binary]>=3.2",
  "pandas>=2.2",
  "typer>=0.12",
  "python-dotenv>=1.0",
  "yfinance>=0.2.52",
]

[project.scripts]
data-sanitizer = "data_sanitizer.cli.__main__:main"

    Si on ajoute ruff/pytest, les mettre en optional-dependencies.dev.

6) Makefile (POO minimal)

Cibles à garder (tu as déjà une bonne base)

    help

    install (editable + deps)

    freeze (→ requirements.lock.txt)

    lint (ruff si présent)

    test (pytest si présent)

    psql (ouvre psql avec DATABASE_URL)

    print-env (montre les DS_* effectives)

    update-prices, update-prices-dry, update-prices-write

    backup-sums

    clean, distclean

Cibles à supprimer

    venv (tu as ton alias/fonction shell)

    db-view-compat (tu préfères piloter direct via psql)

7) Linting & Tests

Ajouter .ruff.toml :

line-length = 100
target-version = "py312"
select = ["E", "F", "W", "I"]
ignore = []
exclude = ["legacy_scripts", "Temp", "scripts"]

    pytest minimal (smoke tests) :

        test import package

        test PricesRepoPg.last_price_date() en lecture sur v_prices_compat

        test CLI --help et exécution --dry-run --limit 1 (mocker YFinanceClient si besoin)

8) Service & Adapters (petits durcissements)

services/update_prices.py : enlever variables inutilisées (ex: inserted si non loggé).

EquitiesRepoPg : méthodes fetch_targets/get_targets + get_existing_ticker + mark_attempt OK (tu les as ajoutées) — valider SQL final.

    PricesRepoPg :

        last_price_date() lit bien DS_PRICE_* (OK).

        upsert_bars() → DELETE/INSERT ou UPSERT : choisir une seule stratégie (conseillé : UPSERT avec ON CONFLICT (isin,symbol,price_date) DO UPDATE).

        recompute_counts() et update_bounds() : à implémenter si vraiment consommés, sinon stub/retour neutre.

9) Logs & Observabilité

Logging niveau INFO + compteur (traités, insérés, ignorés).

    Option --verbose ou LOG_LEVEL=DEBUG pour diagnostics SQL.

10) Documentation

    README.md minimal POO :

        Prérequis (Python 3.12+, Postgres, DS_*)

        Installation rapide (python -m venv, make install)

        Exécution (make update-prices-dry, puis make update-prices-write)

        Variables d’environnement (tableau)

        Modèle de DB utilisé (équities, equity_prices, v_prices_compat)

11) Sécurité / sauvegardes

    backup-sums déjà présent. Si besoin : pg_dump scripts (hors Makefile) pour sauvegardes manuelles.

Mini-plan d’exécution (ordre conseillé)

    Git: commit baseline + .gitignore.

    .env minimal + vérifier make print-env.

    DB: (re)créer v_prices_compat ; valider equity_prices.

    Arborescence: déplacer/supprimer Temp/ & scripts/ non-POO.

    pyproject.toml: vérifier project.scripts.

    Makefile: garder cibles POO, retirer le reste.

    Lint: ajouter .ruff.toml ; exécuter make lint (ignorer Temp/ & scripts/).

    Tests: smoke pytest (optionnel mais recommandé).

    Run: make update-prices-dry → make update-prices-write.

    Doc: actualiser README.md.

Si tu veux, je peux te générer :

    une version finale du Makefile (avec comments),

    un .ruff.toml propre,

    un script SQL idempotent pour (re)créer v_prices_compat,

    un stub d’UPSERT propre pour PricesRepoPg.upsert_bars().

Dis-moi lesquels tu veux tout de suite et je te fournis les fichiers.


ChatGPT peut commettre des erreurs. Il est recommandé de vérifier les informations importantes. Voir les préférences en matière de cookies.
