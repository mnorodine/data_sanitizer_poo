# --- Makefile pour data_sanitizer_poo (POO) ---

PYTHON ?= python
PIP ?= pip

# Variables d'environnement (chargées depuis .env si présent)
ifneq (,$(wildcard .env))
    include .env
    export
endif

# --- HELP ---
help:
	@echo "Cibles disponibles :"
	@echo "  make install         - Installation en mode editable avec dépendances"
	@echo "  make freeze          - Gèle les dépendances dans requirements.lock.txt"
	@echo "  make lint            - Vérifie la qualité du code avec Ruff"
	@echo "  make test            - Lance les tests Pytest"
	@echo "  make print-env       - Affiche les variables DS_*"
	@echo "  make update-prices   - Met à jour les prix (write)"
	@echo "  make update-prices-dry - Simulation update-prices (dry run)"
	@echo "  make clean           - Supprime fichiers temporaires"
	@echo "  make distclean       - Nettoie aussi le venv et les caches pip"

# --- INSTALLATION & DEPENDANCES ---
install:
	$(PIP) install -e .
	$(PIP) install -r requirements.txt || true

freeze:
	$(PIP) freeze > requirements.lock.txt

# --- LINT & TEST ---
lint:
	ruff check .

test:
	pytest -q

# --- OUTILS ---
print-env:
	@echo "DATABASE_URL=$(DATABASE_URL)"
	@echo "DS_PRICE_READ_VIEW=$(DS_PRICE_READ_VIEW)"
	@echo "DS_PRICE_WRITE_TABLE=$(DS_PRICE_WRITE_TABLE)"
	@echo "DS_PRICE_DATE_COL=$(DS_PRICE_DATE_COL)"
	@echo "LOG_LEVEL=$(LOG_LEVEL)"
	@echo "REQUEST_PAUSE_S=$(REQUEST_PAUSE_S)"
	@echo "YF_TIMEOUT_S=$(YF_TIMEOUT_S)"

# --- COMMANDES MÉTIER ---
update-prices:
	$(PYTHON) -m data_sanitizer.cli update-prices --write

update-prices-dry:
	$(PYTHON) -m data_sanitizer.cli update-prices --dry-run

# --- NETTOYAGE ---
clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -type d -exec rm -rf {} +
	rm -rf .ruff_cache .pytest_cache

distclean: clean
	rm -rf .venv
	rm -rf build dist *.egg-info
