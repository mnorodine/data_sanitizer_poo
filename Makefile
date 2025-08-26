# ======================================================================
# Makefile — data_sanitizer POO (sans cible venv, sans db-view-compat)
# ======================================================================

# Utiliser bash + options strictes
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

# ------ Variables principales ------------------------------------------
VENV      := .venv
PY        := $(VENV)/bin/python
PIP       := $(VENV)/bin/pip
CLI       := $(VENV)/bin/data-sanitizer

# Charge .env pour exporter les variables (DATABASE_URL, DS_PRICE_*, …)
# Usage : @$(LOAD_ENV) <commande>
LOAD_ENV  := set -a; [ -f .env ] && . ./.env; set +a

# Valeurs par défaut pour la commande update-prices
LIMIT ?=
SINCE ?=
ONLY  ?=
SLEEP ?=
DRY   ?= 1   # 1=lecture seule, 0=écriture

# ------ Cibles “meta” ---------------------------------------------------
.PHONY: help
help: ## Affiche cette aide
	@echo
	@echo "Targets disponibles:"
	@echo
	@awk 'BEGIN {FS = ":.*?## "}; /^[a-zA-Z0-9_-]+:.*?## /{printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo

# ------ Environnement / installation -----------------------------------
.PHONY: install
install: ## Installe le paquet en mode editable (+deps)
	@if [ ! -d "$(VENV)" ]; then \
		echo ">> Creating $(VENV)"; \
		python3 -m venv "$(VENV)"; \
	fi
	$(PIP) install -U pip setuptools wheel
	$(PIP) install -e .

.PHONY: freeze
freeze: ## Gèle les dépendances dans requirements.lock.txt
	$(PIP) freeze > requirements.lock.txt
	@echo "requirements.lock.txt mis à jour"

# ------ Qualité / tests -------------------------------------------------
.PHONY: lint
lint: ## Lint rapide si ruff est dispo
	@command -v $(VENV)/bin/ruff >/dev/null 2>&1 || { echo "ruff non installé (pip install ruff)"; exit 0; }
	$(VENV)/bin/ruff check data_sanitizer tests

.PHONY: lint-fix
lint-fix: ## Lint + auto-fix (POO uniquement)
	@command -v $(VENV)/bin/ruff >/dev/null 2>&1 || { echo "ruff non installé (pip install ruff)"; exit 0; }
	$(VENV)/bin/ruff check data_sanitizer tests --fix

.PHONY: fmt
fmt: ## Formatage ruff (POO uniquement)
	@command -v $(VENV)/bin/ruff >/dev/null 2>&1 || { echo "ruff non installé (pip install ruff)"; exit 0; }
	$(VENV)/bin/ruff format data_sanitizer tests


.PHONY: test
test: ## Lance pytest si présent
	@command -v $(VENV)/bin/pytest >/dev/null 2>&1 || { echo "pytest non installé (pip install pytest)"; exit 0; }
	$(VENV)/bin/pytest -q

# ------ Base de données -------------------------------------------------
.PHONY: psql
psql: ## Ouvre psql sur DATABASE_URL (via .env)
	@$(LOAD_ENV) psql "$$DATABASE_URL"

.PHONY: print-env
print-env: ## Affiche les DS_PRICE_* chargées depuis .env
	@$(LOAD_ENV) \
	echo "DS_PRICE_READ_VIEW  = $${DS_PRICE_READ_VIEW:-<non défini>}"; \
	echo "DS_PRICE_WRITE_TABLE= $${DS_PRICE_WRITE_TABLE:-<non défini>}"; \
	echo "DS_PRICE_DATE_COL   = $${DS_PRICE_DATE_COL:-<non défini>}";

# ------ Exécution applicative ------------------------------------------
.PHONY: update-prices
update-prices: ## Exécute update-prices avec les variables LIMIT/SINCE/ONLY/SLEEP/DRY
	@$(LOAD_ENV); \
	cmd="$(CLI) update-prices"; \
	[ -n "$(SINCE)" ] && cmd="$$cmd --since $(SINCE)"; \
	[ -n "$(LIMIT)" ] && cmd="$$cmd --limit $(LIMIT)"; \
	[ -n "$(ONLY)"  ] && cmd="$$cmd --only $(ONLY)"; \
	[ -n "$(SLEEP)" ] && cmd="$$cmd --sleep $(SLEEP)"; \
	if [ "$(DRY)" = "1" ]; then cmd="$$cmd --dry-run"; fi; \
	echo $$cmd; \
	$$cmd

.PHONY: update-prices-dry
update-prices-dry: ## Raccourci : DRY=1 (lecture seule)
	@$(MAKE) update-prices DRY=1 LIMIT="$(LIMIT)" SINCE="$(SINCE)" ONLY="$(ONLY)" SLEEP="$(SLEEP)"

.PHONY: update-prices-write
update-prices-write: ## Raccourci : DRY=0 (écriture)
	@$(MAKE) update-prices DRY=0 LIMIT="$(LIMIT)" SINCE="$(SINCE)" ONLY="$(ONLY)" SLEEP="$(SLEEP)"

# ------ Maintenance / utilitaires --------------------------------------
.PHONY: backup-sums
backup-sums: ## SHA256 des fichiers utiles (dump/exports) -> SHA256SUMS.txt
	@{ \
		for p in reports/*.csv datas/*.* sql/*.sql; do \
			[ -e "$$p" ] && sha256sum "$$p"; \
		done; \
	} > SHA256SUMS.txt
	@echo "SHA256SUMS.txt généré"

.PHONY: clean
clean: ## Nettoie artefacts Python
	@find . -type d -name "__pycache__" -print0 | xargs -0 rm -rf --
	@rm -rf .pytest_cache .ruff_cache dist build *.egg-info
	@echo "Nettoyage effectué"

.PHONY: distclean
distclean: clean ## Nettoie tout, y compris le venv
	@rm -rf "$(VENV)"
	@echo "Distclean effectué"
