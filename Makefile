# ============================
# Makefile — data_sanitizer_poo
# Sécurisé, venv forcé, compat binaire/module, idempotent
# ============================

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c

# --------- Réglages ----------
VENV ?= .venv
PYTHON := $(VENV)/bin/python
PIP    := $(VENV)/bin/pip

# Nom du package (module Python) et extras dev
PACKAGE_NAME   ?= data_sanitizer
EXTRA_EDITABLE ?= ".[dev]"

# Charger .env si présent (DS_*, DATABASE_URL, etc.)
ifneq (,$(wildcard .env))
    include .env
    export
endif

# Cible par défaut
.DEFAULT_GOAL := help

# Toutes les cibles symboliques
.PHONY: help venv install reinstall lock freeze freeze-all pip-check lint test \
        update-prices update-prices-dry check-db doctor print-env print-paths \
        clean distclean purge-venv which

# --------- AIDE ----------
help:
	@echo "Cibles disponibles :"
	@echo "  make install            - Crée le venv si besoin et installe le paquet en editable (extras si dispo)"
	@echo "  make reinstall          - Réinstalle proprement en editable (rebuild wheel/metadata)"
	@echo "  make lint               - Lint du code (Ruff)"
	@echo "  make test               - Tests (Pytest)"
	@echo "  make check-db           - Ping DB via le CLI"
	@echo "  make update-prices      - Mise à jour des prix (write)"
	@echo "  make update-prices-dry  - Simulation (dry run)"
	@echo "  make doctor             - Diagnostic rapide (versions, import, pip check, chemins)"
	@echo "  make print-env          - Affiche les variables DS_*"
	@echo "  make print-paths        - Montre les chemins utiles"
	@echo "  make lock               - (Re)génère requirements.lock.txt proprement"
	@echo "  make freeze             - Alias de 'lock'"
	@echo "  make freeze-all         - Fige TOUT (incl. l'editable) dans requirements.lock.txt"
	@echo "  make clean              - Supprime fichiers temporaires"
	@echo "  make distclean          - clean + supprime build/egg-info"
	@echo "  make purge-venv         - supprime entièrement le venv (.venv)"

# --------- VENV ----------
venv:
	@if [ ! -d "$(VENV)" ]; then \
		echo ">> Création du venv dans $(VENV)"; \
		python3 -m venv "$(VENV)"; \
		"$(PYTHON)" -m pip install -U pip setuptools wheel; \
	fi

# --------- INSTALL ----------
install: venv
	@echo ">> Installation editable du paquet"
	@set -e; \
	if $(PIP) install -e $(EXTRA_EDITABLE) ; then \
		echo ">> Install avec extras OK ($(EXTRA_EDITABLE))"; \
	else \
		echo ">> Extras indisponibles, on installe -e ."; \
		$(PIP) install -e . ; \
	fi

reinstall: venv
	@echo ">> Réinstallation editable (nettoyage metadata)"
	@rm -rf $(PACKAGE_NAME).egg-info *.egg-info build dist || true
	@$(PIP) install -U pip setuptools wheel
	@$(MAKE) install

# --------- QUALITÉ / TESTS ----------
lint: venv
	@echo ">> Ruff (lint)"
	@"$(PYTHON)" -m ruff check .

test: venv
	@echo ">> Pytest"
	@"$(PYTHON)" -m pytest -q

pip-check: venv
	@echo ">> pip check"
	@"$(PIP)" check || true

# --------- Compat CLI binaire / module -----------
# Utilise le binaire .venv/bin/data-sanitizer s'il existe, sinon fallback module.
define RUN_SANITIZER
	@if [ -x "$(VENV)/bin/data-sanitizer" ]; then \
		echo ">> Using entrypoint binary: $(VENV)/bin/data-sanitizer $(1)"; \
		"$(VENV)/bin/data-sanitizer" $(1); \
	else \
		echo ">> Using module fallback: $(PYTHON) -m $(PACKAGE_NAME).cli $(1)"; \
		"$(PYTHON)" -m $(PACKAGE_NAME).cli $(1); \
	fi
endef

# --------- CLI / RUN ----------
check-db:
	@data-sanitizer check-db

update-prices:
	@data-sanitizer update-prices --write $(ARGS)

update-prices-dry:
	@data-sanitizer update-prices --dry-run $(ARGS)

# --------- LOCK / FREEZE ----------
# 'lock' régénère un lock cohérent depuis l'env courant (exclut l'editable local)
lock: venv
	@echo ">> (Re)génération du lock dans requirements.lock.txt"
	@"$(PIP)" freeze --exclude-editable > requirements.lock.txt
	@echo ">> Lock écrit : requirements.lock.txt"

freeze: lock

# Variante "tout figer" (inclut l'editable sous forme de -e .)
freeze-all: venv
	@echo ">> Freeze ALL (incluant l'editable)"
	@"$(PIP)" freeze > requirements.lock.txt
	@echo ">> Lock ALL écrit : requirements.lock.txt"

# --------- DIAGNOSTIC ----------
doctor:
	@data-sanitizer doctor

print-env:
	@env | grep '^DS_' || true

print-paths:
	@echo "python        : $(PYTHON)"
	@echo "pip           : $(PIP)"
	@echo "data-sanitizer: $$(test -x "$(VENV)/bin/data-sanitizer" && echo "$(VENV)/bin/data-sanitizer" || echo "(non installé)")"
	@echo "module cli    : $(PYTHON) -m $(PACKAGE_NAME).cli"

which:
	@which python || true
	@which pip || true
	@which $(PACKAGE_NAME) || true

# --------- NETTOYAGE ----------
clean:
	@echo ">> clean"
	@find . -name '*.pyc' -delete
	@find . -name '__pycache__' -type d -exec rm -rf {} +
	@rm -rf .ruff_cache .pytest_cache .mypy_cache .coverage htmlcov || true

distclean: clean
	@echo ">> distclean"
	@rm -rf build dist *.egg-info $(PACKAGE_NAME).egg-info || true

purge-venv:
	@echo ">> purge venv"
	@rm -rf "$(VENV)"






