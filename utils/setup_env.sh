#!/usr/bin/env bash
set -euo pipefail

# --- Paramètres (peuvent être passés en variables d'env) ---
PROJECT_ROOT="${PROJECT_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
MAKEFILE="${PROJECT_ROOT}/Makefile"
ENV_EXAMPLE="${PROJECT_ROOT}/.env.example"
ENV_FILE="${PROJECT_ROOT}/.env"
SERVICE_NAME="${PG_SERVICE_NAME:-ppea}"

# Valeurs par défaut (modifiable via env)
PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"
PG_DB="${PG_DB:-pea_db}"
PG_USER="${PG_USER:-pea_user}"
PG_PASS="${PG_PASS:-}"   # si vide, on n'écrit pas ~/.pgpass

# --- Fonctions utilitaires ---
say() { printf "\033[1;32m[+] %s\033[0m\n" "$*"; }
warn() { printf "\033[1;33m[!] %s\033[0m\n" "$*"; }
err() { printf "\033[1;31m[✗] %s\033[0m\n" "$*" >&2; }
exists_in_file() { local pat="$1" file="$2"; grep -qE "$pat" "$file" 2>/dev/null || return 1; }

# --- 1) .env.example ---
if [[ ! -f "$ENV_EXAMPLE" ]]; then
  cat > "$ENV_EXAMPLE" <<EOF
# === Choisis UNE méthode ci-dessous ===

# (A) Service pg
#PGSERVICE=${SERVICE_NAME}

# (B) DSN unique (override total)
#DATABASE_URL=service=${SERVICE_NAME}
#DATABASE_URL=postgresql://${PG_USER}:***@${PG_HOST}:${PG_PORT}/${PG_DB}

# (C) Variables PG* (TCP explicite)
#PGHOST=${PG_HOST}
#PGPORT=${PG_PORT}
#PGDATABASE=${PG_DB}
#PGUSER=${PG_USER}
#PGPASSWORD=***   # éviter, privilégier ~/.pgpass

# Options applicatives
APP_TZ=Europe/Paris
ACTIVE_MIN_CNT_1Y=200
VALID_WINDOW_DAYS=5
VALID_MIN_QUOTES_5D=2
YF_SLEEP_SECS=2.0
YF_MAX_RETRIES=3
YF_TIMEOUT_SECS=20
EOF
  say "Créé: $(realpath "$ENV_EXAMPLE")"
else
  say ".env.example déjà présent"
fi

# --- 2) .env (on ne l'écrase pas) ---
if [[ ! -f "$ENV_FILE" ]]; then
  cp "$ENV_EXAMPLE" "$ENV_FILE"
  # par défaut on active la méthode Service
  sed -i "s/#PGSERVICE=${SERVICE_NAME}/PGSERVICE=${SERVICE_NAME}/" "$ENV_FILE"
  say "Créé: $(realpath "$ENV_FILE") (PGSERVICE activé)"
else
  say ".env déjà présent (aucune modification)"
fi

# --- 3) Makefile : charger automatiquement .env ---
if [[ ! -f "$MAKEFILE" ]]; then
  err "Makefile introuvable à $MAKEFILE"
  exit 1
fi

# Ajoute une logique dotenv si absente
if ! exists_in_file 'DOTENV.*set -a' "$MAKEFILE"; then
  # Sauvegarde
  cp "$MAKEFILE" "${MAKEFILE}.bak.$(date +%s)"
  say "Sauvegarde du Makefile -> ${MAKEFILE}.bak.*"

  # Injecte les variables DOTENV et une cible env-print si absentes
  awk '
    BEGIN { inserted=0 }
    /^\.PHONY:/ && inserted==0 {
      print $0
      next
    }
    NR==1 {
      print "SHELL := /bin/bash"
      print "ENV_FILE := .env"
      print "DOTENV := set -a; [ -f $(ENV_FILE) ] && . $(ENV_FILE); set +a"
      print ""
    }
    { print $0 }
    END {
      print ""
      print ".PHONY: env-print"
      print "env-print:"
      print "\t@$(DOTENV); echo \"PGSERVICE=$$PGSERVICE\"; echo \"DATABASE_URL=$$DATABASE_URL\"; echo \"PGHOST=$$PGHOST PGDATABASE=$$PGDATABASE PGUSER=$$PGUSER\""
    }
  ' "$MAKEFILE" > "${MAKEFILE}.new"

  mv "${MAKEFILE}.new" "$MAKEFILE"
  say "Ajout du chargement automatique de .env dans le Makefile"
else
  say "Makefile : bloc DOTENV déjà présent"
fi

# Ajoute/patch la cible validate_equities pour utiliser DOTENV
if ! exists_in_file 'validate_equities:.*' "$MAKEFILE"; then
  cat >> "$MAKEFILE" <<'EOF'

.PHONY: validate_equities
validate_equities:
	@$(DOTENV); python3 -m scripts.validate_equities
EOF
  say "Ajout de la cible validate_equities"
else
  # remplace la ligne de commande par l’appel avec DOTENV si nécessaire
  if ! exists_in_file 'validate_equities:\n\t@\$\(DOTENV\); python3 -m scripts.validate_equities' "$MAKEFILE"; then
    # remplace la première ligne de commande sous la cible
    perl -0777 -pe 's/(^validate_equities:\n)(\t.*\n)/$1\t@$(DOTENV); python3 -m scripts.validate_equities\n/sm' -i "$MAKEFILE" || true
    say "Cible validate_equities mise à jour pour charger .env"
  else
    say "Cible validate_equities déjà configurée"
  fi
fi

# --- 4) ~/.pg_service.conf (service ppea) ---
PG_SERVICE_FILE="${PGSERVICEFILE:-$HOME/.pg_service.conf}"
mkdir -p "$(dirname "$PG_SERVICE_FILE")"
if ! grep -q "^\[${SERVICE_NAME}\]" "$PG_SERVICE_FILE" 2>/dev/null; then
  {
    echo ""
    echo "[${SERVICE_NAME}]"
    echo "host=${PG_HOST}"
    echo "port=${PG_PORT}"
    echo "dbname=${PG_DB}"
    echo "user=${PG_USER}"
    echo "sslmode=prefer"
    echo "options=-c search_path=public"
  } >> "$PG_SERVICE_FILE"
  say "Ajout du service '${SERVICE_NAME}' dans ${PG_SERVICE_FILE}"
else
  say "Service '${SERVICE_NAME}' déjà présent dans ${PG_SERVICE_FILE}"
fi

# --- 5) ~/.pgpass (optionnel si mot de passe fourni) ---
if [[ -n "$PG_PASS" ]]; then
  PGPASS_FILE="$HOME/.pgpass"
  touch "$PGPASS_FILE"
  chmod 600 "$PGPASS_FILE"
  # Supprime les doublons éventuels
  grep -vE "^(${PG_HOST//./\\.}:${PG_PORT}:${PG_DB}:${PG_USER}:)" "$PGPASS_FILE" > "${PGPASS_FILE}.tmp" || true
  mv "${PGPASS_FILE}.tmp" "$PGPASS_FILE"
  # Ajoute l’entrée
  echo "${PG_HOST}:${PG_PORT}:${PG_DB}:${PG_USER}:${PG_PASS}" >> "$PGPASS_FILE"
  chmod 600 "$PGPASS_FILE"
  say "Entrée ajoutée dans ~/.pgpass (host=${PG_HOST} db=${PG_DB} user=${PG_USER})"
else
  warn "PG_PASS non fourni : on n’a pas touché ~/.pgpass (tu peux le gérer manuellement)."
fi

# --- 6) Test de connexion via service (préféré) ---
say "Test psql via service ${SERVICE_NAME}…"
if PGSERVICE="${SERVICE_NAME}" psql -At -c "select current_database(), current_user, current_schema();" >/dev/null; then
  say "Connexion OK (service ${SERVICE_NAME})."
else
  warn "Échec test service. Essaie: PGSERVICE=${SERVICE_NAME} psql -c '\\conninfo'"
fi

# --- 7) Test Makefile ---
say "Test Makefile: env-print"
make -C "$PROJECT_ROOT" env-print || warn "Échec env-print"

say "Test Makefile: validate_equities (dry run)"
make -C "$PROJECT_ROOT" validate_equities || warn "validate_equities a retourné un code d’erreur (regarde les logs)"

say "Setup terminé."
