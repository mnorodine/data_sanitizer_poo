# ==== PostgreSQL Backup/Restore ====
# Configurable via environment or .env (export before calling `make`)
# Example:
#   export DB_HOST=localhost
#   export DB_PORT=5432
#   export DB_NAME=pea_db
#   export DB_USER=postgres
#   export PGPASSWORD=secret   # or use ~/.pgpass instead of PGPASSWORD

DB_HOST ?= localhost
DB_PORT ?= 5432
DB_NAME ?= pea_db
DB_USER ?= postgres

BACKUP_DIR ?= backups
DATE := $(shell date +%Y%m%d-%H%M%S)

# Common dump options
DUMP_CONN = -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER)
DUMP_DB   = $(DUMP_CONN) -d $(DB_NAME)

# Ensure backup directory exists
$(BACKUP_DIR):
	@mkdir -p $(BACKUP_DIR)

## backup: Full backup (schema + data) in custom format (.dump)
backup: $(BACKUP_DIR)
	pg_dump $(DUMP_DB) -Fc -f $(BACKUP_DIR)/$(DB_NAME)_full_$(DATE).dump
	@echo "✅ Backup full créé: $(BACKUP_DIR)/$(DB_NAME)_full_$(DATE).dump"

## backup-schema: Schema-only backup (.dump)
backup-schema: $(BACKUP_DIR)
	pg_dump $(DUMP_DB) -s -Fc -f $(BACKUP_DIR)/$(DB_NAME)_schema_$(DATE).dump
	@echo "✅ Backup schéma créé: $(BACKUP_DIR)/$(DB_NAME)_schema_$(DATE).dump"

## backup-data: Data-only backup (.dump)
backup-data: $(BACKUP_DIR)
	pg_dump $(DUMP_DB) -a -Fc -f $(BACKUP_DIR)/$(DB_NAME)_data_$(DATE).dump
	@echo "✅ Backup data créé: $(BACKUP_DIR)/$(DB_NAME)_data_$(DATE).dump"

## backup-plain: Full backup en SQL lisible (.sql.gz)
backup-plain: $(BACKUP_DIR)
	pg_dump $(DUMP_DB) -f $(BACKUP_DIR)/$(DB_NAME)_full_$(DATE).sql
	gzip -f $(BACKUP_DIR)/$(DB_NAME)_full_$(DATE).sql
	@echo "✅ Backup SQL créé: $(BACKUP_DIR)/$(DB_NAME)_full_$(DATE).sql.gz"

## backup-globals: Roles & objects globaux (utilise pg_dumpall)
backup-globals: $(BACKUP_DIR)
	pg_dumpall $(DUMP_CONN) --globals-only > $(BACKUP_DIR)/globals_$(DATE).sql
	@echo "✅ Backup globals créé: $(BACKUP_DIR)/globals_$(DATE).sql"

## restore-full FILE=backups/pea_db_full_YYYYmmdd-HHMMSS.dump
restore-full:
	@if [ -z "$(FILE)" ]; then echo "Usage: make restore-full FILE=path/to/file.dump"; exit 1; fi
	pg_restore $(DUMP_DB) -c --if-exists $(FILE)

## restore-plain FILE=backups/pea_db_full_YYYYmmdd-HHMMSS.sql.gz
restore-plain:
	@if [ -z "$(FILE)" ]; then echo "Usage: make restore-plain FILE=path/to/file.sql[.gz]"; exit 1; fi
	@if echo "$(FILE)" | grep -q '\.gz$$'; then \
		gunzip -c "$(FILE)" | psql $(DUMP_DB); \
	else \
		psql $(DUMP_DB) -f "$(FILE)"; \
	fi

## backups-list: Liste des backups existants
backups-list:
	@ls -lh $(BACKUP_DIR) || true

## backups-clean RETENTION_DAYS=14: Supprime les backups plus vieux que N jours
RETENTION_DAYS ?= 14
backups-clean:
	@find $(BACKUP_DIR) -type f -mtime +$(RETENTION_DAYS) -print -delete 2>/dev/null || true
	@echo "🧹 Backups de plus de $(RETENTION_DAYS) jours supprimés."

.PHONY: backup backup-schema backup-data backup-plain backup-globals restore-full restore-plain backups-list backups-clean
