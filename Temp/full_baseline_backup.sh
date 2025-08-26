# ~/full_baseline_backup.sh
set -euo pipefail

STAMP="$(date +%F_%H%M%S)"
BASE_DIR="$HOME/backups/${STAMP}_baseline"
DB_DIR="$BASE_DIR/db"
SYS_DIR="$BASE_DIR/system"
PRJ_DIR="$BASE_DIR/projects"
SEC_DIR="$BASE_DIR/secrets"
LOG="$BASE_DIR/backup.log"

mkdir -p "$DB_DIR" "$SYS_DIR" "$PRJ_DIR" "$SEC_DIR"
exec > >(tee -a "$LOG") 2>&1

echo "==> Baseline backup @ $STAMP"

# 1) PostgreSQL (logique + métadonnées)
PG_DB="pea_db"
PG_USER="pea_user"
PG_HOST="/var/run/postgresql"   # socket local Ubuntu/Debian
echo "[DB] Dump logique (custom + SQL + globals)"
pg_dump   -U "$PG_USER" -h "$PG_HOST" -Fc -d "$PG_DB" -f "$DB_DIR/${PG_DB}.dump"
pg_dump   -U "$PG_USER" -h "$PG_HOST"     -d "$PG_DB" -f "$DB_DIR/${PG_DB}.sql"
pg_dumpall -U "$PG_USER" -h "$PG_HOST" --globals-only > "$DB_DIR/globals.sql"

echo "[DB] Extensions & version"
psql -U "$PG_USER" -h "$PG_HOST" -d "$PG_DB" -Atc "select extname from pg_extension order by 1" > "$DB_DIR/extensions.txt"
psql -U "$PG_USER" -h "$PG_HOST" -d "$PG_DB" -Atc "select version()" > "$DB_DIR/postgres_version.txt"

echo "[DB] Configs Postgres"
PG_VER="$(psql -V | awk '{print $3}' | cut -d. -f1)"  # ex: 16
# Ajuste si ton instance n'est pas /etc/postgresql/<ver>/main
cp -a "/etc/postgresql/${PG_VER}/main/postgresql.conf" "$DB_DIR/" 2>/dev/null || true
cp -a "/etc/postgresql/${PG_VER}/main/pg_hba.conf"     "$DB_DIR/" 2>/dev/null || true
cp -a "/etc/postgresql/${PG_VER}/main/conf.d"          "$DB_DIR/" 2>/dev/null || true

# 2) Système & jobs
echo "[SYS] Paquets, dépôts APT, cron, systemd"
dpkg --get-selections > "$SYS_DIR/dpkg_selections.txt" || true
apt-mark showmanual   > "$SYS_DIR/apt_manual.txt" || true
cp -a /etc/apt/sources.list* "$SYS_DIR/" 2>/dev/null || true
crontab -l > "$SYS_DIR/crontab.$USER.txt" 2>/dev/null || true
systemctl list-timers --all    > "$SYS_DIR/systemd_timers.txt" || true
systemctl list-unit-files --type=service > "$SYS_DIR/systemd_services.txt" || true
# Copie d’éventuels units persos
sudo bash -c 'tar -C /etc/systemd/system -czf "'"$SYS_DIR"'/systemd_units.tar.gz" . 2>/dev/null' || true

# 3) Secrets & profils dev (permissions restreintes)
echo "[SEC] Secrets/dev profiles"
umask 077
# Clés SSH, Git, Postgres pass, env, Jupyter, etc.
[ -d "$HOME/.ssh" ]     && tar -C "$HOME" -czf "$SEC_DIR/ssh.tar.gz" .ssh
[ -f "$HOME/.pgpass" ]  && cp -a "$HOME/.pgpass" "$SEC_DIR/"
[ -f "$HOME/.gitconfig" ] && cp -a "$HOME/.gitconfig" "$SEC_DIR/"
[ -d "$HOME/.jupyter" ] && tar -C "$HOME" -czf "$SEC_DIR/jupyter.tar.gz" .jupyter
[ -d "$HOME/.ipython" ] && tar -C "$HOME" -czf "$SEC_DIR/ipython.tar.gz" .ipython
# .env de projets (si tu en as)
find "$HOME/Projets" -maxdepth 3 -type f -name ".env" -print0 | tar --null -T - -czf "$SEC_DIR/env_files.tar.gz" 2>/dev/null || true
umask 022

# 4) Projets (Git) + environnements Python
echo "[PRJ] Inventaire Git + requirements"
cd "$HOME/Projets" 2>/dev/null || mkdir -p "$HOME/Projets"
for d in "$HOME/Projets"/*; do
  [ -d "$d/.git" ] || continue
  name="$(basename "$d")"
  out="$PRJ_DIR/$name"
  mkdir -p "$out"
  ( cd "$d"
    echo "== $name =="                       >  "$out/git_info.txt"
    git status --porcelain                  >>  "$out/git_info.txt"
    git remote -v                           >>  "$out/git_info.txt"
    git rev-parse HEAD                      >>  "$out/git_info.txt"
    # Requirements Python (si venv local .venv)
    if [ -d ".venv" ]; then
      . .venv/bin/activate
      pip freeze > "$out/requirements.txt" || true
      deactivate || true
    else
      # fallback: requirements ad hoc (meilleur que rien)
      pip freeze > "$out/requirements_from_system_env.txt" || true
    fi
    # Snapshot léger du code sans le venv/node_modules
    tar --exclude='.git' --exclude='.venv' --exclude='node_modules' \
        -czf "$out/${name}_code_snapshot.tar.gz" .
  )
done

# 5) Checksums et taille
echo "[META] Checksums & taille"
( cd "$BASE_DIR" && find . -type f -print0 | xargs -0 sha256sum ) > "$BASE_DIR/SHA256SUMS.txt" || true
du -sh "$BASE_DIR" | tee "$BASE_DIR/SIZE.txt"

echo "==> Baseline OK: $BASE_DIR"

