
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Orchestrateur de migration — equities

Objectif
- Vider proprement `public.equities` (optionnel backup)
- Exécuter `python scripts/import_equities.py` (Import #1)
- Vérifier colonnes clés et absence de NULL sur (isin, symbol)
- Ré-exécuter l'import (Import #2) pour tester l'idempotence (delta attendu = 0)

Usage
python rebuild_equities_truncate_then_import.py \  --project-root ~/Projets/data_sanitizer \  --db-url postgresql://USER:PASS@HOST:5432/DBNAME \  --schema public --table equities \  --backup-csv ./_backup_equities_before_rebuild.csv \  --yes

Dépendances
- psycopg v3 : pip install psycopg[binary]
"""

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

# psycopg (v3)
try:
    import psycopg
except Exception as e:
    print("[ERREUR] psycopg (v3) requis. Installez avec 'pip install psycopg[binary]'.", file=sys.stderr)
    raise

REQUIRED_COLS = ["isin", "symbol", "name", "market", "currency"]

def run(cmd, cwd=None, timeout=1800):
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    proc = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout)
    return proc.returncode, proc.stdout

def count_rows(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        return cur.fetchone()[0]

def export_table(conn, schema, table, out_csv):
    with conn.cursor() as cur, open(out_csv, "w", encoding="utf-8", newline="") as f:
        cur.copy(f"COPY {schema}.{table} TO STDOUT WITH CSV HEADER", f)

def check_columns_exist(conn, schema, table, required):
    q = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        cols = {r[0] for r in cur.fetchall()}
    missing = [c for c in required if c not in cols]
    return missing

def null_counts(conn, schema, table, cols):
    res = {}
    with conn.cursor() as cur:
        for c in cols:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table} WHERE {c} IS NULL")
            res[c] = cur.fetchone()[0]
    return res

def main():
    ap = argparse.ArgumentParser(description="TRUNCATE -> Import #1 -> Checks -> Import #2 (idempotence).")
    ap.add_argument("--project-root", required=True, help="Racine du projet data_sanitizer (contient scripts/)")
    ap.add_argument("--db-url", required=True, help="URL PostgreSQL ex: postgresql://user:pass@host:5432/dbname")
    ap.add_argument("--schema", default="public", help="Schéma (defaut: public)")
    ap.add_argument("--table", default="equities", help="Table (defaut: equities)")
    ap.add_argument("--backup-csv", default=None, help="Chemin pour exporter un backup avant TRUNCATE (optionnel)")
    ap.add_argument("--cascade", action="store_true", help="TRUNCATE ... CASCADE (utiliser si d'autres tables référencent)")
    ap.add_argument("--yes", action="store_true", help="Ne pas demander de confirmation")
    args = ap.parse_args()

    project_root = Path(args.project_root).expanduser().resolve()
    if not project_root.exists():
        print(f"[ERREUR] project-root introuvable: {project_root}", file=sys.stderr)
        sys.exit(2)

    # Connexion DB
    try:
        conn = psycopg.connect(args.db_url, autocommit=False)
    except Exception as e:
        print(f"[ERREUR] Connexion DB: {e}", file=sys.stderr)
        sys.exit(2)

    schema = args.schema
    table = args.table

    # Backup optionnel
    if args.backup_csv:
        out = Path(args.backup_csv).expanduser().resolve()
        out.parent.mkdir(parents=True, exist_ok=True)
        try:
            export_table(conn, schema, table, str(out))
            print(f"[OK] Backup exporté: {out}")
        except Exception as e:
            print(f"[ERREUR] Export backup: {e}", file=sys.stderr)
            conn.close()
            sys.exit(2)

    # Confirmation
    if not args.yes:
        resp = input(f"Confirmer TRUNCATE {schema}.{table}{' CASCADE' if args.cascade else ''} ? (oui/N) ").strip().lower()
        if resp not in ("o", "oui", "y", "yes"):
            print("[ABANDON] Aucune modification.")
            conn.close()
            sys.exit(0)

    # TRUNCATE
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {schema}.{table} RESTART IDENTITY {'CASCADE' if args.cascade else ''}")
        conn.commit()
        print(f"[OK] Table vidée: {schema}.{table}")
    except Exception as e:
        conn.rollback()
        print(f"[ERREUR] TRUNCATE: {e}", file=sys.stderr)
        conn.close()
        sys.exit(2)

    # Vérif colonnes
    missing = check_columns_exist(conn, schema, table, REQUIRED_COLS)
    if missing:
        print(f"[ERREUR] Colonnes manquantes: {', '.join(missing)}", file=sys.stderr)
        conn.close()
        sys.exit(2)

    before = count_rows(conn, schema, table)
    print(f"[INFO] Lignes avant import #1: {before}")

    # Import #1
    print("[INFO] Import #1 — exécution de: python scripts/import_equities.py")
    rc, out = run("python scripts/import_equities.py", cwd=str(project_root))
    print(out)
    if rc != 0:
        print(f"[ERREUR] Import #1 a échoué (rc={rc}).", file=sys.stderr)
        conn.close()
        sys.exit(2)

    after1 = count_rows(conn, schema, table)
    print(f"[INFO] Lignes après import #1: {after1} (delta: {after1 - before})")

    # Contrôle NULL clés
    nulls = null_counts(conn, schema, table, ["isin","symbol"])
    for c, n in nulls.items():
        print(f"[VALIDATION] {c}: {n} NULL")
    if any(n > 0 for n in nulls.values()):
        print("[ERREUR] NULL détecté dans les colonnes clés (isin/symbol).", file=sys.stderr)
        conn.close()
        sys.exit(2)

    # Import #2 (idempotence)
    print("[INFO] Import #2 — re-exécution pour tester l'idempotence")
    rc, out = run("python scripts/import_equities.py", cwd=str(project_root))
    print(out)
    if rc != 0:
        print(f"[ERREUR] Import #2 a échoué (rc={rc}).", file=sys.stderr)
        conn.close()
        sys.exit(2)

    after2 = count_rows(conn, schema, table)
    print(f"[INFO] Lignes après import #2: {after2} (delta: {after2 - after1})")

    print("\n===== SYNTHÈSE =====")
    print(f"Avant: {before} | Après #1: {after1} | Après #2: {after2} | Delta #2: {after2 - after1}")
    if after2 == after1:
        print("[OK] Idempotence vérifiée: la 2e exécution n'ajoute rien.")
    else:
        print("[ATTENTION] La 2e exécution a modifié le nombre de lignes. Vérifier la logique d'upsert dans l'importeur.")

    conn.close()

if __name__ == "__main__":
    main()
