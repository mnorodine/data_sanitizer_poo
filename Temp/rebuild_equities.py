#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rebuild the `equities` table from the canonical CSV using psycopg (v3).
Designed for the provided scripts/import_equities.py which reads datas/equities.csv and uses scripts.config.get_pg_connection().
"""
import argparse
import shlex
import subprocess
import sys
from pathlib import Path

import csv
import psycopg  # psycopg v3

def run(cmd, cwd=None, timeout=1200):
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    proc = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=timeout)
    return proc.returncode, proc.stdout

def export_table_to_csv(conn, fq_table, out_path):
    with conn.cursor() as cur, open(out_path, "w", newline="", encoding="utf-8") as f:
        cur.copy(f"COPY {fq_table} TO STDOUT WITH CSV HEADER", f)
    return out_path

def count_table(conn, fq_table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {fq_table}")
        return cur.fetchone()[0]

def get_columns(conn, fq_table):
    schema, table = fq_table.split(".", 1)
    q = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        return [r[0] for r in cur.fetchall()]

def check_required_columns(conn, fq_table, required):
    cols = set(get_columns(conn, fq_table))
    missing = [c for c in required if c not in cols]
    return missing

def check_no_nulls(conn, fq_table, cols):
    res = {}
    with conn.cursor() as cur:
        for c in cols:
            cur.execute(f"SELECT COUNT(*) FROM {fq_table} WHERE {c} IS NULL")
            res[c] = cur.fetchone()[0]
    return res

def csv_count_rows(csv_path):
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=';')
        # Skip first 4 header lines like import_equities.py
        for _ in range(4):
            next(reader, None)
        n = 0
        for _ in reader:
            n += 1
    return n

def main():
    p = argparse.ArgumentParser(description="Vider et reconstruire la table equities depuis le CSV d'origine, avec validations (psycopg3).")
    p.add_argument("--project-root", required=True, help="Racine du projet data_sanitizer (contenant scripts/ et datas/)")
    p.add_argument("--db-url", required=True, help="URL de connexion PostgreSQL (pour TRUNCATE/validations), ex: postgresql://user:pass@host:5432/db")
    p.add_argument("--schema", default="public", help="Schéma de la table (defaut: public)")
    p.add_argument("--table", default="equities", help="Nom de la table (defaut: equities)")
    p.add_argument("--backup-csv", default=None, help="Chemin pour exporter un backup de la table avant TRUNCATE (optionnel)")
    p.add_argument("--required-cols", default="isin,symbol,name,market,currency", help="Colonnes obligatoires à vérifier (séparées par des virgules)")
    p.add_argument("--cascade", action="store_true", help="Utiliser TRUNCATE ... CASCADE")
    p.add_argument("--yes", action="store_true", help="Ne pas demander de confirmation interactive")
    args = p.parse_args()

    project_root = Path(args.project_root).expanduser().resolve()
    import_cmd = "python scripts/import_equities.py"

    fq_table = f"{args.schema}.{args.table}"
    try:
        conn = psycopg.connect(args.db_url)
        conn.autocommit = False
    except Exception as e:
        print(f"[ERREUR] Connexion DB: {e}", file=sys.stderr)
        sys.exit(2)

    # Pre-count and optional backup
    try:
        before_count = count_table(conn, fq_table)
        print(f"[INFO] Lignes avant: {before_count} dans {fq_table}")
    except Exception as e:
        print(f"[ERREUR] Lecture du comptage initial: {e}", file=sys.stderr)
        conn.close()
        sys.exit(2)

    if args.backup_csv:
        out_path = Path(args.backup_csv).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            export_table_to_csv(conn, fq_table, str(out_path))
            print(f"[OK] Backup exporté vers: {out_path}")
        except Exception as e:
            print(f"[ERREUR] Export backup: {e}", file=sys.stderr)
            conn.close()
            sys.exit(2)

    if not args.yes:
        resp = input(f"CONFIRMER la vidange de {fq_table} ? (oui/N) ").strip().lower()
        if resp not in ("o", "oui", "y", "yes"):
            print("[ABANDON] Aucune modification effectuée.")
            conn.close()
            sys.exit(0)

    # TRUNCATE
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {fq_table} RESTART IDENTITY {'CASCADE' if args.cascade else ''}")
        conn.commit()
        print(f"[OK] Table {fq_table} vidée.")
    except Exception as e:
        conn.rollback()
        print(f"[ERREUR] TRUNCATE: {e}", file=sys.stderr)
        conn.close()
        sys.exit(2)

    # Import
    print(f"[INFO] Import via: {import_cmd} (cwd={project_root})")
    rc, out = run(import_cmd, cwd=str(project_root))
    print(out)
    if rc != 0:
        print(f"[ERREUR] La commande d'import a échoué (code {rc}).", file=sys.stderr)
        conn.close()
        sys.exit(2)

    # Validations
    try:
        after_count = count_table(conn, fq_table)
        print(f"[INFO] Lignes après import: {after_count}")
    except Exception as e:
        print(f"[ERREUR] Comptage après import: {e}", file=sys.stderr)
        conn.close()
        sys.exit(2)

    req_cols = [c.strip() for c in args.required_cols.split(",") if c.strip()]
    missing_cols = check_required_columns(conn, fq_table, req_cols)
    if missing_cols:
        print(f"[ERREUR] Colonnes manquantes dans {fq_table}: {', '.join(missing_cols)}", file=sys.stderr)
        conn.close()
        sys.exit(2)

    nulls = check_no_nulls(conn, fq_table, req_cols)
    any_nulls = any(v > 0 for v in nulls.values())
    for c, n in nulls.items():
        print(f"[VALIDATION] {c}: {n} valeurs NULL")

    # Compare with CSV rows
    csv_rows = None
    csv_path = project_root / "datas" / "equities.csv"
    if csv_path.exists():
        try:
            csv_rows = csv_count_rows(csv_path)
            print(f"[INFO] Lignes CSV (hors 4 en-têtes): {csv_rows}")
        except Exception as e:
            print(f"[AVERTISSEMENT] Impossible de compter les lignes du CSV: {e}")

    print("\n===== SYNTHÈSE =====")
    print(f"Table: {fq_table}")
    print(f"Avant: {before_count} lignes")
    print(f"Après: {after_count} lignes")
    print(f"Colonnes obligatoires: {', '.join(req_cols)}")
    if csv_rows is not None:
        print(f"CSV (données): {csv_rows} lignes")
    if any_nulls:
        print("[ÉCHEC] Des NULLs ont été détectés dans les colonnes obligatoires.")
        conn.close()
        sys.exit(2)

    print("[OK] Reconstruction terminée sans NULLs sur les colonnes obligatoires.")
    conn.close()

if __name__ == "__main__":
    main()
