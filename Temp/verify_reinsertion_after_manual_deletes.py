
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Vérifie que les enregistrements supprimés manuellement ont bien été réinsérés
après une ré-exécution de scripts/import_equities.py.

Étapes typiques :
1) (après un import initial OK) supprimer quelques lignes manuellement
2) PRE : capturer l'état des clés manquantes depuis le CSV de référence
   python verify_reinsertion_after_manual_deletes.py --db-url ... --stage pre --csv <path> --snapshot _missing.json
3) lancer l'importeur seul (depuis la racine du projet) :
   (cd ~/Projets/data_sanitizer && python scripts/import_equities.py)
4) POST : vérifier la réinsertion
   python verify_reinsertion_after_manual_deletes.py --db-url ... --stage post --csv <path> --snapshot _missing.json

Code de sortie :
- PRE  : 0 (toujours)
- POST : 0 si tout a été réinséré, 2 sinon
'''
import argparse
import csv
import json
import sys
from pathlib import Path

# psycopg v3
try:
    import psycopg
except Exception as e:
    print("[ERREUR] psycopg (v3) requis. Installez avec 'pip install psycopg[binary]'.", file=sys.stderr)
    raise

CSV_SKIP_HEADER_LINES = 4
CSV_DELIMITER = ';'

def parse_csv_keys(csv_path):
    keys = []
    total_rows = 0
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=CSV_DELIMITER)
        for i, row in enumerate(reader):
            if i < CSV_SKIP_HEADER_LINES:
                continue
            total_rows += 1
            try:
                isin = (row[1] or "").strip().upper()  # colonne 2
                symbol = (row[2] or "").strip().upper()  # colonne 3
                if not isin or not symbol or symbol == "-":
                    continue
                keys.append((isin, symbol))
            except IndexError:
                continue
    return set(keys), total_rows

def fetch_db_keys(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT isin, symbol FROM {schema}.{table}")
        return set((r[0], r[1]) for r in cur.fetchall())

def main():
    p = argparse.ArgumentParser(description="Vérifier la réinsertion après suppressions manuelles (equities).")
    p.add_argument("--db-url", required=True, help="URL PostgreSQL ex: postgresql://user:pass@host:5432/dbname")
    p.add_argument("--schema", default="public", help="Schéma (defaut: public)")
    p.add_argument("--table", default="equities", help="Table (defaut: equities)")
    p.add_argument("--csv", required=True, help="CSV de référence (datas/equities.csv)")
    p.add_argument("--stage", choices=["pre", "post"], required=True, help="Étape: pre ou post")
    p.add_argument("--snapshot", required=True, help="Chemin du snapshot JSON à écrire/lire")
    args = p.parse_args()

    csv_path = Path(args.csv).expanduser().resolve()
    if not csv_path.exists():
        print(f"[ERREUR] CSV introuvable: {csv_path}", file=sys.stderr)
        sys.exit(2)

    try:
        conn = psycopg.connect(args.db_url, autocommit=True)
    except Exception as e:
        print(f"[ERREUR] Connexion DB: {e}", file=sys.stderr)
        sys.exit(2)

    expected_keys, total_rows = parse_csv_keys(csv_path)
    db_keys = fetch_db_keys(conn, args.schema, args.table)

    if args.stage == "pre":
        missing = sorted(list(expected_keys - db_keys))
        payload = {
            "csv_total_rows": total_rows,
            "expected_keys": len(expected_keys),
            "db_keys": len(db_keys),
            "missing_before": missing,
        }
        Path(args.snapshot).write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"[PRE] Lignes CSV (données): {total_rows}  |  Clés attendues: {len(expected_keys)}  |  Présentes en base: {len(db_keys)}")
        print(f"[PRE] Manquantes (par rapport au CSV): {len(missing)} (snapshot enregistré: {args.snapshot})")
        sys.exit(0)

    if args.stage == "post":
        snap_path = Path(args.snapshot)
        if not snap_path.exists():
            print(f"[ERREUR] Snapshot introuvable: {snap_path}. Relancez d'abord avec --stage pre.", file=sys.stderr)
            sys.exit(2)
        snap = json.loads(snap_path.read_text(encoding="utf-8"))
        before_missing = set(tuple(x) for x in snap.get("missing_before", []))
        still_missing = sorted(list(before_missing - db_keys))
        recovered = sorted(list(before_missing & db_keys))
        print(f"[POST] Réinsérées: {len(recovered)}  |  Encore manquantes: {len(still_missing)}")
        if still_missing:
            print("[POST] Exemples encore manquants (jusqu'à 10):")
            for pair in still_missing[:10]:
                print(f"  - {pair[0]} | {pair[1]}")
            sys.exit(2)
        else:
            print("[OK] Toutes les lignes supprimées ont été réinsérées par l'import.")
            sys.exit(0)

if __name__ == "__main__":
    main()
