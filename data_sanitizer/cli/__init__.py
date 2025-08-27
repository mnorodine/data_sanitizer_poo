from __future__ import annotations

import typer

app = typer.Typer(help="CLI data_sanitizer (POO).")

# ---- Sous-commandes standard -------------------------------------------------

@app.command("check-db")
def check_db() -> None:
    """
    Vérifie la connexion à la base et sort avec code 0/1.
    """
    # TODO: branche ta logique réelle ici (adapters/services).
    # Exemple simplifié :
    import os
    host = os.getenv("DS_PGHOST", "127.0.0.1")
    db   = os.getenv("DS_PGDATABASE", "pea_db")
    user = os.getenv("DS_PGUSER", "pea_user")
    typer.echo(f"✅ DB OK → host={host} dbname={db} user={user}")

@app.command("update-prices")
def update_prices(
    since: str | None = typer.Option(None, "--since", help="YYYY-MM-DD"),
    limit: int | None = typer.Option(None, "--limit", min=1, help="Limiter le nombre de titres"),
    only: list[str] = typer.Option(None, "--only", "-o", help="ISIN/SYMBOL (répétable)"),
    sleep: float = typer.Option(0.0, "--sleep", help="Pause en secondes entre tickers"),
    write: bool = typer.Option(False, "--write", help="Écrire en base (sinon dry-run)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Forcer le dry-run (prioritaire sur --write)"),
) -> None:
    """
    Télécharge les prix (dry-run par défaut).
    """
    effective_write = bool(write and not dry_run)
    # TODO: appeler ta vraie couche service ici.
    # Pour l’instant on affiche un résumé d’exécution :
    mode = "write" if effective_write else "dry-run"
    typer.echo(f"⏳ update-prices: since={since} limit={limit} only={only} sleep={sleep} mode={mode}")
    # typer.echo("✅ Terminé")

@app.command("doctor")
def doctor() -> None:
    """
    Diagnostic rapide de l'environnement (python, pip, imports).
    """
    import sys
    import shutil
    python = sys.executable
    pip = shutil.which("pip") or "pip"
    typer.echo("== Python ==")
    typer.echo(python)
    typer.echo(sys.version.split()[0])
    typer.echo("== Pip ==")
    typer.echo(pip)
    # Import simple
    try:
        import data_sanitizer as _  # noqa: F401
        typer.echo("== Paquet import ==\nOK import data_sanitizer")
    except Exception as e:  # pragma: no cover
        typer.echo(f"== Paquet import ==\nERREUR import data_sanitizer: {e}")
    # pip check (meilleur effort)
    pip_check = shutil.which("pip")
    if pip_check:
        import subprocess
        typer.echo(">> pip check")
        subprocess.run([pip_check, "check"], check=False)
