from __future__ import annotations
import typer
from datetime import datetime
from typing import Optional, List

from data_sanitizer.config import get_settings
from data_sanitizer.adapters.db.equities_repo_pg import EquitiesRepoPg
from data_sanitizer.adapters.db.prices_repo_pg import PricesRepoPg
from data_sanitizer.adapters.providers.yfinance_client import YFinanceClient
from data_sanitizer.adapters.providers.ticker_resolver_default import DefaultTickerResolver
from data_sanitizer.services.update_prices import UpdatePricesService

# App parent (toujours un "group")
app = typer.Typer(add_completion=False, help="CLI data_sanitizer (POO).")

# Sous-app pour la commande update-prices (garantit l'affichage 'Commands')
update_app = typer.Typer(help="Met à jour les prix depuis le provider")

@update_app.callback(invoke_without_command=True)
def update_prices(
    since: Optional[str] = typer.Option(None, help="YYYY-MM-DD (par défaut: dernière date connue)"),
    limit: Optional[int] = typer.Option(None, help="Limiter le nombre de lignes cibles"),
    only: Optional[List[str]] = typer.Option(None, help="Limiter à certains symbols"),
    sleep: float = typer.Option(0.0, help="Pause (s) entre requêtes"),
    dry_run: bool = typer.Option(False, help="Ne pas écrire en base"),
):
    s = get_settings()
    equities = EquitiesRepoPg()
    prices = PricesRepoPg()
    market = YFinanceClient()
    resolver = DefaultTickerResolver()
    service = UpdatePricesService(equities, prices, market, resolver, pause_s=s.request_pause_s)
    _since = datetime.strptime(since, "%Y-%m-%d").date() if since else None
    service.run(since=_since, limit=limit, only=only, sleep=sleep, dry_run=dry_run)

    res = service.run(since=_since, limit=limit, only=only, sleep=sleep, dry_run=dry_run)
    try:
        ok, skip, err = res  # si tu fais retourner un tuple
        typer.echo(f"[update-prices] ok={ok} skip={skip} err={err} dry_run={dry_run}")
    except Exception:
        # si run() ne retourne rien, affiche juste un message de fin
        typer.echo(f"[update-prices] terminé (dry_run={dry_run})")



# Monte la sous-commande sous le nom 'update-prices'
app.add_typer(update_app, name="update-prices")

def main():
    app()

if __name__ == "__main__":
    main()
