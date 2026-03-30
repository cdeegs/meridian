import pytest

from backend.routes import portfolios


class _FakeResult:
    def __init__(self, rows=None, row=None):
        self._rows = rows or []
        self._row = row

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._row


class _RecordingSession:
    def __init__(self):
        self.calls = []
        self.commit_count = 0

    async def execute(self, statement, params=None):
        self.calls.append((str(statement), params))
        return _FakeResult()

    async def commit(self):
        self.commit_count += 1

    def begin(self):
        raise AssertionError("portfolio handlers should not open a nested transaction")


@pytest.mark.asyncio
async def test_create_portfolio_commits_and_returns_loaded_portfolio(monkeypatch):
    db = _RecordingSession()

    async def fake_get_portfolio_or_404(session, portfolio_id):
        return {
            "id": portfolio_id,
            "name": "Meridian Core",
            "strategy": "Trend",
            "notes": "Smoke",
            "asset_count": 0,
            "allocation_pct": 0.0,
            "assets": [],
        }

    monkeypatch.setattr(portfolios, "_get_portfolio_or_404", fake_get_portfolio_or_404)

    payload = portfolios.PortfolioCreate(name="Meridian Core", strategy="Trend", notes="Smoke")
    response = await portfolios.create_portfolio(payload, db=db)

    assert response["name"] == "Meridian Core"
    assert db.commit_count == 1
    assert any("INSERT INTO portfolios" in sql for sql, _ in db.calls)


@pytest.mark.asyncio
async def test_update_portfolio_commits_after_lookup_without_nested_begin(monkeypatch):
    db = _RecordingSession()
    lookups = {"count": 0}

    async def fake_get_portfolio_or_404(session, portfolio_id):
        lookups["count"] += 1
        if lookups["count"] == 1:
            return {
                "id": portfolio_id,
                "name": "Meridian Core",
                "strategy": "Old strategy",
                "notes": "Old notes",
                "asset_count": 0,
                "allocation_pct": 0.0,
                "assets": [],
            }
        return {
            "id": portfolio_id,
            "name": "Meridian Core",
            "strategy": "Updated strategy",
            "notes": "Updated notes",
            "asset_count": 0,
            "allocation_pct": 0.0,
            "assets": [],
        }

    monkeypatch.setattr(portfolios, "_get_portfolio_or_404", fake_get_portfolio_or_404)

    payload = portfolios.PortfolioUpdate(strategy="Updated strategy", notes="Updated notes")
    response = await portfolios.update_portfolio("portfolio-1", payload, db=db)

    assert response["strategy"] == "Updated strategy"
    assert db.commit_count == 1
    assert any("UPDATE portfolios" in sql for sql, _ in db.calls)


@pytest.mark.asyncio
async def test_asset_mutations_commit_after_lookup_without_nested_begin(monkeypatch):
    db = _RecordingSession()

    async def fake_get_portfolio_or_404(session, portfolio_id):
        return {
            "id": portfolio_id,
            "name": "Meridian Swing",
            "strategy": "Momentum",
            "notes": None,
            "asset_count": 1,
            "allocation_pct": 25.0,
            "assets": [
                {
                    "id": "asset-1",
                    "symbol": "BTC-USD",
                    "asset_type": "crypto",
                    "allocation_pct": 25.0,
                    "strategy": "Core",
                    "notes": None,
                    "created_at": None,
                }
            ],
        }

    async def fake_get_portfolio_asset_or_404(session, portfolio_id, asset_id):
        return {
            "id": asset_id,
            "portfolio_id": portfolio_id,
            "symbol": "BTC-USD",
            "asset_type": "crypto",
            "allocation_pct": 25.0,
            "strategy": "Core",
            "notes": None,
            "created_at": None,
        }

    monkeypatch.setattr(portfolios, "_get_portfolio_or_404", fake_get_portfolio_or_404)
    monkeypatch.setattr(portfolios, "_get_portfolio_asset_or_404", fake_get_portfolio_asset_or_404)

    create_payload = portfolios.PortfolioAssetCreate(
        symbol="BTC-USD",
        asset_type="crypto",
        allocation_pct=25.0,
        strategy="Core",
        notes="Starter position",
    )
    added = await portfolios.add_portfolio_asset("portfolio-1", create_payload, db=db)

    update_payload = portfolios.PortfolioAssetUpdate(strategy="Updated core")
    updated = await portfolios.update_portfolio_asset("portfolio-1", "asset-1", update_payload, db=db)
    deleted = await portfolios.delete_portfolio_asset("portfolio-1", "asset-1", db=db)

    assert added["asset_count"] == 1
    assert updated["asset_count"] == 1
    assert deleted["deleted_id"] == "asset-1"
    assert db.commit_count == 3
    assert any("INSERT INTO portfolio_assets" in sql for sql, _ in db.calls)
    assert any("UPDATE portfolio_assets" in sql for sql, _ in db.calls)
    assert any("DELETE FROM portfolio_assets" in sql for sql, _ in db.calls)
