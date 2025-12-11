def test_imports():
    # Very small smoke-test to ensure package structure imports correctly.
    import backend.api.main as api_main
    import backend.common.models as models
    import backend.ingestion_engine.kalshi_http as kalshi_http
    import backend.ingestion_engine.auto_ingest as auto_ingest
    import backend.execution_engine.order_manager as order_manager
    import backend.execution_engine.risk_manager as risk_manager

    assert hasattr(api_main, "app")
    assert hasattr(models, "MarketTick")
    assert hasattr(kalshi_http, "KalshiHttpClient")
    assert hasattr(auto_ingest, "start_ingestion")
    assert hasattr(order_manager, "OrderManager")
    assert hasattr(risk_manager, "RiskManager")
