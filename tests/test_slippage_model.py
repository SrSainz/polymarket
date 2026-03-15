from strategy import BookLevel, OrderBookState, SlippageModelConfig, estimate_taker_slippage_bps


def test_taker_slippage_grows_with_depth() -> None:
    book = OrderBookState(
        token_id="TOKEN",
        asks=[BookLevel(0.51, 10.0), BookLevel(0.52, 100.0), BookLevel(0.53, 100.0)],
        bids=[BookLevel(0.49, 100.0)],
        updated_ts_ms=1,
        best_bid=0.49,
        best_ask=0.51,
    )
    low = estimate_taker_slippage_bps(book, 2.0, "BUY", SlippageModelConfig(taker_depth_levels=3))
    high = estimate_taker_slippage_bps(book, 40.0, "BUY", SlippageModelConfig(taker_depth_levels=3))
    assert low >= 0.0
    assert high > low
