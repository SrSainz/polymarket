from strategy import fee_cost_usdc


def test_fee_cost_usdc_uses_bps() -> None:
    assert round(fee_cost_usdc(100.0, 15.6), 4) == 0.1560
    assert fee_cost_usdc(0.0, 15.6) == 0.0
