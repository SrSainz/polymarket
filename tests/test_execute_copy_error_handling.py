from app.services.execute_copy import _is_invalid_signature_error, _is_no_match_error


def test_no_match_error_detection() -> None:
    assert _is_no_match_error("exception: no match")
    assert _is_no_match_error("No Match in order book".lower())


def test_invalid_signature_error_detection() -> None:
    assert _is_invalid_signature_error("PolyApiException: invalid signature")
    assert _is_invalid_signature_error("error: Unauthorized/Invalid api key".lower())
