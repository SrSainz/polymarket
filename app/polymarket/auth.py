from __future__ import annotations

from app.settings import EnvSettings


def build_authenticated_clob_client(env: EnvSettings):
    """
    Builds an authenticated py-clob-client instance.

    This is used only when LIVE_TRADING=true.
    Requires installing py-clob-client separately.
    """
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds
    except ImportError as error:
        raise RuntimeError(
            "py-clob-client is required for live trading. Install it manually before running live mode."
        ) from error

    if not env.polymarket_private_key:
        raise RuntimeError("POLYMARKET_PRIVATE_KEY is missing.")

    client = ClobClient(
        env.clob_host,
        key=env.polymarket_private_key,
        chain_id=env.polymarket_chain_id,
        signature_type=env.polymarket_signature_type,
        funder=env.polymarket_funder or None,
    )

    if env.polymarket_api_key and env.polymarket_api_secret and env.polymarket_api_passphrase:
        creds = ApiCreds(
            api_key=env.polymarket_api_key,
            api_secret=env.polymarket_api_secret,
            api_passphrase=env.polymarket_api_passphrase,
        )
    else:
        creds = client.create_or_derive_api_creds()

    client.set_api_creds(creds)
    return client
