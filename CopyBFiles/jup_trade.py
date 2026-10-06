"""
jup_trade.py — buy/sell ANY Solana token via Jupiter aggregator, signed locally.

Why Jupiter instead of PumpPortal here: whales trade on Raydium, Meteora,
Orca, pump AMM, etc. Jupiter routes across all of them, so whatever the
tracked wallet buys, we can mirror it.

The swap transaction is built by Jupiter's API but SIGNED ON YOUR MACHINE
with the burner wallet key. The key never leaves your server.

Requires in .env:  BURNER_PRIVATE_KEY, HELIUS_API_KEY
"""

import base64
import os

import requests
from dotenv import load_dotenv
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solders.commitment_config import CommitmentLevel
from solders.rpc.requests import SendVersionedTransaction
from solders.rpc.config import RpcSendTransactionConfig

load_dotenv()

SOL_MINT   = "So11111111111111111111111111111111111111112"  # wrapped SOL
JUP_API    = "https://lite-api.jup.ag/swap/v1"               # free tier, no key
HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={os.environ['HELIUS_API_KEY']}"

KEYPAIR = Keypair.from_base58_string(os.environ["BURNER_PRIVATE_KEY"])
PUBKEY  = str(KEYPAIR.pubkey())


def _rpc(method: str, params: list):
    """Plain JSON-RPC call to Helius."""
    r = requests.post(HELIUS_RPC, json={"jsonrpc": "2.0", "id": 1,
                                        "method": method, "params": params},
                      timeout=15)
    r.raise_for_status()
    out = r.json()
    if "error" in out:
        raise RuntimeError(out["error"])
    return out["result"]


def get_sol_balance() -> float:
    """Burner wallet SOL balance (used for %-of-wallet position sizing)."""
    return _rpc("getBalance", [PUBKEY])["value"] / 1e9


def get_token_balance(mint: str) -> int:
    """Raw token amount (base units) we hold of `mint`. 0 if none."""
    res = _rpc("getTokenAccountsByOwner",
               [PUBKEY, {"mint": mint}, {"encoding": "jsonParsed"}])
    total = 0
    for acc in res.get("value", []):
        amt = acc["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]
        total += int(amt)
    return total


def _swap(input_mint: str, output_mint: str, amount_raw: int,
          slippage_bps: int = 300) -> tuple[str, dict]:
    """
    Quote + build + sign + send a swap. Returns (tx_signature, quote).
    slippage_bps: 300 = 3%. Memecoins may need 500-1000 to land.
    """
    # 1) Quote — best route across all DEXes.
    q = requests.get(f"{JUP_API}/quote", params={
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": amount_raw,
        "slippageBps": slippage_bps,
    }, timeout=15)
    q.raise_for_status()
    quote = q.json()
    if "error" in quote or not quote.get("outAmount"):
        raise RuntimeError(f"No route: {quote}")

    # 2) Ask Jupiter to build the transaction for OUR public key.
    s = requests.post(f"{JUP_API}/swap", json={
        "quoteResponse": quote,
        "userPublicKey": PUBKEY,
        "wrapAndUnwrapSol": True,   # handle SOL <-> wSOL automatically
        "dynamicComputeUnitLimit": True,
        "prioritizationFeeLamports": {   # tip so the tx lands during hype
            "priorityLevelWithMaxLamports": {
                "priorityLevel": "high",
                "maxLamports": 2_000_000,   # hard cap: 0.002 SOL
            }
        },
    }, timeout=15)
    s.raise_for_status()
    swap = s.json()
    if "swapTransaction" not in swap:
        raise RuntimeError(f"Swap build failed: {swap}")

    # 3) Sign locally with the burner key.
    tx = VersionedTransaction.from_bytes(base64.b64decode(swap["swapTransaction"]))
    signed = VersionedTransaction(tx.message, [KEYPAIR])

    # 4) Broadcast via Helius.
    cfg = RpcSendTransactionConfig(preflight_commitment=CommitmentLevel.Confirmed)
    payload = SendVersionedTransaction(signed, cfg).to_json()
    r = requests.post(HELIUS_RPC, headers={"Content-Type": "application/json"},
                      data=payload, timeout=15)
    r.raise_for_status()
    out = r.json()
    if "error" in out:
        raise RuntimeError(out["error"])
    return out["result"], quote


def buy_with_sol(mint: str, sol_amount: float, slippage_bps: int = 300):
    """Spend `sol_amount` SOL to buy `mint`. Returns (signature, quote)."""
    return _swap(SOL_MINT, mint, int(sol_amount * 1e9), slippage_bps)


def sell_all(mint: str, slippage_bps: int = 500):
    """Sell entire balance of `mint` back to SOL. Returns (signature, quote)."""
    bal = get_token_balance(mint)
    if bal == 0:
        raise RuntimeError("No balance to sell")
    return _swap(mint, SOL_MINT, bal, slippage_bps)


if __name__ == "__main__":
    # Dust test:  python jup_trade.py buy <mint> 0.005
    #             python jup_trade.py sell <mint>
    import sys
    if sys.argv[1] == "buy":
        sig, _ = buy_with_sol(sys.argv[2], float(sys.argv[3]))
    else:
        sig, _ = sell_all(sys.argv[2])
    print("https://solscan.io/tx/" + sig)
