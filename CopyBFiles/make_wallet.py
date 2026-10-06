"""
make_wallet.py — generate a fresh burner wallet for the bot.

Run ONCE:  python make_wallet.py
- Prints the public address (fund this with a small amount of SOL).
- Prints the base58 private key ONCE -> paste into .env as BURNER_PRIVATE_KEY.
- Never reuse this wallet for anything else. Never fund it with more than
  you are fully prepared to lose.
"""

from solders.keypair import Keypair

kp = Keypair()
print("PUBLIC ADDRESS (fund this):", kp.pubkey())
print("PRIVATE KEY (put in .env, never share):", kp)
