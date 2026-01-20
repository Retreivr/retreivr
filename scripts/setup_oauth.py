#!/usr/bin/env python3
"""
Usage:
  python scripts/setup_oauth.py --account main tokens/client_secret_main.json tokens/token_main.json

This will run the OAuth flow and save the token to tokens/token_main.json
"""
import os
import sys
import argparse

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from engine.json_utils import safe_json_dump
from engine.paths import TOKENS_DIR, ensure_dir, resolve_dir

def _require_python_311():
    if sys.version_info[:2] != (3, 11):
        found = sys.version.split()[0]
        raise SystemExit(
            f"ERROR: Retreivr requires Python 3.11.x; found Python {found} "
            f"(executable: {sys.executable})"
        )

if __name__ == "__main__":
    _require_python_311()

from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]

def main():
    parser = argparse.ArgumentParser(
        description="Run the OAuth flow and write a token JSON file."
    )
    parser.add_argument("account")
    parser.add_argument("client_secret")
    parser.add_argument("token_out")
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Do not auto-open the browser; print the Google auth URL instead.",
    )
    args = parser.parse_args()

    try:
        client_secret_file = resolve_dir(args.client_secret, TOKENS_DIR)
        token_file = resolve_dir(args.token_out, TOKENS_DIR)
    except ValueError as exc:
        raise SystemExit(f"ERROR: {exc}") from exc

    ensure_dir(TOKENS_DIR)

    flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)

    auth_prompt = (
        "\nOpen this URL in your browser to authorize:\n{url}\n"
        "After approving, close the browser window.\n"
    )
    success_msg = "Authentication complete. You may close this window and return to the app."

    creds = flow.run_local_server(
        port=0,  # pick a free port automatically
        open_browser=not args.no_browser,
        authorization_prompt_message=auth_prompt,
        success_message=success_msg,
    )

    creds_data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes
    }

    # Save token
    with open(token_file, "w") as f:
        safe_json_dump(creds_data, f, indent=2)
    print(f"Saved token to {token_file}")

if __name__ == "__main__":
    main()
