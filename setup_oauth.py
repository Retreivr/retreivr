#!/usr/bin/env python3
"""
Usage:
  python setup_oauth.py --account main tokens/client_secret_main.json tokens/token_main.json

This will run the OAuth flow and save the token to tokens/token_main.json
"""
from google_auth_oauthlib.flow import InstalledAppFlow
import argparse, json

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

    client_secret_file = args.client_secret
    token_file = args.token_out

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
        json.dump(creds_data, f, indent=2)
    print(f"Saved token to {token_file}")

if __name__ == "__main__":
    main()
