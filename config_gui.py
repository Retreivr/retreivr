import tkinter as tk
from tkinter import ttk
from tkinter import filedialog, messagebox
import json
import os
import subprocess
import sys
import threading
from pathlib import Path

DEFAULT_CONFIG_PATH = "~/config.json"

APP_SETTINGS_FILE = os.path.expanduser("~/.yt_archiver_gui_settings.json")

def load_app_settings():
    if os.path.exists(APP_SETTINGS_FILE):
        try:
            with open(APP_SETTINGS_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_app_settings(settings):
    try:
        with open(APP_SETTINGS_FILE, "w") as f:
            json.dump(settings, f, indent=4)
    except:
        pass


class ConfigGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("YouTube Archiver Configuration")

        # State
        self.playlist_rows = []
        self.config = {}

        # App settings (dark mode, last config path)
        self.app_settings = load_app_settings()
        self.dark_mode = self.app_settings.get("dark_mode", False)
        self.last_config_path = self.app_settings.get("last_config_path", "")

        # =========================
        # TOP BAR: CONFIG PATH + BUTTONS
        # =========================
        top = ttk.Frame(root)
        top.pack(side="top", fill="x", padx=10, pady=8)

        ttk.Label(top, text="Config File Path:").pack(side="left")

        self.config_path_entry = ttk.Entry(top, width=60)
        self.config_path_entry.pack(side="left", padx=5)
        if self.last_config_path:
            self.config_path_entry.insert(0, self.last_config_path)
        else:
            self.config_path_entry.insert(0, os.path.expanduser(DEFAULT_CONFIG_PATH))

        ttk.Button(top, text="Browse", command=self.browse_config).pack(side="left", padx=4)
        ttk.Button(top, text="Reload", command=self.reload_config).pack(side="left", padx=4)
        ttk.Button(top, text="Dark Mode", command=self.toggle_dark_mode).pack(side="left", padx=4)
        ttk.Button(top, text="Open in Finder", command=self.open_in_finder).pack(side="left", padx=4)

        # =========================
        # SCROLLABLE MAIN AREA
        # =========================
        container = ttk.Frame(root)
        container.pack(side="top", fill="both", expand=True, padx=10, pady=(0, 10))

        self.canvas = tk.Canvas(container, highlightthickness=0)
        scrollbar = ttk.Scrollbar(container, orient="vertical", command=self.canvas.yview)
        self.main_frame = ttk.Frame(self.canvas)

        self.main_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )

        self.canvas.create_window((0, 0), window=self.main_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # =========================
        # ACCOUNTS SECTION (READ-ONLY DISPLAY)
        # =========================
        ttk.Label(self.main_frame, text="Google Accounts", font=("Arial", 12, "bold")).pack(anchor="w", pady=(5, 0))
        self.accounts_frame = ttk.Frame(self.main_frame)
        self.accounts_frame.pack(fill="x", pady=5)

        # =========================
        # OAUTH FLOW (runs setup_oauth.py)
        # =========================
        ttk.Label(self.main_frame, text="OAuth Token Generator", font=("Arial", 12, "bold")).pack(anchor="w", pady=(10, 0))
        oauth_frame = ttk.Frame(self.main_frame)
        oauth_frame.pack(fill="x", pady=5)

        ttk.Label(oauth_frame, text="Account label:").grid(row=0, column=0, sticky="e")
        self.oauth_account_entry = ttk.Entry(oauth_frame, width=20)
        self.oauth_account_entry.grid(row=0, column=1, padx=5, pady=3, sticky="w")
        self.oauth_account_entry.insert(0, "family")

        ttk.Label(oauth_frame, text="Client secret JSON:").grid(row=1, column=0, sticky="e")
        self.oauth_client_secret_entry = ttk.Entry(oauth_frame, width=50)
        self.oauth_client_secret_entry.grid(row=1, column=1, padx=5, pady=3, sticky="w")
        self.oauth_client_secret_entry.insert(0, "tokens/client_secret_family.json")
        ttk.Button(oauth_frame, text="Browse", command=self.browse_client_secret).grid(row=1, column=2, padx=4)

        ttk.Label(oauth_frame, text="Token output path:").grid(row=2, column=0, sticky="e")
        self.oauth_token_entry = ttk.Entry(oauth_frame, width=50)
        self.oauth_token_entry.grid(row=2, column=1, padx=5, pady=3, sticky="w")
        self.oauth_token_entry.insert(0, "tokens/token_family.json")
        ttk.Button(oauth_frame, text="Save As", command=self.browse_token_output).grid(row=2, column=2, padx=4)

        self.oauth_open_browser = tk.BooleanVar(value=True)
        ttk.Checkbutton(oauth_frame, text="Open browser automatically", variable=self.oauth_open_browser).grid(row=3, column=1, sticky="w", pady=2)

        self.oauth_status_var = tk.StringVar(value="")
        self.oauth_run_btn = ttk.Button(oauth_frame, text="Run OAuth Flow", command=self.run_oauth_flow)
        self.oauth_run_btn.grid(row=4, column=1, sticky="w", pady=6)
        ttk.Label(oauth_frame, textvariable=self.oauth_status_var, foreground="green").grid(row=4, column=2, sticky="w", padx=6)

        # =========================
        # PLAYLISTS SECTION
        # =========================
        ttk.Label(self.main_frame, text="Playlists", font=("Arial", 12, "bold")).pack(anchor="w", pady=(10, 0))
        self.playlist_frame = ttk.Frame(self.main_frame)
        self.playlist_frame.pack(fill="x", pady=5)

        ttk.Button(self.main_frame, text="Add Playlist", command=self.add_playlist_card).pack(anchor="w", pady=3)

        # =========================
        # TELEGRAM SECTION
        # =========================
        ttk.Label(self.main_frame, text="Telegram Notifications", font=("Arial", 12, "bold")).pack(anchor="w", pady=(10, 0))

        tg_frame = ttk.Frame(self.main_frame)
        tg_frame.pack(fill="x", pady=5)

        ttk.Label(tg_frame, text="Bot Token:").grid(row=0, column=0, sticky="e")
        self.telegram_bot_token_entry = ttk.Entry(tg_frame, width=50)
        self.telegram_bot_token_entry.grid(row=0, column=1, padx=5, pady=3)

        ttk.Label(tg_frame, text="Chat ID:").grid(row=1, column=0, sticky="e")
        self.telegram_chat_id_entry = ttk.Entry(tg_frame, width=50)
        self.telegram_chat_id_entry.grid(row=1, column=1, padx=5, pady=3)

        # =========================
        # FILENAME TEMPLATE
        # =========================
        ttk.Label(self.main_frame, text="Filename Template", font=("Arial", 12, "bold")).pack(anchor="w", pady=(10, 0))

        self.filename_template_entry = ttk.Entry(self.main_frame, width=70)
        self.filename_template_entry.pack(anchor="w", pady=5)
        self.filename_template_entry.insert(0, "%(title)s - %(uploader)s - %(upload_date)s.%(ext)s")

        # =========================
        # FINAL FORMAT DROPDOWN (Combobox)
        # =========================
        ttk.Label(self.main_frame, text="Final Output Format", font=("Arial", 12, "bold")).pack(anchor="w", pady=(10, 0))

        self.final_format_var = tk.StringVar()
        self.format_box = ttk.Combobox(self.main_frame, textvariable=self.final_format_var, values=["mp4", "mkv", "webm"], state="readonly", width=10)
        self.format_box.pack(anchor="w", pady=5)
        self.final_format_var.set("mp4")

        # =========================
        # SAVE BUTTON
        # =========================
        ttk.Button(self.main_frame, text="Save Configuration", command=self.save_config).pack(anchor="w", pady=15)

        # Startup: ask for config, then load, then apply theme + center window
        self.root.after(50, self.startup_select_config)

    # ============================================================
    # STARTUP / CONFIG LOADING
    # ============================================================
    def startup_select_config(self):
        path = self.config_path_entry.get().strip()
        if path and os.path.exists(os.path.expanduser(path)):
            self.load_config_from_path(os.path.expanduser(path))

        self.apply_dark_mode()
        self.center_window()

    def center_window(self):
        self.root.update_idletasks()
        w = self.root.winfo_width()
        h = self.root.winfo_screenheight()  # fill screen height
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        x = (sw // 2) - (w // 2)
        y = 0  # start at top
        self.root.geometry(f"{w}x{h}+{x}+{y}")

    def browse_config(self):
        path = filedialog.askopenfilename(title="Select config.json", filetypes=[("JSON Files", "*.json")])
        if path:
            self.config_path_entry.delete(0, tk.END)
            self.config_path_entry.insert(0, path)

    def reload_config(self):
        path = self.config_path_entry.get().strip()
        if not path:
            messagebox.showerror("Missing Path", "Please select a config.json file first.")
            return
        self.load_config_from_path(path)

    def load_config_from_path(self, path):
        if not os.path.exists(path):
            messagebox.showerror("Error", f"Config file does not exist:\n{path}")
            return
        try:
            with open(path, "r") as f:
                self.config = json.load(f)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load config.json:\n{e}")
            return

        # remember for next session
        self.app_settings["last_config_path"] = path
        save_app_settings(self.app_settings)

        self.populate_from_config()

    def populate_from_config(self):
        # ----- Accounts (read-only cards) -----
        for w in self.accounts_frame.winfo_children():
            w.destroy()

        accounts = self.config.get("accounts", {})
        if isinstance(accounts, dict) and accounts:
            for name, entry in accounts.items():
                card = ttk.Frame(self.accounts_frame, padding=8, style="Card.TFrame")
                label_text = f"{name}"
                cs = entry.get("client_secret")
                tok = entry.get("token")
                if cs:
                    label_text += f"  | client_secret: {cs}"
                if tok:
                    label_text += f"  | token: {tok}"
                ttk.Label(card, text=label_text, wraplength=600).pack(anchor="w")
                card.pack(fill="x", pady=2)

            # Pre-fill OAuth section with the first account values, if any
            first_name, first_entry = next(iter(accounts.items()))
            if not self.oauth_account_entry.get().strip():
                self.oauth_account_entry.insert(0, first_name)
            cs_path = first_entry.get("client_secret")
            tok_path = first_entry.get("token")
            if cs_path and (self.oauth_client_secret_entry.get().strip() in ["", "tokens/client_secret_family.json"]):
                self.oauth_client_secret_entry.delete(0, tk.END)
                self.oauth_client_secret_entry.insert(0, cs_path)
            if tok_path and (self.oauth_token_entry.get().strip() in ["", "tokens/token_family.json"]):
                self.oauth_token_entry.delete(0, tk.END)
                self.oauth_token_entry.insert(0, tok_path)
        else:
            ttk.Label(self.accounts_frame, text="No accounts defined (managed in JSON).").pack(anchor="w")

        # ----- Playlists -----
        for w in self.playlist_frame.winfo_children():
            w.destroy()
        self.playlist_rows = []

        playlists = self.config.get("playlists", [])
        if isinstance(playlists, list) and playlists:
            for pl in playlists:
                self.add_playlist_card(
                    playlist_id=pl.get("playlist_id") or pl.get("id", ""),
                    directory=pl.get("folder") or pl.get("directory", ""),
                    account=pl.get("account", ""),
                    remove_after=pl.get("remove_after_download", False),
                )
        else:
            self.add_playlist_card()

        # ----- Telegram -----
        self.telegram_bot_token_entry.delete(0, tk.END)
        self.telegram_chat_id_entry.delete(0, tk.END)

        tg = self.config.get("telegram", {})
        if isinstance(tg, dict):
            bot_token = tg.get("bot_token", "")
            chat_id = tg.get("chat_id", "")
        else:
            bot_token = self.config.get("telegram_bot_token", "")
            chat_id = self.config.get("telegram_chat_id", "")

        self.telegram_bot_token_entry.insert(0, bot_token)
        self.telegram_chat_id_entry.insert(0, chat_id)

        # ----- Template -----
        tmpl = self.config.get("filename_template") or "%(title)s - %(uploader)s - %(upload_date)s.%(ext)s"
        self.filename_template_entry.delete(0, tk.END)
        self.filename_template_entry.insert(0, tmpl)

        # ----- Final format -----
        final_format = self.config.get("final_format", "mp4")
        if final_format not in ["mp4", "mkv", "webm"]:
            final_format = "mp4"
        self.final_format_var.set(final_format)

    # ============================================================
    # DARK / LIGHT THEME
    # ============================================================
    def apply_dark_mode(self):
        style = ttk.Style()
        # Preserve native platform theme (Aqua on macOS, Vista/XP on Windows, etc.)
        native_theme = style.theme_use()

        if self.dark_mode:
            bg = "#1e1e1e"
            fg = "#ffffff"
            entry_bg = "#2e2e2e"
            style.configure("TFrame", background=bg)
            style.configure("TLabel", background=bg, foreground=fg)
            style.configure("TEntry", fieldbackground=entry_bg, foreground=fg)
            style.configure("TButton", foreground=fg)
            style.configure("TScrollbar", troughcolor=bg, background=entry_bg)
            style.map("TButton",
                      background=[("active", "#333333")],
                      foreground=[("active", "#ffffff")])

            style.configure(
                "Card.TFrame",
                background=bg,
                borderwidth=1,
                relief="ridge"
            )
            self.canvas.configure(background=bg)
        else:
            bg = "#f0f0f0"
            fg = "#000000"
            entry_bg = "#ffffff"
            style.configure("TFrame", background=bg)
            style.configure("TLabel", background=bg, foreground=fg)
            style.configure("TEntry", fieldbackground=entry_bg, foreground=fg)
            style.configure("TButton", foreground=fg)
            style.configure("TScrollbar", troughcolor="#d0d0d0", background="#c0c0c0")
            style.map("TButton",
                      background=[("active", "#e0e0e0")],
                      foreground=[("active", "#000000")])

            style.configure(
                "Card.TFrame",
                background=bg,
                borderwidth=1,
                relief="ridge"
            )
            self.canvas.configure(background="white")

        self.root.configure(bg=bg)

    def toggle_dark_mode(self):
        self.dark_mode = not self.dark_mode
        self.app_settings["dark_mode"] = self.dark_mode
        save_app_settings(self.app_settings)
        self.apply_dark_mode()

    # ============================================================
    # OAUTH FLOW HANDLERS
    # ============================================================
    def browse_client_secret(self):
        path = filedialog.askopenfilename(title="Select client_secret JSON", filetypes=[("JSON Files", "*.json")])
        if path:
            self.oauth_client_secret_entry.delete(0, tk.END)
            self.oauth_client_secret_entry.insert(0, path)

    def browse_token_output(self):
        path = filedialog.asksaveasfilename(
            title="Save token JSON",
            defaultextension=".json",
            initialfile="token_family.json",
            filetypes=[("JSON Files", "*.json")],
        )
        if path:
            self.oauth_token_entry.delete(0, tk.END)
            self.oauth_token_entry.insert(0, path)

    def get_setup_oauth_path(self):
        base_dir = getattr(sys, "_MEIPASS", os.path.abspath(os.path.dirname(__file__)))
        return os.path.join(base_dir, "setup_oauth.py")

    def run_oauth_flow(self):
        account = self.oauth_account_entry.get().strip()
        client_secret = os.path.expanduser(self.oauth_client_secret_entry.get().strip())
        token_out = os.path.expanduser(self.oauth_token_entry.get().strip())

        if not account:
            messagebox.showerror("Missing Account", "Please enter an account label.")
            return
        if not client_secret or not os.path.exists(client_secret):
            messagebox.showerror("Missing Client Secret", "Please choose a valid client_secret JSON file.")
            return
        if not token_out:
            messagebox.showerror("Missing Token Path", "Please choose where to save the token JSON.")
            return

        script_path = self.get_setup_oauth_path()
        if not os.path.exists(script_path):
            messagebox.showerror("Missing Script", f"setup_oauth.py not found at:\n{script_path}")
            return

        try:
            Path(token_out).expanduser().parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        cmd = [sys.executable, script_path, account, client_secret, token_out]
        if not self.oauth_open_browser.get():
            cmd.append("--no-browser")

        self.oauth_run_btn.config(state="disabled")
        self.oauth_status_var.set("Running OAuth flow...")

        def worker():
            try:
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                out, _ = proc.communicate()
                code = proc.returncode
            except Exception as e:
                code = -1
                out = f"Error: {e}"

            def done():
                self.oauth_run_btn.config(state="normal")
                if code == 0:
                    self.oauth_status_var.set(f"Token saved to {token_out}")
                    messagebox.showinfo("OAuth Complete", out or f"Token saved to {token_out}")
                else:
                    self.oauth_status_var.set("OAuth failed")
                    messagebox.showerror("OAuth Failed", out or "Check console output for details.")

            self.root.after(0, done)

        threading.Thread(target=worker, daemon=True).start()

    # ============================================================
    # PLAYLIST CARD MANAGEMENT
    # ============================================================
    def add_playlist_card(self, playlist_id="", directory="", account="", remove_after=False):
        card = ttk.Frame(self.playlist_frame, padding=8, style="Card.TFrame")

        # Playlist ID
        row1 = ttk.Frame(card)
        ttk.Label(row1, text="Playlist ID:").pack(side="left")
        id_entry = ttk.Entry(row1, width=40)
        id_entry.pack(side="left", padx=5)
        id_entry.insert(0, playlist_id)
        row1.pack(anchor="w", pady=2)

        # Directory
        row2 = ttk.Frame(card)
        ttk.Label(row2, text="Directory:").pack(side="left")
        dir_entry = ttk.Entry(row2, width=40)
        dir_entry.pack(side="left", padx=5)
        dir_entry.insert(0, directory)

        def browse():
            folder = filedialog.askdirectory()
            if folder:
                dir_entry.delete(0, tk.END)
                dir_entry.insert(0, folder)

        ttk.Button(row2, text="Browse", command=browse).pack(side="left", padx=5)
        row2.pack(anchor="w", pady=2)

        # Account + remove checkbox
        row3 = ttk.Frame(card)
        ttk.Label(row3, text="Account:").pack(side="left")
        account_entry = ttk.Entry(row3, width=20)
        account_entry.pack(side="left", padx=5)
        account_entry.insert(0, account)

        remove_var = tk.BooleanVar(value=remove_after)
        ttk.Checkbutton(row3, text="Remove after download", variable=remove_var).pack(side="left", padx=10)
        row3.pack(anchor="w", pady=2)

        # Remove card button
        def remove_card():
            card.destroy()
            self.playlist_rows.remove((id_entry, dir_entry, account_entry, remove_var))

        ttk.Button(card, text="Remove", command=remove_card).pack(anchor="e", pady=3)

        card.pack(fill="x", pady=5)
        self.playlist_rows.append((id_entry, dir_entry, account_entry, remove_var))

    # ============================================================
    # SAVE CONFIG
    # ============================================================
    def save_config(self):
        path = self.config_path_entry.get().strip()
        if not path:
            messagebox.showerror("Missing Config Path", "Please specify a config.json file path.")
            return

        # Start from existing config to preserve unknown fields, especially accounts
        base = self.config if isinstance(self.config, dict) else {}
        new_cfg = dict(base)

        # Playlists
        playlists = []
        for id_entry, dir_entry, account_entry, remove_var in self.playlist_rows:
            pid = id_entry.get().strip()
            pdir = dir_entry.get().strip()
            acc = account_entry.get().strip()
            rm = bool(remove_var.get())
            if pid and pdir:
                playlists.append(
                    {
                        "playlist_id": pid,
                        "folder": pdir,
                        "account": acc,
                        "remove_after_download": rm,
                    }
                )
        if not playlists:
            messagebox.showerror("No Playlists", "Please add at least one playlist.")
            return
        new_cfg["playlists"] = playlists

        # Telegram
        bot_token = self.telegram_bot_token_entry.get().strip()
        chat_id = self.telegram_chat_id_entry.get().strip()
        if bot_token or chat_id:
            new_cfg["telegram"] = {"bot_token": bot_token, "chat_id": chat_id}

        # Filename template & final format
        new_cfg["filename_template"] = self.filename_template_entry.get().strip() or "%(title)s - %(uploader)s - %(upload_date)s.%(ext)s"
        new_cfg["final_format"] = self.final_format_var.get() or "mp4"

        # Write file
        try:
            with open(path, "w") as f:
                json.dump(new_cfg, f, indent=4)
            self.config = new_cfg
            messagebox.showinfo("Saved", "Configuration saved successfully!")
        except Exception as e:
            messagebox.showerror("Error Saving", f"Could not save config:\n{e}")

    # ============================================================
    # MISC HELPERS
    # ============================================================
    def open_in_finder(self):
        path = self.config_path_entry.get().strip()
        if os.path.exists(path):
            os.system(f'open -R "{path}"')
        else:
            messagebox.showerror("Error", "Cannot open path in Finder.")


# ============================================================
# RUN THE GUI
# ============================================================
if __name__ == "__main__":
    root = tk.Tk()
    app = ConfigGUI(root)
    root.mainloop()
