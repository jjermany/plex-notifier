# 📺 Plex Notifier

Plex Notifier is a lightweight Flask-based web service that monitors your Plex library and emails users when new episodes are available — **but only for shows they’ve already shown interest in** (i.e. fully watched at least one episode).

Built with Docker, designed for Unraid, and powered by Tautulli.

---

## ✨ Features

- ✅ Sends **personalized email notifications** for new episodes
- ✅ Automatically detects user interest via Tautulli watch history
- ✅ Fully responsive **Web UI** for configuring:
  - Plex and Tautulli credentials
  - Email (SMTP) settings
  - Notification interval
- ✅ Users can unsubscribe globally or per-show
- 🔐 Optional HTTP Basic Auth for admin access
- 📄 Per-user logs stored locally for review
- 🐳 Runs cleanly in Docker with Unraid support

---

## 🧰 Requirements

- A running **Plex** Media Server
- A **Tautulli** instance connected to your Plex
- SMTP credentials (Gmail, Mailgun, etc.)
- Docker (or Unraid)

---

## 🚀 Getting Started

### Docker CLI
```bash
docker run -d \
  --name plex-notifier \
  --net=host \
  -v /mnt/cache/appdata/plex-notifier:/app/instance \
  -e TZ=America/Chicago \
  -e SECRET_KEY=$(openssl rand -hex 32) \
  -e WEBUI_USER=admin \
  -e WEBUI_PASS=changeme \
  jjermany/plex-notifier:beta
```

> Replace the env values as needed. The container runs on port **5000**.

---

### Unraid Setup

1. Go to the **Apps** tab (Community Applications).
2. Add this template repo:
   ```
   https://github.com/jjermany/unraid-templates
   ```
3. Install the **Plex Notifier (Beta)** template.
4. Configure your settings in the WebUI (`http://[Unraid IP]:5000`).

---

## 🔒 Authentication

HTTP Basic Auth is optional but recommended.
Set the following environment variables:

```env
WEBUI_USER=yourusername
WEBUI_PASS=yourpassword
```

---

## 📬 Email Configuration

The following SMTP fields are required and configured via the Web UI:

- SMTP Host
- SMTP Port
- SMTP Username
- SMTP Password
- From Address (email)
- Base URL (used in unsubscribe links)

---

## 📝 Subscription Management

Each email includes a secure unsubscribe link:
- **Global opt-out**: stop all future notifications
- **Per-show opt-out**: stop notifications for specific shows only

You can review and manage preferences directly in the Web UI after clicking the unsubscribe link.

---

## 📦 Data & Logs

- All settings and user preferences are saved in:
  ```
  /app/instance/config.sqlite3
  ```
- Each user also gets a personal log file:
  ```
  /app/instance/logs/[user]-notification.log
  ```

---

## 🧪 Development

Want to help or test features?

```bash
git clone https://github.com/jjermany/plex-notifier.git
cd plex-notifier
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
flask run
```

You can also use Docker Compose for local dev (optional).

---

## 🙌 Contributing

Pull requests welcome! Please:
- Follow existing coding patterns
- Keep logging clean and minimal
- Add clear commit messages

---

## 🛠️ Roadmap

- ✅ Per-show opt-out
- ✅ Web UI settings panel
- ⏳ Discord/Pushbullet support (future)
- ⏳ Plex token auto-refresh (if needed)

---

## 📘 License

MIT License. See [LICENSE](LICENSE) for details.

---

## 🛠 Web UI Configuration Guide

After starting the container, navigate to the Web UI (e.g. `http://localhost:5000`) and fill in the following fields:

### 📺 Plex & Tautulli
| Field | Example | Description |
|-------|---------|-------------|
| **Plex URL** | `http://localhost:32400` | URL to your Plex server. Use `http://[LAN IP]:32400` if running separately. |
| **Plex Token** | `xxxxx` | Get your token from: [Plex Token Guide](https://support.plex.tv/articles/204059436-finding-an-authentication-token-x-plex-token/) |
| **Tautulli URL** | `http://localhost:8181` | URL to your Tautulli instance (must be reachable by this container). |
| **Tautulli API Key** | `xxxxxxxx` | Found under Tautulli Settings → Web Interface → API. |

### 📧 Email Settings
| Field | Example | Description |
|-------|---------|-------------|
| **SMTP Host** | `smtp.gmail.com` | Your email provider’s SMTP server. |
| **SMTP Port** | `587` | Common ports: `587` (TLS), `465` (SSL). |
| **SMTP Username** | `your.email@gmail.com` | The email address you’ll send from. |
| **SMTP Password** | `xxxxxxxx` | App-specific password (for Gmail/Mailgun/etc). |
| **From Address** | `notifier@example.com` | The email address shown to recipients. |
| **Base URL** | `http://notifier.example.com` | Publicly accessible URL to your notifier app. Usually this is a domain or tunnel (e.g. Cloudflare Tunnel, NGINX Proxy Manager) pointing to your app’s port. This is used for unsubscribe links. |

### ⏱️ Polling Interval
| Field | Example | Description |
|-------|---------|-------------|
| **Notify Interval** | `30` | How often (in minutes) to check for new Plex episodes. Default is 30. |

### 🧪 Testing
- Use the **"Send Test Email"** form to verify SMTP settings.
- Use **"Run Now (last 24h)"** to manually trigger a check for new episodes.
