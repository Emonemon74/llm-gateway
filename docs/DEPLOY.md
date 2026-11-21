# Deploying to a fresh server (AWS EC2)

Result: `https://<your-host>/v1/chat/completions` (OpenAI-compatible) and a login-protected
Grafana at `https://<your-host>/grafana/`. The `/admin` API and `/metrics` are never exposed to
the internet (Caddy returns 404); you operate them from the server with `deploy/admin.sh`.

Everything below was tested locally against the exact same Compose file: HTTPS routing rules,
blocked admin/metrics, OpenAI SDK + SSE streaming through the proxy, batch worker, Prometheus
scraping every replica, Grafana provisioning, scaling to 2 API replicas, and a full
down/up restart with data intact. Idle footprint of the whole stack is about 400 MB RAM.

## 1. Launch the instance (AWS console)
| Setting | Value |
|---|---|
| AMI | Ubuntu Server 24.04 LTS |
| Type | `t3.small` (2 GB) is enough for the demo; `t3.medium` (4 GB) if you will load-test it |
| Storage | 20 GB gp3 |
| Key pair | an existing one, or create a new `.pem` and keep it safe |
| Security group | **22** from *your IP only*, **80** and **443** from anywhere (0.0.0.0/0) |

Then allocate an **Elastic IP** and associate it with the instance (otherwise the IP changes on
restart and your hostname/DNS breaks).

## 2. Hostname
HTTPS needs a hostname. Either a real domain (A record -> Elastic IP) or a free wildcard-DNS name
built from the IP: for `3.111.210.77` use `3-111-210-77.nip.io`. Caddy gets the certificate
automatically (Let's Encrypt, with ZeroSSL fallback). If issuance fails on a shared domain because
of provider rate limits, switch to a real domain.

## 3. Prepare the server
```bash
ssh -i ~/.ssh/<key>.pem ubuntu@<elastic-ip>
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker ubuntu && exit        # log out/in so the group applies
```

The repo is private, so give the server a **read-only deploy key**:
```bash
ssh -i ~/.ssh/<key>.pem ubuntu@<elastic-ip>
ssh-keygen -t ed25519 -N "" -f ~/.ssh/deploy_key && cat ~/.ssh/deploy_key.pub
# GitHub -> repo Settings -> Deploy keys -> Add key (leave "Allow write access" OFF)
GIT_SSH_COMMAND="ssh -i ~/.ssh/deploy_key -o IdentitiesOnly=yes" \
  git clone git@github.com:Emonemon74/llm-gateway.git && cd llm-gateway
git config core.sshCommand "ssh -i ~/.ssh/deploy_key -o IdentitiesOnly=yes"
```

## 4. Configure secrets (never committed)
```bash
cp .env.prod.example .env.prod && chmod 600 .env.prod
# generate three secrets and paste them in:
openssl rand -base64 32 | tr -d '=+/' | cut -c1-40
nano .env.prod       # SITE_ADDRESS, POSTGRES_PASSWORD, ADMIN_TOKEN, GRAFANA_ADMIN_PASSWORD
```
Provider keys (`GROQ_API_KEY`, ...) are optional; the built-in mock upstream makes the demo work
without them.

## 5. Start it
```bash
docker compose -f deploy/docker-compose.prod.yml --env-file .env.prod up -d --build
docker compose -f deploy/docker-compose.prod.yml --env-file .env.prod ps   # all healthy?
curl https://<your-host>/healthz
```

## 6. Create a demo tenant and call it
```bash
deploy/seed_demo.sh demo           # prints an API key ONCE, save it
```
```python
from openai import OpenAI

c = OpenAI(base_url="https://<your-host>/v1", api_key="sk-gw-...")
print(c.chat.completions.create(model="demo", messages=[{"role": "user", "content": "hi"}]))
```
Other admin calls: `deploy/admin.sh GET /admin/routes`, `deploy/admin.sh POST /admin/tenants '{"name":"x"}'`.

## 7. Operate
| Task | Command |
|---|---|
| Logs | `docker compose -f deploy/docker-compose.prod.yml --env-file .env.prod logs -f api worker` |
| Update to latest `main` | `git pull && docker compose -f deploy/docker-compose.prod.yml --env-file .env.prod up -d --build` (migrations run automatically) |
| Roll back | `git checkout <good-commit>` then the same `up -d --build` |
| Add API replicas | `... up -d --scale api=2` (Caddy and Prometheus discover them automatically) |
| Add batch workers | `... up -d --scale worker=2` |
| Back up Postgres | `docker compose ... exec -T postgres pg_dump -U gateway gateway > backup.sql` |
| Grafana | `https://<your-host>/grafana/` (user `admin`, your `GRAFANA_ADMIN_PASSWORD`) |

## Security notes
- Only 80/443 are published; Postgres, Redis, Prometheus and the workers are internal-only.
- `/admin*` and `/metrics*` return 404 at the proxy. Prometheus scrapes the API over the internal
  network with the admin token injected as a Docker secret (not in git, not in the image).
- Containers run as a non-root user; logs are size-capped; memory limits protect a small instance.
- Keep SSH (22) restricted to your own IP. Rotate `ADMIN_TOKEN` by editing `.env.prod` and re-running `up -d`.
- The API is a money-burning endpoint if you add real provider keys: keep demo tenants' budgets small.

## Known limits
- Stock Caddy has no request-concurrency limit. Overload protection is the in-app load shedder
  (`app/shedding.py`, see docs/benchmarks.md). A proxy-level limit would need a Caddy plugin or nginx.
- Single server = single point of failure. The design is multi-replica-ready (all state is in
  Redis/Postgres) but this guide deploys one host.
