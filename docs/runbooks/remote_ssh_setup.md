# VS Code Remote-SSH → the CTV VM

Goal: connect a VS Code window on the Windows desktop to the EC2 VM so the integrated terminal,
Docker, and dbt all run **on the VM**, driven from the laptop.

**Connection facts:** host `18.222.25.33` · user `ec2-user` · key
`C:\Users\venkata.adapa\Downloads\aws_key.pem` · repo on VM `~/CTV_dbt_iceberg_poc`.
SSH (port 22) is already open (the `scp` step worked).

> This is the VM's **public IP, which changes whenever the instance is stopped/restarted** (it is not a
> persistent Elastic IP). Last changed 2026-08-11 after a DevOps memory upgrade + restart
> (`3.145.213.86` → `18.222.25.33`). If it changes again, update `HostName` in your `~/.ssh/config`
> (step 3), the `scp` line in `scripts/vm_setup.md`, and the checkpoint's VM/env line.

## 0b. Git identity + auth on the VM (push/pull as yourself)

The repo is public, so `git pull` works unauthenticated — but pushing (and pulling a private repo) needs
auth. The VM had no git identity and an HTTPS remote; set your identity and switch to an **SSH deploy key**
so nothing sensitive is stored on the box. Run these **on the VM**:

```bash
# 1. commit identity (user.name is just the commit label; use the GitHub handle)
git config --global user.name  "venkataraman-mr"
git config --global user.email "venkataraman@mediaradar.com"   # must be a VERIFIED email on the GitHub account

# 2. generate a key (Enter for no passphrase, or set one)
ssh-keygen -t ed25519 -C "venkataraman@mediaradar.com" -f ~/.ssh/id_ed25519

# 3. print the PUBLIC key and copy the whole line
cat ~/.ssh/id_ed25519.pub

# 4. add it in GitHub -> Settings -> SSH and GPG keys -> New SSH key (Authentication key), paste, Add

# 5. test
ssh -T git@github.com          # first time: type "yes"; success = "Hi venkataraman-mr! ..."

# 6. switch this repo from HTTPS to SSH
git remote set-url origin git@github.com:venkataraman-mr/ctv-dbt-iceberg-poc.git
git remote -v                  # both lines should read git@github.com:...

# 7. verify
git pull
```

- The **private** key (`~/.ssh/id_ed25519`) stays on the VM — never share or commit it; only the `.pub`
  goes to GitHub. Linux sets the right perms automatically (`~/.ssh` 700, key 600).
- If you set a passphrase, load it once per session: `eval "$(ssh-agent -s)" && ssh-add ~/.ssh/id_ed25519`.
- `Permission denied (publickey)` almost always means the public key wasn't saved to GitHub yet, or only
  part of the line was copied.
- HTTPS-token alternative (if SSH is blocked): create a fine-grained PAT (Contents: read/write) and
  `git config --global credential.helper 'cache --timeout=3600'`; avoid `store` (writes the token in
  plaintext to `~/.git-credentials`).
- **For production / DevOps handoff:** use a per-repo read-only **deploy key** or a machine user for
  automation rather than a personal key.

## 1. Install the Remote-SSH extension
VS Code → Extensions (`Ctrl+Shift+X`) → search **"Remote - SSH"** (Microsoft,
`ms-vscode-remote.remote-ssh`) → Install.

## 2. Lock down the key's permissions (Windows)
OpenSSH refuses a private key that other users can read. In **PowerShell**:
```powershell
icacls "C:\Users\venkata.adapa\Downloads\aws_key.pem" /inheritance:r
icacls "C:\Users\venkata.adapa\Downloads\aws_key.pem" /grant:r "$env:USERNAME:R"
```
(Removes inherited permissions, then grants only your user read. Skipping this gives an
"UNPROTECTED PRIVATE KEY FILE" error on connect.)

## 3. Add the SSH host
Command Palette (`Ctrl+Shift+P`) → **"Remote-SSH: Open SSH Configuration File"** → choose
`C:\Users\venkata.adapa\.ssh\config` → add:
```sshconfig
Host ctv-vm
    HostName 18.222.25.33
    User ec2-user
    IdentityFile C:\Users\venkata.adapa\Downloads\aws_key.pem
    ServerAliveInterval 60
    ServerAliveCountMax 10
```
`ServerAliveInterval` keeps the session from dropping while idle.

## 4. Connect
Command Palette → **"Remote-SSH: Connect to Host"** → `ctv-vm`. Pick **Linux** if prompted.
First connect downloads the VS Code server onto the VM (~300–500 MB) — see the disk note below.
Success = bottom-left status bar shows **`SSH: ctv-vm`**.

## 5. Open the repo on the VM
File → **Open Folder** → `/home/ec2-user/CTV_dbt_iceberg_poc`.

## 6. (Optional) remote extensions
Install **Docker** and **Python** extensions while connected — they install into the VM's server,
giving container views and Python tooling against the VM.

## 7. Run things (integrated terminal = a VM shell)
`` Ctrl+` `` opens a terminal **on the VM**:
```bash
cd ~/CTV_dbt_iceberg_poc
git pull
docker compose ps
bash scripts/smoke_test.sh
docker compose exec dbt dbt debug
```

## Disk note (8 GB root is tight)
Before step 4, check headroom on the VM:
```bash
df -h /
```
If it's very low, reclaim space before installing the server:
```bash
docker image prune -f
```

## Workflow reminder (avoid divergence)
Keep **editing** in the *local* window (`C:\work\CTV_dbt_iceberg_poc`) → commit & push → `git pull`
on the VM. Use this **Remote-SSH** window mainly for **running/monitoring** (terminal, docker, dbt,
logs). If you instead edit files directly in the Remote-SSH window, you're editing the VM's copy —
commit from there and treat the VM as the source, so the two copies don't drift.
