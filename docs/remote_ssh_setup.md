# VS Code Remote-SSH → the CTV VM

Goal: connect a VS Code window on the Windows desktop to the EC2 VM so the integrated terminal,
Docker, and dbt all run **on the VM**, driven from the laptop.

**Connection facts:** host `3.145.213.86` · user `ec2-user` · key
`C:\Users\venkata.adapa\Downloads\aws_key.pem` · repo on VM `~/CTV_dbt_iceberg_poc`.
SSH (port 22) is already open (the `scp` step worked).

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
    HostName 3.145.213.86
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
