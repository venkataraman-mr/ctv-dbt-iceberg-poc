# Upload CTV .bz2 sample file(s) to the S3 ingestion prefix (Piece 1 landing source).
# Runs on your local Windows machine. Requires AWS CLI v2 installed and credentials available
# (either `aws configure` already done, or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY set in the
# environment). The landing step then reads from this prefix; processed files move to archive/.
#
# Usage:
#   powershell -File scripts\upload_ctv_sample.ps1
#   powershell -File scripts\upload_ctv_sample.ps1 -File "C:\path\to\other.bz2"

param(
  [string]$File   = "C:\Users\venkata.adapa\Downloads\daily_US_CTV_20260701_20260701_v_1_1.bz2",
  [string]$Bucket = "dataplatformpoc-venketa",
  [string]$Prefix = "landing/ctv/ingestion",
  [string]$Region = "us-east-2"
)

if (-not (Test-Path $File)) { Write-Error "File not found: $File"; exit 1 }

$dest = "s3://$Bucket/$Prefix/"
Write-Host "Uploading $File -> $dest"
aws s3 cp "$File" "$dest" --region $Region
if ($LASTEXITCODE -ne 0) { Write-Error "Upload failed (exit $LASTEXITCODE)."; exit $LASTEXITCODE }

Write-Host "`nVerifying:"
aws s3 ls "$dest" --region $Region
