# Upload CTV .bz2 file(s) from a local folder to the S3 ingestion prefix (Piece 1 landing source),
# then REMOVE the local copies once their upload succeeds. Uses `aws s3 mv` (S3-side move = upload,
# then delete the local source only after that file's upload succeeds).
#
# Runs on your local Windows machine. Requires AWS CLI v2 installed and credentials available
# (either `aws configure` already done, or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY set in the
# environment). The Piece-1 landing step then reads from this prefix; processed files move to
# archive/ in S3.
#
# Daily use: drop the day's *.bz2 into the local landing folder, then run this script - it uploads
# every *.bz2 and deletes each local file after its upload succeeds. Pass -KeepLocal to copy only
# (no local delete). A single file path also works.
#
# Usage:
#   powershell -File scripts\upload_ctv_sample.ps1                              # move all *.bz2 in the default folder
#   powershell -File scripts\upload_ctv_sample.ps1 -Path "C:\ctv\landing"       # move all *.bz2 in a folder
#   powershell -File scripts\upload_ctv_sample.ps1 -Path "C:\path\to\one.bz2"   # single file
#   powershell -File scripts\upload_ctv_sample.ps1 -KeepLocal                   # copy (do NOT delete local)

param(
  [string]$Path   = "C:\Users\venkata.adapa\Downloads\ctv_landing",
  [string]$Bucket = "dataplatformpoc-venketa",
  [string]$Prefix = "landing/ctv/ingestion",
  [string]$Region = "us-east-2",
  [switch]$KeepLocal
)

if (-not (Test-Path $Path)) { Write-Error "Path not found: $Path"; exit 1 }

$dest = "s3://$Bucket/$Prefix/"
$verb = if ($KeepLocal) { "cp" } else { "mv" }   # mv = upload then delete local source; cp = keep local
$isFolder = (Get-Item $Path).PSIsContainer

if ($isFolder) {
  Write-Host "$verb  $Path\*.bz2  ->  $dest    (KeepLocal=$KeepLocal)"
  aws s3 $verb "$Path" "$dest" --recursive --exclude "*" --include "*.bz2" --region $Region
} else {
  Write-Host "$verb  $Path  ->  $dest    (KeepLocal=$KeepLocal)"
  aws s3 $verb "$Path" "$dest" --region $Region
}
if ($LASTEXITCODE -ne 0) { Write-Error "Upload failed (exit $LASTEXITCODE) - local files left in place."; exit $LASTEXITCODE }

Write-Host "`nUpload complete. Objects now under the ingestion prefix:"
aws s3 ls "$dest" --region $Region
if (-not $KeepLocal) { Write-Host "`nLocal .bz2 file(s) removed after successful upload (S3-side move)." }
