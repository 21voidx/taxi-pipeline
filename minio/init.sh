#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════
#  minio-init.sh
#  Membuat bucket CDC dan set policy agar Kafka Connect bisa write.
#  Dijalankan sekali oleh service minio-init di docker-compose.
# ══════════════════════════════════════════════════════════════
set -euo pipefail

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
CDC_BUCKET="${CDC_BUCKET:-cdc-raw}"
MINIO_ALIAS="local"
MAX_WAIT=60
INTERVAL=3

# ── Wait for MinIO ────────────────────────────────────────────
echo "⏳  Waiting for MinIO at ${MINIO_ENDPOINT} ..."
elapsed=0
until mc alias set "${MINIO_ALIAS}" "${MINIO_ENDPOINT}" \
        "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" > /dev/null 2>&1; do
    if [ $elapsed -ge $MAX_WAIT ]; then
        echo "❌  Timeout: MinIO not reachable after ${MAX_WAIT}s"
        exit 1
    fi
    sleep $INTERVAL
    elapsed=$((elapsed + INTERVAL))
    echo "   ... still waiting (${elapsed}s)"
done
echo "✅  MinIO is ready!"

# ── Create CDC bucket ─────────────────────────────────────────
echo ""
echo "🪣  Creating bucket: ${CDC_BUCKET}"
if mc ls "${MINIO_ALIAS}/${CDC_BUCKET}" > /dev/null 2>&1; then
    echo "   ℹ️  Bucket '${CDC_BUCKET}' already exists, skipping."
else
    mc mb "${MINIO_ALIAS}/${CDC_BUCKET}"
    echo "   ✅  Bucket '${CDC_BUCKET}' created."
fi

# ── Set bucket versioning (opsional, berguna untuk audit) ─────
mc version enable "${MINIO_ALIAS}/${CDC_BUCKET}" || true

# ── Create folder structure (MinIO buat otomatis, tapi kita touch placeholder) ──
mc cp /dev/null "${MINIO_ALIAS}/${CDC_BUCKET}/raw/cdc/.keep" > /dev/null 2>&1 || true

# ── Create dedicated CDC service account ─────────────────────
echo ""
echo "👤  Creating service account for Kafka Connect..."
mc admin user add "${MINIO_ALIAS}" kafkaconnect kafkaconnect123 > /dev/null 2>&1 || \
    echo "   ℹ️  User 'kafkaconnect' already exists."

# Policy: readwrite hanya untuk bucket cdc-raw
cat > /tmp/cdc-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads"
      ],
      "Resource": ["arn:aws:s3:::cdc-raw"]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListMultipartUploadParts",
        "s3:AbortMultipartUpload"
      ],
      "Resource": ["arn:aws:s3:::cdc-raw/*"]
    }
  ]
}
EOF

mc admin policy create "${MINIO_ALIAS}" cdc-readwrite /tmp/cdc-policy.json > /dev/null 2>&1 || \
    mc admin policy update "${MINIO_ALIAS}" cdc-readwrite /tmp/cdc-policy.json > /dev/null 2>&1 || true

mc admin policy attach "${MINIO_ALIAS}" cdc-readwrite \
    --user kafkaconnect > /dev/null 2>&1 || true

echo "   ✅  Service account 'kafkaconnect' ready."
echo ""
echo "═══════════════════════════════════════════"
echo "  MinIO Setup Complete"
echo "═══════════════════════════════════════════"
echo "  Endpoint  : ${MINIO_ENDPOINT}"
echo "  Bucket    : ${CDC_BUCKET}"
echo "  SA User   : kafkaconnect"
echo "  Console   : http://localhost:9001"
echo ""
echo "  Output path akan:"
echo "  s3://cdc-raw/raw/cdc/<table>/year=YYYY/month=MM/day=dd/hour=HH/"
echo "═══════════════════════════════════════════"
