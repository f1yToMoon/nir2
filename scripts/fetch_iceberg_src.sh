#!/usr/bin/env bash
# Загружает исходники Apache Iceberg той версии, что используется на стенде,
# в third_party/iceberg-src (не коммитится). Нужны для проверки утверждений
# о поведении Iceberg/Spark по коду (docs/REVIEWER.md, принцип P6).
#
# Использование: bash scripts/fetch_iceberg_src.sh [версия]   (по умолчанию из infra/.env.example)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VER="${1:-$(grep -E '^ICEBERG_VERSION=' "$ROOT/infra/.env.example" | cut -d= -f2)}"
DEST="$ROOT/third_party/iceberg-src"
TAG="apache-iceberg-${VER}"

if [ -d "$DEST/.git" ] && [ "$(git -C "$DEST" describe --tags --exact-match 2>/dev/null)" = "$TAG" ]; then
  echo "Исходники $TAG уже в $DEST"; exit 0
fi
rm -rf "$DEST"; mkdir -p "$(dirname "$DEST")"
git clone --quiet --depth 1 --branch "$TAG" --filter=blob:none --sparse \
  https://github.com/apache/iceberg.git "$DEST"
git -C "$DEST" sparse-checkout set api core spark/v4.0/spark flink/v1.20/flink aws
echo "Готово: $DEST ($TAG). Пример: grep -n validateNoConflictingDataFiles -r $DEST/spark"
