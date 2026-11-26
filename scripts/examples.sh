#!/usr/bin/env bash
# API 与 DNS 请求示例（针对 `docker compose up --build` 之后的本机映射端口）。
# 依赖：curl、dig（dnsutils/bind-utils）。没有 dig 时可用：
#   python scripts/dns_client.py --port 5354 demo.local
set -u

API=http://127.0.0.1:8000
DNS=127.0.0.1
PORT=5354

line() { printf '\n### %s\n' "$1"; }

line "健康检查"
curl -s "$API/healthz"; echo

line "查看当前上游配置与最近请求"
curl -s "$API/admin/state" | python3 -m json.tool

line "把 demo.local 的应答改成旧址 203.0.113.10，TTL=3 秒"
curl -s -X POST "$API/admin/answer" \
  -H 'Content-Type: application/json' \
  -d '{"qname":"demo.local","ip":"203.0.113.10","ttl":3}'; echo

line "DNS 查询（首次，正常应答）"
dig @"$DNS" -p "$PORT" demo.local A +time=2 +tries=1

line "给上游加 300ms 延迟"
curl -s -X POST "$API/admin/delay" -H 'Content-Type: application/json' \
  -d '{"delay_ms":300}'; echo

line "让上游停答（收到包但不回包）"
curl -s -X POST "$API/admin/silence" -H 'Content-Type: application/json' \
  -d '{"silence":true}'; echo

line "恢复应答"
curl -s -X POST "$API/admin/resume"; echo

line "把应答改成新址 198.51.100.77"
curl -s -X POST "$API/admin/answer" \
  -H 'Content-Type: application/json' \
  -d '{"qname":"demo.local","ips":["198.51.100.77"],"ttl":60}'; echo

line "把 gone.local 配成 NXDOMAIN"
curl -s -X POST "$API/admin/nxdomain" \
  -H 'Content-Type: application/json' \
  -d '{"qname":"gone.local"}'; echo

line "查询上游实际收到的 DNS 包记录"
curl -s "$API/admin/requests?limit=20" | python3 -m json.tool

line "重置样例上游"
curl -s -X POST "$API/admin/reset"; echo

line "DNS 示例：A 记录"
dig @"$DNS" -p "$PORT" demo.local A +time=2 +tries=1

line "DNS 示例：非 A/IN（AAAA）会被转发器 REFUSED"
dig @"$DNS" -p "$PORT" demo.local AAAA +time=2 +tries=1

line "不想装 dig 时使用仓库自带客户端"
python3 "$(dirname "$0")/dns_client.py" --server "$DNS" --port "$PORT" demo.local A
