# DNS 旧址兜底转发器（Python UDP + FastAPI 样例上游）

内网 DNS 上游卡住（丢包/高延迟）时，转发器在 **100ms** 后先把未过期超过 **30s** 的旧地址以 **TTL=0** 回给客户端，同域名并发查询共用一次回源；回源总限时 **1s**，发出旧址后回源仍在后台继续，上游恢复后下一次查询即可拿到新地址。

- 纯 Python 标准库实现 DNS 编解码、缓存、single-flight 与 UDP 转发（运行转发器零第三方依赖）。
- 样例上游同时提供 UDP DNS 服务和 FastAPI 管控接口：**改应答 / 调延迟 / 停答 / 配 NXDOMAIN**。
- 一条 `docker compose up` 启动完整环境。

---

## 1. 它如何工作

```
客户端 ──UDP/53──▶ dns-forwarder ──UDP──▶ sample-upstream (DNS :5353)
                     │  TTL 缓存                ▲
                     │  single-flight          │ FastAPI :8000 管控
                     └─────────────────────────┘（改应答/延迟/停答）
```

对每个 **A/IN** 查询（其它类型/类别回 `REFUSED`）：

1. **缓存新鲜**（年龄 < 上游 TTL）：直接回答，TTL 按已缓存时间递减。
2. **缓存缺失或到期**：加入/启动该域名唯一的一次回源（single-flight）。
3. 回源满 **100ms** 仍无成功/明确应答，且存在过期未超过 **30s** 的旧址：
   立即回 `NOERROR + 旧址 A 记录，TTL=0`。
4. 旧址回包后，共享回源任务**不取消**，继续到 **1s** 总限时；期间若上游应答则按上游 TTL 刷新缓存，供后续查询使用。
5. 没有可用旧址（从未查到、过期超过 30s、或被 NXDOMAIN 清除）：等待回源，满 **1s** 仍失败回 `SERVFAIL`。
6. 上游回 `NXDOMAIN`：立即删除该域名旧址并回 NXDOMAIN。
7. 只有上游 TTL>0 的 A 应答才入缓存；回源超时/网络错误**不写缓存、不更新 `fetched_at`**，因此不会给旧址续期。

同一域名的 8 个并发到期查询实测只产生 **1** 个上游 UDP 包，8 个客户端都在 ~100ms 收到 TTL=0 旧址。

---

## 2. 目录

```
app/
  dns_codec.py     # DNS 报文编解码（支持域名压缩指针）
  cache.py         # 按上游 TTL 的缓存 + 30s stale 窗口（时钟可注入）
  resolver.py      # single-flight、100ms stale、1s 总限时状态机
  forwarder.py     # UDP 转发器入口（仅 A/IN，其余 REFUSED）
  upstream.py      # 样例上游：UDP DNS + FastAPI 管控 API
  config.py        # 环境变量配置
  runtime.py       # 测试用 stdin-EOF 自杀钩子（Docker 不启用）
scripts/
  demo.py               # 自动演示“先旧址、后新址”等 8 个场景
  dns_client.py         # 零依赖 DNS 查询 CLI（无 dig 时使用）
  run_demo_local.py     # 无 Docker 时本地起两个进程并跑 demo
  examples.sh           # curl + dig 示例合集
tests/
  test_dns_codec.py     # 编解码单测（装了 dnspython 时做互通校验）
  test_cache.py         # TTL / 30s 边界 / NXDOMAIN 删除
  test_resolver.py      # single-flight / stale / 超时 / 失败处理
  selftest_e2e.py       # 起真实子进程的端到端自测（31 项断言）
docker-compose.yml
Dockerfile
requirements.txt
```

---

## 3. Docker Compose 启动

```bash
# 先构建并后台启动两个服务
docker compose up --build -d

# 跑“先旧址、后新址”自动演示容器（也可直接在宿主机 python3 scripts/demo.py）
docker compose --profile demo run --rm demo

# 手工验证：dig / curl（见下面两节），结束后
docker compose down
```

前台查看日志用 `docker compose logs -f dns-forwarder sample-upstream`。

端口映射（绑在回环，避免和本机 53 端口的 resolved/named 冲突）：

| 宿主机地址 | 容器端口 | 用途 |
| --- | --- | --- |
| `127.0.0.1:53/udp` | `dns-forwarder:53/udp` | DNS 查询入口（标准端口，`dig @127.0.0.1`） |
| `127.0.0.1:5354/udp` | `dns-forwarder:53/udp` | 同一入口的免冲突备用映射（`dig -p 5354`） |
| `127.0.0.1:8000/tcp` | `sample-upstream:8000/tcp` | FastAPI 管控 API |

两个 UDP 映射指向同一服务，用任一端口查询均可。若本机 `127.0.0.1:53` 已被
dnsmasq/named 等占用，删除 compose 中 `"127.0.0.1:53:53/udp"` 一行、只用 5354 即可。
需要对局域网提供 DNS 时，把端口映射改成 `"53:53/udp"`。

也可以在宿主机直接跑演示脚本（DNS 部分零依赖；默认连 compose 映射端口）：

```bash
python3 scripts/demo.py
# 目标可用环境变量覆盖：DNS_HOST / DNS_PORT / API_BASE
```

无 Docker 时用 `python3 scripts/run_demo_local.py` 自动在本地高端口起两个服务并演示。

---

## 4. DNS 请求示例

### dig

```bash
# 普通 A 查询（compose 已把容器 53 映射到宿主机回环 53）
dig @127.0.0.1 demo.local A
# 若 53 与本机服务冲突，使用备用映射：
dig @127.0.0.1 -p 5354 demo.local A

# 非 A/IN：转发器回 REFUSED
dig @127.0.0.1 demo.local AAAA

# 观察 +time 与 TTL=0 的旧址应答
dig @127.0.0.1 demo.local A +stats
```

正常新应答期望：`status: NOERROR`，ANSWER 段含配置的 IPv4，TTL 为上游 TTL（随缓存年龄递减）。
卡住时的旧址应答期望（约在查询后 100ms 返回）：

```
demo.local.    0    IN    A    203.0.113.10
;; ->>HEADER<<- opcode: QUERY, status: NOERROR
;; Query time: 101 msec
```

上游停答且该域名从无可用旧址时，约 1000ms 后：

```
;; ->>HEADER<<- ... status: SERVFAIL
```

### 不带 dig：仓库自带客户端

```bash
python3 scripts/dns_client.py --server 127.0.0.1 --port 5354 demo.local A
# demo-xxxx.local A: NOERROR 203.0.113.10 ttl=0 (101ms)
```

---

## 5. FastAPI 管控接口与 curl 示例

Swagger 文档：<http://127.0.0.1:8000/docs>

| 方法 & 路径 | 作用 |
| --- | --- |
| `GET  /healthz` | 健康检查 |
| `GET  /` | 服务与端点列表 |
| `GET  /admin/state` | 当前延迟/停答/应答配置 |
| `PUT  /admin/config` | 部分更新 `delay_ms` / `silence` / `default_ips` / `default_ttl` |
| `POST /admin/answer` | 改应答：`{qname?, ip|ips, ttl?, rcode?}`；省略 `qname` 改兜底应答 |
| `POST /admin/delay` | 调延迟：`{"delay_ms": 300}` |
| `POST /admin/silence` | 停答：`{"silence": true}`（空 body 也表示停答） |
| `POST /admin/resume` | 恢复应答 |
| `POST /admin/nxdomain` | 把某域名配成 NXDOMAIN：`{"qname":"gone.local"}` |
| `DELETE /admin/record/{qname}` | 删除域名级覆盖，回到兜底应答 |
| `POST /admin/reset` | 重置全部管控状态 |
| `GET  /admin/requests?qname=&limit=` | 查看上游实际收到的 DNS 包/动作 |
| `DELETE /admin/requests` | 清空请求记录 |

未配置域名级应答时，样例上游用兜底 A 记录 `203.0.113.10`（TTL 30）回答所有 A/IN。

### 演示“先收到旧址，再查到新址”的完整命令序列

```bash
API=http://127.0.0.1:8000

# 1) 配置旧址，TTL=2s
curl -s -X POST $API/admin/answer -H 'Content-Type: application/json' \
  -d '{"qname":"demo.local","ip":"203.0.113.10","ttl":2}'

dig @127.0.0.1 -p 5354 demo.local A        # -> 203.0.113.10, ttl=2

sleep 2.2                                   # 等 TTL 到期（仍在 30s stale 窗口）

# 2) 上游停答（模拟卡住）
curl -s -X POST $API/admin/silence -H 'Content-Type: application/json' \
  -d '{"silence":true}'

dig @127.0.0.1 -p 5354 demo.local A        # ~100ms -> 203.0.113.10, ttl=0

# 3) 等过 1s 回源总限时，然后恢复并改答为新址
sleep 1.1
curl -s -X POST $API/admin/answer -H 'Content-Type: application/json' \
  -d '{"qname":"demo.local","ip":"198.51.100.77","ttl":60}'
curl -s -X POST $API/admin/resume

dig @127.0.0.1 -p 5354 demo.local A        # -> 198.51.100.77, ttl=60
```

其它管控示例：

```bash
curl -s -X POST $API/admin/delay   -H 'Content-Type: application/json' -d '{"delay_ms":400}'
curl -s -X POST $API/admin/nxdomain -H 'Content-Type: application/json' -d '{"qname":"gone.local"}'
curl -s "$API/admin/requests?limit=20"
bash scripts/examples.sh           # 以上示例的可执行合集
```

实测 curl 输出（节选自本地真实运行）：

```text
### POST /admin/answer
{"ok":true,"key":"curl-demo.local","spec":{"ips":["203.0.113.10"],"ttl":2,"rcode":0}}
### 首次 DNS
curl-demo.local A: NOERROR 203.0.113.10 ttl=2 (4ms)
### POST /admin/silence
{"ok":true,"silence":true}
### 停答后的 DNS
curl-demo.local A: NOERROR 203.0.113.10 ttl=0 (101ms)
### resume + 改新址后的 DNS
curl-demo.local A: NOERROR 198.51.100.77 ttl=60 (2ms)
```

---

## 6. 配置项（环境变量）

转发器：

| 变量 | 默认（compose 内） | 说明 |
| --- | --- | --- |
| `DNS_LISTEN_HOST` / `DNS_LISTEN_PORT` | `0.0.0.0` / `53` | UDP 监听地址 |
| `UPSTREAM_DNS_HOST` / `UPSTREAM_DNS_PORT` | `sample-upstream` / `5353` | 上游 DNS |
| `STALE_AFTER_MS` | `100` | 回源多久无应答后回 TTL=0 旧址 |
| `UPSTREAM_TIMEOUT_MS` | `1000` | 回源总限时 |
| `STALE_GRACE_S` | `30` | 过期后允许使用旧址的最长秒数 |

样例上游：`UPSTREAM_DNS_HOST/PORT`（`0.0.0.0:5353`）、`API_HOST/PORT`（`0.0.0.0:8000`）。

---

## 7. 本地自测（无需 Docker）

```bash
# 转发器与测试本身只用标准库；若要在本机起 FastAPI 上游，先装依赖：
python3 -m pip install -r requirements.txt

python3 -m unittest discover -s tests -v     # 24 个单测
python3 tests/selftest_e2e.py                # 起真实子进程，31 项端到端断言
python3 scripts/run_demo_local.py            # 本地起栈并跑完整演示
```

端到端自测会覆盖：基本 A 转发、A/AAAA 与 A/CH 的 REFUSED、缓存命中不回源、
8 并发 single-flight、~100ms TTL=0 旧址、后台回源续新、延迟中途改答后后台刷新到新址、
停答→旧址→恢复改答→新址、无旧址 ~1000ms SERVFAIL、NXDOMAIN 清旧址后 SERVFAIL、
超 grace 不用旧址、API 422 校验，以及 dnspython 的 EDNS 报文互通。

最近一次实测结果与未完成项见 **[docs/自测报告.md](docs/自测报告.md)**。

---

## 8. 已实现功能与限制（摘要）

**已做成**：题目要求的全部主路径与关键时序，均有自动化测试和实测耗时数据；
Dockerfile / compose 文件通过 `docker compose config` 校验。

**主要限制 / 未完成项**：

- 本交付沙箱没有 Docker daemon 且无 root，**镜像未在此环境实际 build/up**；
  已用等价的本机进程完成全部运行验证，compose 文件通过 v2.29.7 `config` 校验。
- 仅 UDP、仅 A/IN；非 A/IN 选择回 `REFUSED`（也可改为 `NOERROR` 空应答）。
- 未实现 TCP/53、DNSSEC、AAAA 等其它记录的转发与完整 CNAME 链处理。
- NXDOMAIN 立即清除旧址且不做负缓存；NODATA（NOERROR 无 A）透传空应答、不清旧址、不缓存。
- 单进程 asyncio 模型，面向演示/小规模内网使用，未做水平扩展与安全加固（限流、ACL 等）。
