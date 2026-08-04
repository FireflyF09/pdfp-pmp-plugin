# PDFP Lite 联邦插件（pdfp-pmp-plugin）

Phira-mp+ 的 PDFP Lite 联邦 WASM 插件。实现 [PDFP Lite MVP v0.1](
/root/sd/Phira+%20提示词工程/PDFP_Lite_MVP_v0.1.md) 的最小互通层，作为
**PMP↔PMP** 与 **PMP↔gooophira-mp** 跨服房间互通的第一步。

角色模型（§1/§6）：
- **authority**：监听联邦端口，接受 proxy 连接（地址/节点白名单），把本地房间事件
  广播给所有 proxy，并把 proxy 上报的远端玩家应用到本地房间。
- **proxy**：承载本地玩家，连接对等 authority 节点；把本地房间事件（RoomJoin/
  GameStart/GameEnd/RoundComplete 等）转成 PDFP 事件帧发往 authority；接收 PDFP
  事件帧，通过 WIT `phira-room-mgmt` 的 `add-remote-player` / `remote-ready` /
  `remote-abort` / `remote-leave` 把远端玩家应用到本地房间。

## 功能（v1 最小可用）

1. **配置**：`data/plugins/pdfp-pmp-plugin/config.json`（`node_id` / `role` /
   `listen` / `peers`）。插件加载时经 `phira-config` 读取。
2. **联邦连接**：`phira-tcp`；TCP 明文；帧格式 `<4 字节大端长度><JSON 帧>`。
3. **握手 + 心跳**：连接建立后互发 `hello`；15s 心跳、30s 无心跳标记失联。
4. **事件外发（proxy→authority）**：PMP PluginEvent → PDFP 事件帧（`join` /
   `leave` / `ready` / `start` / `played` / `settle`）。
5. **事件应用（双向）**：接收 PDFP 事件帧 → 远程玩家 API 应用到本地房间。
6. **心跳/断连**：超时标记节点失联；proxy 断线自动重连（15s 节流）。

## 配置

```jsonc
// data/plugins/pdfp-pmp-plugin/config.json
{
  "node_id": "pmp-north",              // 节点名，hello 中声明
  "role": "authority",                 // "authority" | "proxy"
  "listen": "0.0.0.0:7900",            // authority 角色的联邦监听端口
  "peers": [                           // 对等节点白名单
    { "address": "10.0.0.2:7900", "node_id": "pmp-south" },
    { "address": "10.0.0.3:7900", "node_id": "pmp-west" }
  ]
}
```

proxy 角色示例（`role: "proxy"`，无 `listen`）：

```jsonc
{
  "node_id": "pmp-south",
  "role": "proxy",
  "peers": [
    { "address": "10.0.0.1:7900", "node_id": "pmp-north" }
  ]
}
```

参考示例见 `config/authority.json` 与 `config/proxy.json`。

## 帧协议（PDFP Lite MVP §3/§4/§6）

```
<4 字节大端长度> <JSON 帧>
{"type":"hello|heartbeat|event|snapshot|cmd","node_id":"...","seq":1,"payload":{...}}
```

- `hello`：`{"protocol":"lite/0.1","roles":[...],"rooms":[]}`，握手 + 角色声明。
- `heartbeat`：保活。
- `event`：`{"room_id":"...","evt":"join|leave|ready|abort|start|played|settle","payload":{...}}`
- `snapshot`：authority 在 proxy hello 后下发存量房间状态。
- `cmd`：保留权威裁决命令帧（v1 仅记录，命令裁决模型留待后续阶段）。

## 构建与部署

```bash
cargo build --target wasm32-unknown-unknown --release
wasm-tools component new target/wasm32-unknown-unknown/release/pdfp_pmp_plugin.wasm \
  -o pdfp_pmp_plugin.component.wasm
# 复制到 Phira-mp+ 插件目录并重启：
#   plugins/pdfp-pmp-plugin/plugin.wasm
#   data/plugins/pdfp-pmp-plugin/config.json
```

## 验证

插件仓库独立，用临时 path 依赖验证语法：

```bash
# 临时把 plugin/Cargo.toml 中 phira-plugin-sdk 改为：
#   phira-plugin-sdk = { path = "../../Phira-mp-plus/phira-plugin-sdk" }
cargo check --workspace
cargo test --workspace
# 通过后还原为 git 依赖再提交
```

## 与 Lite MVP 的对应关系

| Lite MVP 概念 | 本插件实现 |
|---|---|
| §2.1 节点信任（白名单） | `peers` 列表；accept 时地址校验 + hello 时 node_id 校验 |
| §2.3 节点握手 | `hello` 帧（node_id/role/protocol） |
| §3.2 帧格式 | `<4 字节大端长度><JSON 帧>` |
| §4 节点级消息 | `hello` / `heartbeat` /（`bye` 未实现，close 即断） |
| §5.2 核心命令 | `cmd` 帧保留；v1 采用事件中继（见边界） |
| §6.2 事件广播 | `event` 帧（join/leave/ready/abort/start/played/settle） |
| §6.3 快照 | `snapshot` 帧，authority 在 proxy hello 后下发 |
| §6.4 失联 | 30s 无心跳标记失联；proxy 断线自动重连；无分叉 |

## 已知边界（v1）

- **无周期时钟**：宿主 `phira-timer` 的 `timer_callback` 当前为 `None`（未接线），
  插件拿不到周期定时。心跳/超时判定采用「任意派发机会驱动」的尽力而为模型，并预留
  re-arm 定时器，待宿主接通后自动升级为周期心跳。
- **`played`/`settle` 只记录**：无 `remote_played` 插件 API，远程玩家计分应用留待
  后续阶段；房间结算仍需超时或 abort 驱动。
- **命令裁决模型未启用**：v1 按任务要求走「事件中继」（proxy→authority 发事件帧），
  Lite MVP §5.2 的 Authority 命令裁决模型（`cmd` 帧）保留待后续阶段。
- **ready 同步为 react 式**：PMP 没有 ready 的插件事件，插件在 GameStart/RoomJoin
  等触发点查询房间状态补发 `ready`。
- **无安全层**：地址白名单为端口级匹配，仅限可信/私有网络（MVP §8 诚实声明）。
