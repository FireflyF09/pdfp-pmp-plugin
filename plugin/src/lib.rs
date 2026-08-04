//! PDFP Lite 联邦插件（Phira-mp+ WASM / component-model）
//!
//! 目标：PMP↔PMP / PMP↔gooophira-mp 跨服房间互通的第一步（PDFP Lite MVP v0.1）。
//!
//! 角色：
//! - `proxy`：承载本地玩家，连接对等 authority 节点，把本地房间事件转发为
//!   PDFP 事件帧，并把远端事件应用到本地房间（远程玩家 API）。
//! - `authority`：监听联邦端口，接受 proxy 连接（地址白名单），把本地事件广播
//!   给所有 proxy，并把 proxy 上报的远端玩家应用到本地房间。
//!
//! 传输：TCP 明文，帧格式 `<4 字节大端长度前缀><JSON 帧>`。
//! 帧对象：`{"type":"hello|heartbeat|event|snapshot|cmd","node_id":...,"seq":...,"payload":{...}}`
//!
//! 已知边界（v1）：
//! - 宿主 phira-timer 的 `timer_callback` 当前为 None（未接线），插件无法获得周期
//!   时钟；心跳/超时判定采用「任意派发机会驱动」的尽力而为模型，并预留 re-arm
//!   定时器，待宿主接通 timer 派发后自动升级为周期心跳。
//! - `played`/`settle` 帧当前只记录状态（无 `remote_played` 插件 API），远程玩家
//!   计分应用留待后续阶段。
//! - 无安全层：地址白名单为端口级匹配，仅限可信/私有网络。

phira_plugin_sdk::wit_bindgen!("phira-plugin-v3");
export!(PdfpPlugin);

use crate::phira::plugin::phira_config;
use crate::phira::plugin::phira_host;
use crate::phira::plugin::phira_room_mgmt;
use crate::phira::plugin::phira_tcp;
use crate::phira::plugin::phira_timer;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

// ── 协议常量（PDFP Lite MVP §3/§4/§6）──────────────────────────────
const PROTOCOL: &str = "lite/0.1";
const HEARTBEAT_INTERVAL_MS: u64 = 15_000;
const HEARTBEAT_TIMEOUT_MS: u64 = 30_000;
const RETRY_INTERVAL_MS: u64 = 15_000;
const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024; // 单帧 4MiB 上限
const MAX_SNAPSHOT_ROOMS: usize = 50; // hello 后同步的房间数上限
const MAX_RELAY_HOP: u64 = 4; // 星形转发的跳数上限（防 mesh 环）

// ── 配置 ─────────────────────────────────────────────────────────────
#[derive(Default, Clone)]
struct PeerCfg {
    address: String,
    node_id: String,
}

#[derive(Default, Clone)]
struct Config {
    node_id: String,
    role: String, // "proxy" | "authority"
    listen: Option<String>,
    peers: Vec<PeerCfg>,
}

// ── 运行时状态 ───────────────────────────────────────────────────────
struct Peer {
    addr: String,
    node_id: String,
    role: String,
    outbound: bool, // 我方主动连接（proxy→authority）
    last_seen_ms: u64,
    last_hb_ms: u64,
    recv_buf: Vec<u8>,
    lost: bool,
}

#[derive(Default)]
struct RoomMirror {
    /// 已通过远程玩家 API 应用到本房间的 player_id → name
    remote: HashMap<u32, String>,
    /// 已向对端转发 ready 的本地 player_id
    forwarded_ready: HashSet<u32>,
}

#[derive(Default)]
struct State {
    cfg: Config,
    listener: Option<u64>,
    conns: HashMap<u64, Peer>,
    rooms: HashMap<String, RoomMirror>,
    seq: u64,
    retry: HashSet<String>, // 待重连的 peer 地址
    last_retry_ms: u64,
    timer_armed: bool,
    lost_nodes: HashSet<String>,
}

static STATE: Mutex<Option<State>> = Mutex::new(None);

fn with_state<T>(f: impl FnOnce(&mut State) -> T) -> T {
    let mut guard = STATE.lock().unwrap_or_else(|e| e.into_inner());
    if guard.is_none() {
        *guard = Some(State::default());
    }
    f(guard.as_mut().unwrap())
}

struct PdfpPlugin;

// ════════════════════════════════════════════════════════════════════
// 基础设施
// ════════════════════════════════════════════════════════════════════

fn log(level: &str, msg: &str) {
    phira_host::log(level, msg);
}

fn now_ms() -> u64 {
    phira_host::current_time_ms()
}

fn json_value_to_wit(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(b) => JsonValue::Flag(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                JsonValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                JsonValue::Float(f)
            } else {
                JsonValue::Text(n.to_string())
            }
        }
        Value::String(s) => JsonValue::Text(s.clone()),
        Value::Array(arr) => JsonValue::Array(serde_json::to_string(arr).unwrap_or_default()),
        Value::Object(obj) => JsonValue::Object(serde_json::to_string(obj).unwrap_or_default()),
    }
}

fn wit_json_to_serde(value: &JsonValue) -> Value {
    match value {
        JsonValue::Null => Value::Null,
        JsonValue::Flag(b) => Value::Bool(*b),
        JsonValue::Integer(i) => json!(*i),
        JsonValue::Float(f) => json!(*f),
        JsonValue::Text(s) => Value::String(s.clone()),
        JsonValue::Array(s) | JsonValue::Object(s) => {
            serde_json::from_str(s).unwrap_or(Value::String(s.clone()))
        }
    }
}

fn host_api(method: &str, args: &[Value]) -> Result<Value, String> {
    let wit_args: Vec<JsonValue> =
        args.iter().map(json_value_to_wit).collect();
    match phira_host::api_call(method, &wit_args) {
        ApiResult::Ok(v) => Ok(wit_json_to_serde(&v)),
        ApiResult::Error(e) => Err(e),
    }
}

// ── 配置加载（data/plugins/pdfp-pmp-plugin/config.json）──────────────

fn get_cfg_str(key: &str) -> Option<String> {
    match phira_config::get_config(key) {
        ApiResult::Ok(JsonValue::Text(s)) => Some(s),
        _ => None,
    }
}

fn get_cfg_peers() -> Vec<PeerCfg> {
    let raw = match phira_config::get_config("peers") {
        ApiResult::Ok(JsonValue::Array(s))
        | ApiResult::Ok(JsonValue::Object(s)) => s,
        _ => return Vec::new(),
    };
    let mut out = Vec::new();
    if let Ok(v) = serde_json::from_str::<Value>(&raw) {
        if let Some(arr) = v.as_array() {
            for e in arr {
                let address = e.get("address").and_then(|x| x.as_str()).unwrap_or("").to_string();
                let node_id = e.get("node_id").and_then(|x| x.as_str()).unwrap_or("").to_string();
                if !address.is_empty() {
                    out.push(PeerCfg { address, node_id });
                }
            }
        }
    }
    out
}

fn load_config() -> Config {
    let node_id = get_cfg_str("node_id").unwrap_or_else(|| "pdfp-node".to_string());
    let role = get_cfg_str("role").unwrap_or_else(|| "proxy".to_string());
    let listen = get_cfg_str("listen");
    let peers = get_cfg_peers();
    Config { node_id, role, listen, peers }
}

// ── 帧编解码 ─────────────────────────────────────────────────────────

fn encode_frame(cfg: &Config, seq: u64, f_type: &str, extra_top: &Value, payload: &Value) -> Vec<u8> {
    let mut obj = serde_json::Map::new();
    obj.insert("type".into(), json!(f_type));
    obj.insert("node_id".into(), json!(cfg.node_id));
    obj.insert("seq".into(), json!(seq));
    if let Some(o) = extra_top.as_object() {
        for (k, v) in o {
            obj.insert(k.clone(), v.clone());
        }
    }
    obj.insert("payload".into(), payload.clone());
    let bytes = serde_json::to_vec(&Value::Object(obj)).unwrap_or_default();
    let mut out = Vec::with_capacity(4 + bytes.len());
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&bytes);
    out
}

fn encode_event(cfg: &Config, seq: u64, room_id: &str, evt: &str, payload: &Value) -> Vec<u8> {
    encode_frame(cfg, seq, "event", &json!({"room_id": room_id, "evt": evt}), payload)
}

fn encode_snapshot(
    cfg: &Config,
    seq: u64,
    room_id: &str,
    phase: &str,
    host_id: i64,
    players: &[Value],
    locked: bool,
    chart_id: i64,
) -> Vec<u8> {
    let payload = json!({
        "state": {
            "phase": phase,
            "host_id": host_id,
            "players": players,
            "locked": locked,
            "chart_id": chart_id,
        }
    });
    encode_frame(cfg, seq, "snapshot", &json!({"room_id": room_id}), &payload)
}

/// 尝试从字节缓冲中取出完整帧（<4 字节长度><JSON 体>）。
/// 返回 (完整帧列表, 剩余缓冲)。
fn decode_frames(buf: &[u8]) -> (Vec<Value>, Vec<u8>) {
    let mut frames = Vec::new();
    let mut rest = buf;
    loop {
        if rest.len() < 4 {
            break;
        }
        let len = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
        if len == 0 || len > MAX_FRAME_BYTES {
            // 非法长度：丢弃剩余缓冲，防止死循环
            return (frames, Vec::new());
        }
        if rest.len() < 4 + len {
            break;
        }
        let body = &rest[4..4 + len];
        match serde_json::from_slice::<Value>(body) {
            Ok(v) => frames.push(v),
            Err(_) => { /* 丢弃坏帧 */ }
        }
        rest = &rest[4 + len..];
    }
    (frames, rest.to_vec())
}

// ── 发送 ─────────────────────────────────────────────────────────────

fn send_frame(handle: u64, frame: &[u8]) {
    if let Err(e) = phira_tcp::send(handle, frame) {
        log("warn", &format!("tcp send handle={handle} 失败: {e}"));
    }
}

fn broadcast_except(s: &State, except: Option<u64>, frame: &[u8]) {
    for (h, peer) in &s.conns {
        if peer.lost {
            continue;
        }
        if Some(*h) == except {
            continue;
        }
        send_frame(*h, frame);
    }
}

fn next_seq(s: &mut State) -> u64 {
    s.seq += 1;
    s.seq
}

fn hello_payload(s: &State) -> Value {
    json!({
        "protocol": PROTOCOL,
        "roles": [s.cfg.role],
        "rooms": [],
    })
}

/// 本地房间事件 → PDFP event 帧，发往所有已连接 peer。
fn forward_room_event(room_id: &str, evt: &str, payload: &Value) {
    with_state(|s| {
        if s.conns.is_empty() {
            return;
        }
        let seq = next_seq(s);
        let frame = encode_event(&s.cfg, seq, room_id, evt, payload);
        broadcast_except(s, None, &frame);
    });
}

fn connect_peer(s: &mut State, addr: &str) {
    match phira_tcp::connect(addr) {
        Ok(handle) => {
            log("info", &format!("federated TCP connect {addr} → handle {handle}"));
            s.conns.insert(
                handle,
                Peer {
                    addr: addr.to_string(),
                    node_id: String::new(),
                    role: String::new(),
                    outbound: true,
                    last_seen_ms: now_ms(),
                    last_hb_ms: 0,
                    recv_buf: Vec::new(),
                    lost: false,
                },
            );
            let seq = next_seq(s);
            let frame = encode_frame(
                &s.cfg,
                seq,
                "hello",
                &json!({"role": s.cfg.role}),
                &hello_payload(s),
            );
            send_frame(handle, &frame);
        }
        Err(e) => {
            log("warn", &format!("connect {addr} 失败: {e}，稍后重试"));
            s.retry.insert(addr.to_string());
        }
    }
}

// ── 心跳 / 超时 / 重连（尽力而为 + timer 预留）──────────────────────

fn arm_heartbeat_timer(s: &mut State) {
    if s.timer_armed {
        return;
    }
    s.timer_armed = true;
    let _ = phira_timer::set_timer(HEARTBEAT_INTERVAL_MS, "pdfp-heartbeat");
}

fn maintenance() {
    let now = now_ms();
    let mut sends: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut closes: Vec<u64> = Vec::new();

    {
        let mut guard = STATE.lock().unwrap_or_else(|e| e.into_inner());
        if guard.is_none() {
            *guard = Some(State::default());
        }
        let s = guard.as_mut().unwrap();
        let handles: Vec<u64> = s.conns.keys().cloned().collect();
        for h in handles {
            if s.conns.get(&h).map(|p| p.lost).unwrap_or(true) {
                continue;
            }
            let (last_hb, last_seen, _outbound, node_id, addr) = {
                let p = s.conns.get(&h).unwrap();
                (p.last_hb_ms, p.last_seen_ms, p.outbound, p.node_id.clone(), p.addr.clone())
            };
            // 心跳：≥15s 未发则补发
            if now.saturating_sub(last_hb) >= HEARTBEAT_INTERVAL_MS {
                let seq = next_seq(s);
                let frame = encode_frame(&s.cfg, seq, "heartbeat", &json!({}), &json!({}));
                sends.push((h, frame));
                if let Some(p) = s.conns.get_mut(&h) {
                    p.last_hb_ms = now;
                }
            }
            // 失联：≥30s 无心跳标记；出站连接关闭并加入重连
            if now.saturating_sub(last_seen) >= HEARTBEAT_TIMEOUT_MS {
                if let Some(p) = s.conns.get_mut(&h) {
                    if !p.lost {
                        p.lost = true;
                        s.lost_nodes.insert(node_id.clone());
                        log("warn", &format!("peer {node_id} @ {addr} 失联（30s 无心跳）"));
                    }
                    if p.outbound {
                        closes.push(h);
                        s.retry.insert(addr.clone());
                    }
                }
            }
        }
        // 重连（15s 节流）
        if now.saturating_sub(s.last_retry_ms) >= RETRY_INTERVAL_MS {
            s.last_retry_ms = now;
            let pending: Vec<String> = s.retry.drain().collect();
            for addr in pending {
                connect_peer(s, &addr);
            }
        }
        arm_heartbeat_timer(s);
    }

    for (h, f) in sends {
        send_frame(h, &f);
    }
    for h in closes {
        let _ = phira_tcp::close(h);
        with_state(|s| {
            s.conns.remove(&h);
        });
    }
}

// ── 远端事件应用（远程玩家 API）──────────────────────────────────────

fn lookup_user_name(user_id: u32) -> String {
    match host_api("user_name", &[json!(user_id)]) {
        Ok(v) => v.get("name").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        Err(_) => String::new(),
    }
}

/// 房间不存在则先创建空房间（幂等）。
fn ensure_room(room_id: &str) {
    if host_api("rooms.by_name", &[json!(room_id)]).is_ok() {
        return;
    }
    match phira_room_mgmt::create_empty_room(room_id, None) {
        ApiResult::Ok(_) => {
            log("info", &format!("自动创建空房间 {room_id}"));
        }
        ApiResult::Error(e) => {
            log("warn", &format!("创建空房间 {room_id} 失败: {e}"));
        }
    }
}

fn log_api(op: &str, room_id: &str, pid: u32, r: &ApiResult) {
    match r {
        ApiResult::Ok(_) => {
            log("info", &format!("{op} room={room_id} player={pid} ok"));
        }
        ApiResult::Error(e) => {
            log("warn", &format!("{op} room={room_id} player={pid}: {e}"));
        }
    }
}

fn apply_remote_event(room_id: &str, evt: &str, payload: &Value) {
    let pid = payload.get("player_id").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
    match evt {
        "join" => {
            if pid == 0 {
                return;
            }
            let name = payload
                .get("player_name")
                .and_then(|v| v.as_str())
                .unwrap_or("remote")
                .to_string();
            ensure_room(room_id);
            with_state(|s| {
                s.rooms
                    .entry(room_id.to_string())
                    .or_default()
                    .remote
                    .insert(pid, name.clone());
            });
            let r = phira_room_mgmt::add_remote_player(room_id, pid, &name);
            log_api("add_remote_player", room_id, pid, &r);
        }
        "leave" => {
            if pid == 0 {
                return;
            }
            with_state(|s| {
                if let Some(m) = s.rooms.get_mut(room_id) {
                    m.remote.remove(&pid);
                }
            });
            let r = phira_room_mgmt::remote_leave(room_id, pid);
            log_api("remote_leave", room_id, pid, &r);
        }
        "ready" => {
            if pid == 0 {
                return;
            }
            let r = phira_room_mgmt::remote_ready(room_id, pid);
            log_api("remote_ready", room_id, pid, &r);
        }
        "abort" => {
            if pid == 0 {
                return;
            }
            let r = phira_room_mgmt::remote_abort(room_id, pid);
            log_api("remote_abort", room_id, pid, &r);
        }
        "start" => {
            log("info", &format!("event start room={room_id}（无对应 remote API，仅记录）"));
        }
        "played" => {
            log("info", &format!("event played room={room_id}（结算应用留待后续）"));
        }
        "settle" => {
            log("info", &format!("event settle room={room_id}（结算应用留待后续）"));
        }
        other => {
            log("warn", &format!("未知 event 类型 {other} room={room_id}"));
        }
    }
}

/// 星形拓扑下把收到的 event 转发给除来源外的其他连接（hop 防环）。
fn relay_event(origin: u64, frame: &Value) {
    let hop = frame.get("hop").and_then(|v| v.as_u64()).unwrap_or(0);
    if hop >= MAX_RELAY_HOP {
        return;
    }
    let mut f2 = frame.clone();
    if let Some(o) = f2.as_object_mut() {
        o.insert("hop".into(), json!(hop + 1));
    }
    let bytes = serde_json::to_vec(&f2).unwrap_or_default();
    let mut framed = Vec::with_capacity(4 + bytes.len());
    framed.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    framed.extend_from_slice(&bytes);
    with_state(|s| {
        broadcast_except(s, Some(origin), &framed);
    });
}

fn apply_snapshot(frame: &Value) {
    let room_id = frame.get("room_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let payload = frame.get("payload").cloned().unwrap_or(json!({}));
    let state = payload.get("state").cloned().unwrap_or(json!({}));
    if room_id.is_empty() {
        return;
    }
    let phase = state.get("phase").and_then(|v| v.as_str()).unwrap_or("");
    ensure_room(&room_id);
    let players = state.get("players").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    for p in &players {
        let pid = p.get("player_id").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
        if pid == 0 {
            continue;
        }
        let name = p.get("player_name").and_then(|v| v.as_str()).unwrap_or("remote").to_string();
        with_state(|s| {
            s.rooms
                .entry(room_id.clone())
                .or_default()
                .remote
                .insert(pid, name.clone());
        });
        let r = phira_room_mgmt::add_remote_player(&room_id, pid, &name);
        if let ApiResult::Error(e) = r {
            // 已在房间 / 本地 session 冲突——快照重放时属预期，降级为 debug
            log("info", &format!("snapshot add {pid}@{room_id} 未应用: {e}"));
        }
    }
    log("info", &format!("应用快照 room={room_id} phase={phase} players={}", players.len()));
}

// ── 本地房间 ready 状态同步（react 式，补发 remote-ready）────────────

fn sync_room_readiness(room_id: &str) {
    let data = match host_api("rooms.by_name", &[json!(room_id)]) {
        Ok(v) => v.get("data").cloned().unwrap_or(json!({})),
        Err(_) => return,
    };
    let state = data.get("state").and_then(|v| v.as_str()).unwrap_or("");
    if state != "WAITING_FOR_READY" && state != "PLAYING" {
        return;
    }
    let users_info = data.get("users_info").and_then(|v| v.as_array()).cloned().unwrap_or_default();
    // PLAYING 时 ready_users 快照为空，但能开局说明所有本地玩家均已 ready；
    // WAITING_FOR_READY 时取权威 ready_users。
    let ready_users: HashSet<u32> = if state == "PLAYING" {
        users_info
            .iter()
            .filter_map(|u| u.get("id").and_then(|v| v.as_i64()))
            .map(|x| x as u32)
            .collect()
    } else {
        data.get("ready_users")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_i64())
                    .map(|x| x as u32)
                    .collect()
            })
            .unwrap_or_default()
    };

    let mut to_send: Vec<u32> = Vec::new();
    with_state(|s| {
        let m = s.rooms.entry(room_id.to_string()).or_default();
        for u in &users_info {
            let uid = u.get("id").and_then(|v| v.as_i64()).unwrap_or(0) as u32;
            if uid == 0 {
                continue;
            }
            // 远端玩家（我应用过的）不转发，避免回声环
            if m.remote.contains_key(&uid) {
                continue;
            }
            if ready_users.contains(&uid) {
                if !m.forwarded_ready.contains(&uid) {
                    to_send.push(uid);
                    m.forwarded_ready.insert(uid);
                }
            } else {
                m.forwarded_ready.remove(&uid);
            }
        }
    });
    for uid in to_send {
        forward_room_event(room_id, "ready", &json!({"player_id": uid}));
    }
}

// ── authority：proxy hello 后下发房间快照 ────────────────────────────

fn send_room_snapshots(target: u64) {
    let rooms = match host_api("rooms.list", &[]) {
        Ok(v) => v,
        Err(_) => return,
    };
    let arr = rooms.as_array().cloned().unwrap_or_default();
    let mut sent = 0usize;
    for entry in arr.into_iter().take(MAX_SNAPSHOT_ROOMS) {
        let name = entry.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let data = entry.get("data").cloned().unwrap_or(json!({}));
        if name.is_empty() {
            continue;
        }
        let st = data.get("state").and_then(|v| v.as_str()).unwrap_or("");
        let phase = match st {
            "PLAYING" => "playing",
            "WAITING_FOR_READY" => "waiting_for_ready",
            _ => "select_chart",
        };
        let host_id = data.get("host").and_then(|v| v.as_i64()).unwrap_or(-1);
        let locked = data.get("locked").and_then(|v| v.as_bool()).unwrap_or(false);
        let chart_id = data.get("chart").and_then(|v| v.as_i64()).unwrap_or(0);
        let players: Vec<Value> = data
            .get("users_info")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|u| {
                json!({
                    "player_id": u.get("id").and_then(|v| v.as_i64()).unwrap_or(0),
                    "player_name": u.get("name").and_then(|v| v.as_str()).unwrap_or(""),
                })
            })
            .collect();
        with_state(|s| {
            let seq = next_seq(s);
            let frame = encode_snapshot(&s.cfg, seq, &name, phase, host_id, &players, locked, chart_id);
            send_frame(target, &frame);
        });
        sent += 1;
    }
    log("info", &format!("向 handle {target} 下发 {sent} 个房间快照"));
}

// ── 帧处理 ───────────────────────────────────────────────────────────

/// hello 声明的 node_id 是否在配置白名单内（未配置 node_id 则开放）。
fn node_id_known(node_id: &str) -> bool {
    with_state(|s| {
        let ids: Vec<&str> = s
            .cfg
            .peers
            .iter()
            .map(|p| p.node_id.as_str())
            .filter(|n| !n.is_empty())
            .collect();
        ids.is_empty() || ids.iter().any(|n| *n == node_id)
    })
}

fn handle_frame(origin: u64, frame: &Value) {
    let ftype = frame.get("type").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let node_id = frame.get("node_id").and_then(|v| v.as_str()).unwrap_or("").to_string();
    match ftype.as_str() {
        "hello" => {
            let is_known = node_id_known(&node_id);
            with_state(|s| {
                if let Some(p) = s.conns.get_mut(&origin) {
                    p.node_id = node_id.clone();
                    p.role = frame.get("role").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    log("info", &format!("hello 来自 {}({}) @ {}", p.node_id, p.role, p.addr));
                }
            });
            // node_id 不在白名单 → 拒绝（§2.1 握手校验）
            if !is_known {
                log("warn", &format!("hello 声称 node_id={node_id} 不在白名单，关闭连接"));
                let _ = phira_tcp::close(origin);
                with_state(|s| {
                    s.conns.remove(&origin);
                });
                return;
            }
            // authority 收到 proxy hello：补发房间快照，让新 proxy 同步存量房间
            send_room_snapshots(origin);
        }
        "heartbeat" => {
            // last_seen 已在 feed_tcp_data 更新
        }
        "event" => {
            let room_id = frame.get("room_id").and_then(|v| v.as_str()).unwrap_or("");
            let evt = frame.get("evt").and_then(|v| v.as_str()).unwrap_or("");
            let payload = frame.get("payload").cloned().unwrap_or(json!({}));
            apply_remote_event(room_id, evt, &payload);
            // 星形 authority 向其他 proxy 转发
            relay_event(origin, frame);
        }
        "cmd" => {
            let cmd = frame.get("cmd").and_then(|v| v.as_str()).unwrap_or("");
            let room_id = frame.get("room_id").and_then(|v| v.as_str()).unwrap_or("");
            log("info", &format!("cmd {cmd} room={room_id}（v1 命令裁决模型留待后续）"));
        }
        "snapshot" => {
            apply_snapshot(frame);
        }
        other => {
            log("warn", &format!("未知帧类型 {other}"));
        }
    }
}

fn feed_tcp_data(handle: u64, bytes: &[u8]) {
    let frames: Vec<Value> = with_state(|s| {
        let peer = match s.conns.get_mut(&handle) {
            Some(p) => p,
            None => return Vec::new(),
        };
        peer.last_seen_ms = now_ms();
        peer.recv_buf.extend_from_slice(bytes);
        let (frames, rest) = decode_frames(&peer.recv_buf);
        peer.recv_buf = rest;
        frames
    });
    for f in frames {
        handle_frame(handle, &f);
    }
}

// ── TCP 事件（tcp:event）─────────────────────────────────────────────

fn address_matches(configured: &str, actual: &str) -> bool {
    if configured == actual {
        return true;
    }
    // 退而求其次：按端口匹配（联邦端口每节点唯一；主机名/IP 差异容忍）
    let c_port = configured.rsplit(':').next().and_then(|s| s.parse::<u16>().ok());
    let a_port = actual.rsplit(':').next().and_then(|s| s.parse::<u16>().ok());
    c_port.is_some() && c_port == a_port
}

fn whitelisted(addr: &str) -> bool {
    with_state(|s| {
        if s.cfg.peers.is_empty() {
            log("warn", &format!("接受未在白名单的连接 {addr}（peers 为空，开放模式）"));
            return true;
        }
        let ok = s.cfg.peers.iter().any(|p| address_matches(&p.address, addr));
        if !ok {
            log("warn", &format!("拒绝不在白名单的连接 {addr}"));
        }
        ok
    })
}

fn handle_tcp_event(evt: &str, payload: &Value) {
    match evt {
        "tcp:accept" => {
            let conn = payload.get("conn_handle").and_then(|v| v.as_u64()).unwrap_or(0);
            let addr = payload.get("remote_addr").and_then(|v| v.as_str()).unwrap_or("").to_string();
            if conn == 0 {
                return;
            }
            if !whitelisted(&addr) {
                let _ = phira_tcp::close(conn);
                return;
            }
            with_state(|s| {
                if s.conns.contains_key(&conn) {
                    return;
                }
                s.conns.insert(
                    conn,
                    Peer {
                        addr: addr.clone(),
                        node_id: String::new(),
                        role: String::new(),
                        outbound: false,
                        last_seen_ms: now_ms(),
                        last_hb_ms: 0,
                        recv_buf: Vec::new(),
                        lost: false,
                    },
                );
                log("info", &format!("接受入站连接 {addr} (handle {conn})"));
                let seq = next_seq(s);
                let frame = encode_frame(
                    &s.cfg,
                    seq,
                    "hello",
                    &json!({"role": s.cfg.role}),
                    &hello_payload(s),
                );
                send_frame(conn, &frame);
            });
        }
        "tcp:receive" => {
            let h = payload.get("handle").and_then(|v| v.as_u64()).unwrap_or(0);
            let bytes: Vec<u8> = payload
                .get("bytes")
                .and_then(|v| v.as_array())
                .map(|a| a.iter().filter_map(|x| x.as_u64()).map(|x| x as u8).collect())
                .unwrap_or_default();
            if h != 0 && !bytes.is_empty() {
                feed_tcp_data(h, &bytes);
            }
        }
        "tcp:disconnect" => {
            let h = payload.get("handle").and_then(|v| v.as_u64()).unwrap_or(0);
            let reason = payload.get("reason").and_then(|v| v.as_str()).unwrap_or("").to_string();
            with_state(|s| {
                if let Some(p) = s.conns.remove(&h) {
                    if p.outbound {
                        s.retry.insert(p.addr.clone());
                    }
                    log("info", &format!("连接断开 {}({}) @ {}: {}", p.node_id, p.role, p.addr, reason));
                }
            });
        }
        "tcp:error" => {
            let h = payload.get("handle").and_then(|v| v.as_u64()).unwrap_or(0);
            let err = payload.get("error").and_then(|v| v.as_str()).unwrap_or("");
            log("warn", &format!("tcp error handle={h}: {err}"));
        }
        other => {
            log("debug", &format!("未处理 tcp 事件 {other}"));
        }
    }
}

// ════════════════════════════════════════════════════════════════════
// Guest exports
// ════════════════════════════════════════════════════════════════════

impl Guest for PdfpPlugin {
    fn init() -> Result<(), String> {
        let cfg = load_config();
        log(
            "info",
            &format!(
                "PDFP Lite 插件初始化 node={} role={} peers={} listen={:?}",
                cfg.node_id,
                cfg.role,
                cfg.peers.len(),
                cfg.listen
            ),
        );
        with_state(|s| {
            s.cfg = cfg.clone();
        });

        if cfg.role == "authority" {
            if let Some(addr) = &cfg.listen {
                match phira_tcp::listen(addr) {
                    Ok(h) => {
                        with_state(|s| s.listener = Some(h));
                        log("info", &format!("authority 监听 {addr} (handle {h})"));
                    }
                    Err(e) => {
                        log("error", &format!("authority 监听 {addr} 失败: {e}"));
                        return Err(format!("listen {addr}: {e}"));
                    }
                }
            }
        }

        // proxy（或配了 peers 的节点）：连接对等节点
        if cfg.role == "proxy" {
            if cfg.peers.is_empty() {
                log("warn", "proxy 角色但 peers 为空，联邦连接不建立");
            } else {
                with_state(|s| {
                    for p in &cfg.peers {
                        connect_peer(s, &p.address);
                    }
                });
            }
        }

        with_state(|s| arm_heartbeat_timer(s));
        Ok(())
    }

    fn get_info() -> PluginInfo {
        PluginInfo {
            name: "pdfp-pmp-plugin".to_string(),
            version: "0.1.0".to_string(),
            author: "FireflyF09".to_string(),
            description: "PDFP Lite federation plugin (PMP↔PMP / PMP↔gooophira-mp)".to_string(),
        }
    }

    fn cleanup() {
        with_state(|s| {
            let handles: Vec<u64> = s.conns.keys().cloned().collect();
            for h in handles {
                let _ = phira_tcp::close(h);
            }
            if let Some(l) = s.listener {
                let _ = phira_tcp::close(l);
            }
            s.conns.clear();
            s.rooms.clear();
            s.retry.clear();
            s.lost_nodes.clear();
        });
    }

    fn on_event(event: PluginEvent) -> Result<bool, String> {
        maintenance();
        let mut handled = false;
        match event {
            PluginEvent::RoomJoin(info) => {
                let user_id = info.user_id;
                let room_id = info.room_id;
                let name = lookup_user_name(user_id);
                forward_room_event(
                    &room_id,
                    "join",
                    &json!({
                        "player_id": user_id,
                        "player_name": name,
                        "monitor": info.is_monitor,
                    }),
                );
                sync_room_readiness(&room_id);
                handled = true;
            }
            PluginEvent::RoomLeave(info) => {
                forward_room_event(&info.room_id, "leave", &json!({"player_id": info.user_id}));
                handled = true;
            }
            PluginEvent::GameStart(info) => {
                // 开局前补发本地玩家 ready（远端需要 remote-ready 才能开赛）
                sync_room_readiness(&info.room_id);
                forward_room_event(&info.room_id, "start", &json!({"player_id": info.user_id}));
                handled = true;
            }
            PluginEvent::GameEnd(info) => {
                let rec = &info.game_result;
                forward_room_event(
                    &info.room_id,
                    "played",
                    &json!({
                        "player_id": info.user_id,
                        "record": {
                            "user_id": rec.user_id,
                            "user_name": rec.user_name,
                            "score": rec.score,
                            "accuracy": rec.accuracy,
                            "perfect": rec.perfect,
                            "good": rec.good,
                            "bad": rec.bad,
                            "miss": rec.miss,
                            "max_combo": rec.max_combo,
                            "full_combo": rec.full_combo,
                        },
                    }),
                );
                handled = true;
            }
            PluginEvent::RoundComplete(info) => {
                forward_room_event(
                    &info.room_id,
                    "settle",
                    &json!({"chart_id": info.chart_id, "chart_name": info.chart_name}),
                );
                handled = true;
            }
            // RoomModify 提供额外 react 触发点：join/leave/lock 等事件后补查 ready 状态
            PluginEvent::RoomModify(info) => {
                sync_room_readiness(&info.room_id);
                handled = false; // 不声明处理，供其他插件继续消费
            }
            _ => {}
        }
        Ok(handled)
    }

    fn on_api(method: String, args: Vec<JsonValue>) -> ApiResult {
        match method.as_str() {
            "tcp:event" => {
                let evt = args
                    .get(0)
                    .and_then(|v| match v {
                        JsonValue::Text(s) => Some(s.clone()),
                        _ => None,
                    })
                    .unwrap_or_default();
                let payload = args
                    .get(1)
                    .map(wit_json_to_serde)
                    .unwrap_or(json!({}));
                handle_tcp_event(&evt, &payload);
                ApiResult::Ok(JsonValue::Null)
            }
            "timer:fired" => {
                // 宿主接通 timer 派发后的周期心跳入口
                maintenance();
                ApiResult::Ok(JsonValue::Null)
            }
            _ => ApiResult::Ok(JsonValue::Null),
        }
    }
}

// ════════════════════════════════════════════════════════════════════
// 单元测试（纯函数，不触碰宿主）
// ════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cfg() -> Config {
        Config {
            node_id: "node-a".into(),
            role: "authority".into(),
            listen: Some("0.0.0.0:7900".into()),
            peers: vec![PeerCfg { address: "127.0.0.1:7900".into(), node_id: "node-b".into() }],
        }
    }

    #[test]
    fn frame_roundtrip() {
        let c = cfg();
        let bytes = encode_event(&c, 7, "room-1", "join", &json!({"player_id": 42}));
        let body_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        assert_eq!(body_len, bytes.len() - 4);
        let (frames, rest) = decode_frames(&bytes);
        assert!(rest.is_empty());
        assert_eq!(frames.len(), 1);
        let f = &frames[0];
        assert_eq!(f["type"], "event");
        assert_eq!(f["seq"], 7);
        assert_eq!(f["node_id"], "node-a");
        assert_eq!(f["room_id"], "room-1");
        assert_eq!(f["evt"], "join");
        assert_eq!(f["payload"]["player_id"], 42);
    }

    #[test]
    fn frame_split_and_coalesce() {
        let c = cfg();
        let a = encode_event(&c, 1, "r", "join", &json!({"player_id": 1}));
        let b = encode_event(&c, 2, "r", "leave", &json!({"player_id": 2}));
        // 前半 + 后半
        let mid = a.len() / 2;
        let mut buf = Vec::new();
        buf.extend_from_slice(&a[..mid]);
        let (f0, rest) = decode_frames(&buf);
        assert!(f0.is_empty());
        buf = rest;
        buf.extend_from_slice(&a[mid..]);
        buf.extend_from_slice(&b);
        let (frames, rest) = decode_frames(&buf);
        assert!(rest.is_empty());
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0]["seq"], 1);
        assert_eq!(frames[1]["seq"], 2);
    }

    #[test]
    fn bad_length_drops_buffer() {
        let mut buf = vec![0xff, 0xff, 0xff, 0xff];
        buf.extend_from_slice(b"whatever");
        let (frames, rest) = decode_frames(&buf);
        assert!(frames.is_empty());
        assert!(rest.is_empty());
    }

    #[test]
    fn snapshot_encoding() {
        let c = cfg();
        let players = vec![json!({"player_id": 1, "player_name": "a"})];
        let bytes = encode_snapshot(&c, 3, "r", "playing", 1, &players, false, 10);
        let (frames, _) = decode_frames(&bytes);
        assert_eq!(frames[0]["type"], "snapshot");
        assert_eq!(frames[0]["payload"]["state"]["phase"], "playing");
        assert_eq!(frames[0]["payload"]["state"]["players"][0]["player_id"], 1);
    }

    #[test]
    fn address_matching() {
        assert!(address_matches("10.0.0.5:7900", "10.0.0.5:7900"));
        assert!(address_matches("10.0.0.5:7900", "10.0.0.9:7900"));
        assert!(!address_matches("10.0.0.5:7900", "10.0.0.9:7901"));
    }
}
