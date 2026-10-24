//! 撮合引擎 Aeron UDP 压测发单器（Rust 版，替代 Java StressProducer）。
//!
//! 两种模式：
//! - 在线模式（默认）：边生成边发，与 Java 版行为一致；
//! - 池子模式（`--pool-size N [--go-file PATH]`）：先预生成 N 帧定长 OrderIn
//!   （引擎未启动时可用满资源），打印 `POOL_READY`，等待 go 文件后按目标 TPS
//!   重放——热路径每帧只写 8 字节 orderId（撤单再写 8 字节目标），其余纯 offer，
//!   注入端 CPU 占用降到极限，避免与撮合争抢资源污染观测。
//!
//! 帧编码为定长 SBE OrderIn（101 字节，见 `order_in.rs`），与 Java 编码器
//! 逐字节对齐（黄金帧单测）。输出格式兼容 `stress-test.sh` 的解析。

mod order_in;

use order_in::{OrderInFrame, FRAME_LEN};
use rusteron_client::*;
use rusteron_media_driver::{AeronDriver, AeronDriverContext};
use std::cell::Cell;
use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::ffi::CString;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering as AOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const SCALE: f64 = 1e8;
/// 入站 IPC 行布局（与 Java AeronWire 对齐）：8B 收单时刻 + 256B 帧区（101B 帧 + 零填充）。
const INBOUND_TS_LENGTH: usize = 8;
const INBOUND_FRAME_LENGTH: usize = 256;
const INBOUND_ROW_SIZE: usize = INBOUND_TS_LENGTH + INBOUND_FRAME_LENGTH;
/// 压测控制流（与 Java StreamIds.CONTROL_STREAM 对齐）：GO 信号替代 go 文件握手。
const STRESS_CONTROL_STREAM: i32 = 500_000;
/// 控制行类型（与 Java ControlStream 对齐）：1=READY（引擎进程启动完成，忽略），2=GO。
const CONTROL_TYPE_GO: i32 = 2;

struct Args {
    aeron_endpoint: String,
    profile: String,
    symbol: String,
    target_tps: u64,
    duration_secs: u64,
    base_price: f64,
    spread: f64,
    batch: u64,
    side: u8,
    depth: String,
    decay: f64,
    mid_vol: f64,
    tick: f64,
    /// 限价单平均寿命（秒）：限价单以 cancel_ratio 概率登记到期撤单，寿命指数分布
    /// （真实市场：GDAX 上 2/3 的 BTC 限价单 2 秒内被撤；SEC 口径美股 cancel-to-trade ≈ 23:1）。
    order_lifetime_secs: f64,
    batch_cancel_ratio: f64,
    /// 资金对账专用用户数（0=关闭）：userId 1..N 走确定性场景帧，其余仍随机 10 万。
    fund_users: usize,
    market_ratio: f64,
    stop_ratio: f64,
    iceberg_ratio: f64,
    large_ratio: f64,
    price_regime: bool,
    /// 池子模式：预生成帧数（0 = 在线生成）。
    pool_size: usize,
    /// 池子模式：等待该文件存在后再开始重放（引擎就绪信号；与 go_aeron_dir 二选一）。
    go_file: String,
    /// 池子模式（本地）：订阅共享媒体驱动的控制流等待 GO（替代 go 文件；远程 producer 仍走文件）。
    go_aeron_dir: String,
    /// IPC 直连模式：producer 直发 core 入站 IPC 环（跳过 ingress）。
    aeron_ipc: bool,
    /// IPC 模式共享媒体驱动目录（core 的 -Docx.aeron.dir）。
    aeron_dir: String,
    /// 发号方 ID（部署内唯一，1..=65535）：orderId 三段式高位前缀（docs/02-data-structure.md §2.3）。
    instance_id: u32,
}

fn parse_args() -> Args {
    let mut a = Args {
        aeron_endpoint: "localhost:11000".into(),
        profile: "spot".into(),
        symbol: "BTCUSDT".into(),
        target_tps: 10000,
        duration_secs: 60,
        base_price: 70000.0,
        spread: 50.0,
        batch: 100,
        side: 0,
        depth: "exp".into(),
        decay: 10.0,
        mid_vol: 1.0,
        tick: 0.01,
        order_lifetime_secs: 2.0,
        batch_cancel_ratio: 0.000001,
        fund_users: 0,
        market_ratio: 0.0,
        stop_ratio: 0.0,
        iceberg_ratio: 0.0,
        large_ratio: 0.0,
        price_regime: false,
        pool_size: 0,
        go_file: String::new(),
        go_aeron_dir: String::new(),
        aeron_ipc: false,
        aeron_dir: "/tmp/ocx-aeron".into(),
        instance_id: 1,
    };
    let mut i = 0;
    let argv: Vec<String> = std::env::args().skip(1).collect();
    while i < argv.len() {
        let arg = argv[i].clone();
        let mut val = || -> String {
            i += 1;
            argv.get(i).cloned().unwrap_or_default()
        };
        match arg.as_str() {
            "--aeron-endpoint" => a.aeron_endpoint = val(),
            "--profile" => a.profile = val(),
            "--symbol" => a.symbol = val(),
            "--tps" => a.target_tps = val().parse().unwrap_or(10000),
            "--duration" => a.duration_secs = val().parse().unwrap_or(60),
            "--price" => a.base_price = val().parse().unwrap_or(70000.0),
            "--spread" => a.spread = val().parse().unwrap_or(50.0),
            "--batch" => a.batch = val().parse().unwrap_or(100),
            "--side" => a.side = val().parse().unwrap_or(0),
            "--depth" => a.depth = val(),
            "--decay" => a.decay = val().parse().unwrap_or(10.0),
            "--mid-vol" => a.mid_vol = val().parse().unwrap_or(1.0),
            "--tick" => a.tick = val().parse().unwrap_or(0.01),
            "--order-lifetime-secs" => a.order_lifetime_secs = val().parse().unwrap_or(2.0),
            "--batch-cancel-ratio" => a.batch_cancel_ratio = val().parse().unwrap_or(0.000001),
            "--fund-users" => a.fund_users = val().parse().unwrap_or(0),
            "--market-ratio" => a.market_ratio = val().parse().unwrap_or(0.0),
            "--stop-ratio" => a.stop_ratio = val().parse().unwrap_or(0.0),
            "--iceberg-ratio" => a.iceberg_ratio = val().parse().unwrap_or(0.0),
            "--large-ratio" => a.large_ratio = val().parse().unwrap_or(0.0),
            "--price-regime" => a.price_regime = true,
            "--pool-size" => a.pool_size = val().parse().unwrap_or(0),
            "--go-file" => a.go_file = val(),
            "--go-aeron-dir" => a.go_aeron_dir = val(),
            "--aeron-ipc" => a.aeron_ipc = true,
            "--aeron-dir" => a.aeron_dir = val(),
            "--instance-id" => {
                let v = val().parse::<u32>().unwrap_or(1);
                if v == 0 || v as u64 > ISSUER_MAX {
                    eprintln!(
                        "[ocx-stress-producer] --instance-id 必须在 1..={}（部署内唯一发号方 ID）",
                        ISSUER_MAX
                    );
                    std::process::exit(2);
                }
                a.instance_id = v;
            }
            _ => {}
        }
        i += 1;
    }
    a
}

/// StreamIds.inbound(symbol) = 2000 + fnv1a32(symbol) % 30000（与 Java 一致）。
fn inbound_stream_id(symbol: &str) -> i32 {
    let mut h: u32 = 0x811c9dc5;
    for b in symbol.bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h &= 0x7fff_ffff;
    2000 + (h % 30_000) as i32
}

/// xorshift64* + 缓存 Box-Muller 高斯。
struct Rng {
    state: u64,
    gauss_cache: Option<f64>,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Rng { state: seed.max(1), gauss_cache: None }
    }

    #[inline]
    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    #[inline]
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    #[inline]
    fn next_gaussian(&mut self) -> f64 {
        if let Some(v) = self.gauss_cache.take() {
            return v;
        }
        let u1 = (self.next_f64() + 1e-12).min(1.0 - 1e-12);
        let u2 = self.next_f64();
        let r = (-2.0 * u1.ln()).sqrt();
        let th = 2.0 * std::f64::consts::PI * u2;
        self.gauss_cache = Some(r * th.sin());
        r * th.cos()
    }

    #[inline]
    fn next_usize(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

#[inline]
fn to_mantissa_exp(v: f64) -> (i64, i8) {
    ((v * SCALE).round() as i64, -8)
}

fn format_thousands(v: u64) -> String {
    let s = v.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// 一帧的生成结果：撤单（带目标 ID，调用方 ID 空间）或订单。
struct Gen {
    is_cancel: bool,
    is_batch_cancel: bool,
    cancel_target: i64,
    side: u8,
    type_code: u8,
    price: f64,
    qty: f64,
    merge_low: f64,
    visible_qty: f64,
    user_id: i64,
}

/// 到期撤单（2026-08-31 真实市场模型）：限价单登记一次到期撤单，一单一撤。
/// 按 expiry_frame 小顶堆排序（同帧按 target_id，保持确定性）。
struct PendingCancel {
    expiry_frame: i64,
    target_id: i64,
    user_id: i64,
}

impl Ord for PendingCancel {
    fn cmp(&self, other: &Self) -> Ordering {
        self.expiry_frame
            .cmp(&other.expiry_frame)
            .then(self.target_id.cmp(&other.target_id))
    }
}

impl PartialOrd for PendingCancel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PendingCancel {
    fn eq(&self, other: &Self) -> bool {
        self.expiry_frame == other.expiry_frame && self.target_id == other.target_id
    }
}

impl Eq for PendingCancel {}

/// 指数分布寿命（帧）：均值 order_lifetime_secs × target_tps。
/// 真实市场订单寿命为幂律/指数（GDAX 2/3 限价单 2s 内被撤），指数近似足够。
fn lifetime_frames(rng: &mut Rng, args: &Args) -> i64 {
    let mean_frames = (args.order_lifetime_secs * args.target_tps as f64).max(1.0);
    let u = rng.next_f64().max(1e-9);
    ((-u.ln()) * mean_frames) as i64 + 1
}

fn cancel_gen(target: i64, user: i64) -> Gen {
    Gen {
        is_cancel: true,
        is_batch_cancel: false,
        cancel_target: target,
        side: 0,
        type_code: 1,
        price: 0.0,
        qty: 0.0,
        merge_low: f64::NAN,
        visible_qty: f64::NAN,
        user_id: user,
    }
}

/// 订单模型：随机数、价格游走、到期撤单队列、上一笔状态（与 Java 版语义对齐）。
struct OrderModel {
    rng: Rng,
    mid: f64,
    anchor: f64,
    vol: f64,
    /// 到期撤单队列（小顶堆）：限价单登记一次，寿命到期发撤单（一单一撤）。
    pending: BinaryHeap<Reverse<PendingCancel>>,
}

impl OrderModel {
    fn new(args: &Args) -> Self {
        OrderModel {
            rng: Rng::new(SystemTimeSeed::now()),
            mid: args.base_price,
            anchor: args.base_price,
            vol: 1.0,
            pending: BinaryHeap::new(),
        }
    }

    /// 每批推进 mid（价格 regime / 均值回归）。
    fn advance_mid(&mut self, args: &Args) {
        if args.depth == "uniform" {
            return;
        }
        if args.price_regime {
            self.anchor += self.rng.next_gaussian() * 0.5;
            self.vol = (self.vol * (self.rng.next_gaussian() * 0.05).exp()).clamp(0.3, 5.0);
        }
        self.mid += self.rng.next_gaussian() * args.mid_vol * self.vol
            + (self.anchor - self.mid) * 0.002;
    }

    /// 当前帧到期撤单（若有）：返回 (目标 orderId, 目标 userId)，一单一撤。
    /// 真实市场撤单总是针对"自己还活着的单"；寿命窗口内的目标大概率在簿。
    fn next_cancel(&mut self, current_frame: i64) -> Option<(i64, i64)> {
        while let Some(Reverse(p)) = self.pending.peek() {
            if p.expiry_frame <= current_frame {
                let p = self.pending.pop().unwrap().0;
                return Some((p.target_id, p.user_id));
            }
            break;
        }
        None
    }

    /// 生成一笔普通订单（current_id 为调用方 ID 空间：在线=绝对单号，池子=槽位；
    /// current_frame 为生成帧序号，max_frame 为到期撤单登记的帧上限）。
    /// 撤单不再随机生成：限价单以 cancel_ratio 概率登记到期撤单（一单一撤）。
    fn next(&mut self, args: &Args, current_id: i64, current_frame: i64, max_frame: i64) -> Gen {
        // 批撤（status=99，按 userId 全撤）：低频运营操作，独立于单笔撤单比例。
        // 引擎 batchCancel 全簿扫描（O(在簿单数)），比例须极小（默认 1e-6 ≈ 每秒几次）。
        if args.batch_cancel_ratio > 0.0 && self.rng.next_f64() < args.batch_cancel_ratio {
            return Gen {
                is_cancel: false,
                is_batch_cancel: true,
                cancel_target: 0,
                side: 0,
                type_code: 1,
                price: 0.0,
                qty: 0.0,
                merge_low: f64::NAN,
                visible_qty: f64::NAN,
                user_id: (self.rng.next_u64() % 100_000) as i64,
            };
        }

        let order_side: u8 = if args.side == 0 {
            if self.rng.next_f64() < 0.5 { 0 } else { 1 }
        } else {
            args.side - 1
        };

        let roll = self.rng.next_f64();
        let order_type: u8 = if roll < args.market_ratio {
            2
        } else if roll < args.market_ratio + args.stop_ratio {
            3 + self.rng.next_usize(4) as u8
        } else {
            1
        };

        let large = args.large_ratio > 0.0 && self.rng.next_f64() < args.large_ratio;
        let qty = if large {
            1.0 + self.rng.next_f64() * 2.0
        } else {
            0.01 + self.rng.next_f64() * 0.1
        };

        let mut price = if order_type == 2 || order_type == 4 || order_type == 6 {
            0.0
        } else if args.depth == "uniform" {
            args.base_price + (self.rng.next_f64() * 2.0 - 1.0) * args.spread
        } else {
            let mut d = -args.decay * (1.0 - self.rng.next_f64()).ln();
            if d > args.spread {
                d = args.spread;
            }
            if order_side == 0 { self.mid - d } else { self.mid + d }
        };
        if args.tick > 0.0 && order_type != 2 && order_type != 4 && order_type != 6 {
            price = (price / args.tick).round() * args.tick;
        }

        let merge_low = if order_type >= 3 {
            let dist = (0.0005 + self.rng.next_f64() * 0.002) * self.mid;
            self.mid + if self.rng.next_f64() < 0.5 { -dist } else { dist }
        } else {
            f64::NAN
        };

        let visible_qty = if order_type == 1
            && args.iceberg_ratio > 0.0
            && self.rng.next_f64() < args.iceberg_ratio
        {
            qty * (0.2 + self.rng.next_f64() * 0.4)
        } else {
            f64::NAN
        };

        let user_id = (self.rng.next_u64() % 100_000) as i64;
        // 真实市场模型：每笔限价单登记一次到期撤单（一单一撤；做市商报价最终
        // 不是被撤就是被成交，不存在"永不撤"的残留单）。寿命指数分布：
        // - 在线模式（max_frame=i64::MAX）：精确指数寿命；
        // - 池子模式（max_frame=池尾）：寿命截断到池内剩余空间，且池尾订单的
        //   到期帧在剩余区间均匀分散（避免同帧堆积发不完）。
        if order_type == 1 {
            let max_d = (max_frame - current_frame).max(1);
            let frames = lifetime_frames(&mut self.rng, args);
            let d = if frames >= max_d {
                1 + self.rng.next_usize(max_d as usize) as i64
            } else {
                frames
            };
            self.pending.push(Reverse(PendingCancel {
                expiry_frame: current_frame + d,
                target_id: current_id,
                user_id,
            }));
        }

        Gen {
            is_cancel: false,
            is_batch_cancel: false,
            cancel_target: 0,
            side: order_side,
            type_code: order_type,
            price,
            qty,
            merge_low,
            visible_qty,
            user_id,
        }
    }
}

/// 把生成结果编码进帧（id 由调用方先行写入）。
fn encode_gen(frame: &mut OrderInFrame, g: &Gen) {
    frame.set_user_id(g.user_id);
    if g.price > 0.0 {
        let (m, e) = to_mantissa_exp(g.price);
        frame.set_price(m, e);
    } else {
        frame.clear_price();
    }
    let (qm, qe) = to_mantissa_exp(g.qty);
    frame.set_orig_qty(qm, qe);
    frame.clear_executed();
    if g.merge_low.is_finite() {
        let (m, e) = to_mantissa_exp(g.merge_low);
        frame.set_merge_low(m, e);
    } else {
        frame.clear_merge_low();
    }
    frame.set_side(g.side);
    frame.set_type_code(g.type_code);
    frame.set_time_in_force(0);
    frame.set_status(if g.is_cancel {
        2
    } else if g.is_batch_cancel {
        99
    } else {
        0
    });
    if g.visible_qty.is_finite() {
        let (m, e) = to_mantissa_exp(g.visible_qty);
        frame.set_visible_qty(m, e);
    } else {
        frame.clear_visible_qty();
    }
}

/// 池子帧：预编码帧 + 撤单目标相对回退槽位（-1 = 普通订单）。
struct PoolFrame {
    buf: [u8; FRAME_LEN],
    cancel_back: i32,
}

/// 资金对账专用用户确定性场景帧（2026-08-30，`--fund-users N`，userId=1..N）：
/// 池首固定槽位，每轮重放由重放循环改写 orderId（userId/价格/数量保持帧内不变）。
/// 场景覆盖：STP 同价双向对、跨多 maker 大单、批撤目标多档、冰山、市价、条件单、普通限价。
fn fund_check_frames(args: &Args) -> Vec<OrderInFrame> {
    let n = args.fund_users;
    let mut out = Vec::new();
    if n == 0 {
        return out;
    }
    let mut make = |user: i64, side: u8, price: f64, qty: f64, type_code: u8,
                    visible: f64, merge_low: f64| {
        let mut f = OrderInFrame::new(&args.symbol);
        f.set_user_id(user);
        if price > 0.0 {
            let (pm, pe) = to_mantissa_exp(price);
            f.set_price(pm, pe);
        } else {
            f.clear_price();
        }
        let (qm, qe) = to_mantissa_exp(qty);
        f.set_orig_qty(qm, qe);
        f.clear_executed();
        f.set_side(side);
        f.set_type_code(type_code);
        f.set_time_in_force(0);
        if visible.is_finite() {
            let (vm, ve) = to_mantissa_exp(visible);
            f.set_visible_qty(vm, ve);
        }
        if merge_low.is_finite() {
            let (mm, me) = to_mantissa_exp(merge_low);
            f.set_merge_low(mm, me);
        }
        f.set_status(0);
        f
    };
    let base = args.base_price;
    // user=1：同价 BUY+SELL（隔离价 base+5000，只与对方相交）→ 引擎 STP 跳过自成交
    if n >= 1 {
        let stp_price = base + 5000.0;
        out.push(make(1, 0, stp_price, 1.0, 1, f64::NAN, f64::NAN));
        out.push(make(1, 1, stp_price, 1.0, 1, f64::NAN, f64::NAN));
    }
    // user=2：大单 taker（吃多档 maker，验证累计成交额）
    if n >= 2 {
        out.push(make(2, 0, base - 50.0, 5.0, 1, f64::NAN, f64::NAN));
    }
    // user=3：5 档限价（批撤目标；池首另有 status=99 帧按 user 全撤）
    if n >= 3 {
        for i in 0..5 {
            out.push(make(3, 0, base - 100.0 + i as f64 * 40.0, 0.5, 1, f64::NAN, f64::NAN));
        }
    }
    // user=4：冰山（显示 0.1 / 隐藏 2.0）
    if n >= 4 {
        out.push(make(4, 1, base + 80.0, 2.0, 1, 0.1, f64::NAN));
    }
    // user=5：市价
    if n >= 5 {
        out.push(make(5, 0, 0.0, 1.0, 2, f64::NAN, f64::NAN));
    }
    // user=6：条件单（stop-limit，触发价 = base+210）
    if n >= 6 {
        out.push(make(6, 0, base + 200.0, 0.8, 3, f64::NAN, base + 210.0));
    }
    // user=7/8：普通限价
    if n >= 7 {
        out.push(make(7, 1, base + 120.0, 0.3, 1, f64::NAN, f64::NAN));
    }
    if n >= 8 {
        out.push(make(8, 0, base - 140.0, 0.4, 1, f64::NAN, f64::NAN));
    }
    out
}

/// 预生成订单池（引擎未启动时调用，可用满资源）。
fn generate_pool(args: &Args) -> Vec<PoolFrame> {
    let n = args.pool_size;
    let mut model = OrderModel::new(args);
    let mut pool: Vec<PoolFrame> = Vec::with_capacity(n);
    let mut i: i64 = 0;
    // 专用用户确定性场景帧（池首固定槽位，先于随机帧）
    for mut frame in fund_check_frames(args) {
        if (i as usize) >= n {
            break;
        }
        frame.set_id(i);
        pool.push(PoolFrame { buf: frame.into_bytes(), cancel_back: -1 });
        i += 1;
    }
    // 批撤专用帧（user=3）：池首，先撤上一轮遗留的 user=3 在簿单
    if args.fund_users >= 3 && (i as usize) < n {
        let mut frame = OrderInFrame::new(&args.symbol);
        frame.set_id(i);
        frame.set_user_id(3);
        frame.set_status(99);
        pool.push(PoolFrame { buf: frame.into_bytes(), cancel_back: -1 });
        i += 1;
    }
    while (i as usize) < n {
        model.advance_mid(args);
        let end = ((i as usize) + args.batch as usize).min(n);
        while (i as usize) < end {
            // 1) 到期撤单优先（一单一撤，2026-08-31 真实市场模型）：
            //    目标 = 寿命窗口内刚发的限价单（池内索引），撤单几乎必然命中
            if let Some((target, user)) = model.next_cancel(i) {
                let mut frame = OrderInFrame::new(&args.symbol);
                frame.set_id(target); // 占位，重放时改写为绝对单号
                encode_gen(&mut frame, &cancel_gen(target, user));
                let cancel_back = (i - target) as i32;
                pool.push(PoolFrame { buf: frame.into_bytes(), cancel_back });
                i += 1;
                continue;
            }
            // 2) 普通订单（限价/市价/条件/冰山；池子模式：ID 空间 = 槽位）
            let g = model.next(args, i, i, n as i64 - 1);
            let mut frame = OrderInFrame::new(&args.symbol);
            frame.set_id(i);
            encode_gen(&mut frame, &g);
            pool.push(PoolFrame { buf: frame.into_bytes(), cancel_back: -1 });
            i += 1;
        }
    }
    pool
}

fn connect(args: &Args) -> Result<AeronExclusivePublication, Box<dyn std::error::Error>> {
    let ctx = AeronContext::new()?;
    let channel;
    if args.aeron_ipc {
        // IPC 直连：连共享媒体驱动（core 的 -Docx.aeron.dir），不启动内嵌驱动。
        channel = "aeron:ipc".to_string();
        let dir = CString::new(args.aeron_dir.as_str())?;
        ctx.set_dir(dir.as_c_str())?;
    } else {
        let driver_dir = std::env::temp_dir().join(format!(
            "ocx-producer-aeron-{}",
            std::process::id()
        ));
        let driver_ctx = AeronDriverContext::new()?;
        let driver_dir_c = CString::new(driver_dir.to_str().unwrap())?;
        driver_ctx.set_dir(driver_dir_c.as_c_str())?;
        driver_ctx.set_dir_delete_on_shutdown(true)?;
        let (_driver_stop, _driver_handle) = AeronDriver::launch_embedded(driver_ctx.clone(), false);
        let ctx_dir = CString::new(driver_ctx.get_dir())?;
        ctx.set_dir(ctx_dir.as_c_str())?;
        channel = format!("aeron:udp?endpoint={}", args.aeron_endpoint);
    }
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;
    let channel_c = CString::new(channel.as_str())?;
    let publication = aeron
        .async_add_exclusive_publication(&channel_c, inbound_stream_id(&args.symbol))?
        .poll_blocking(Duration::from_secs(5))?;
    Ok(publication)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    let stream_id = inbound_stream_id(&args.symbol);
    let channel = if args.aeron_ipc {
        format!("aeron:ipc dir={}", args.aeron_dir)
    } else {
        format!("aeron:udp?endpoint={}", args.aeron_endpoint)
    };

    println!("=== 撮合引擎压测 (Rust) ===");
    println!("  endpoint: {}  stream={}", channel, stream_id);
    println!("  profile:  {}", args.profile);
    println!("  target:   {} TPS", format_thousands(args.target_tps));
    println!("  duration: {} s", args.duration_secs);
    println!("  price:    {:.2} (±{:.2})", args.base_price, args.spread);
    println!("  batch:    {}", args.batch);
    println!("  depth:    {}", args.depth);
    if args.pool_size > 0 {
        println!(
            "  pool:     {} frames (~{:.1} MB)",
            args.pool_size,
            args.pool_size as f64 * FRAME_LEN as f64 / 1_048_576.0
        );
    }
    let order_id_base = issuer_order_id_base(args.instance_id);
    println!(
        "  issuer:   instance_id={} seg={} prefix=0x{:07x}",
        args.instance_id,
        (order_id_base as u64) >> SEG_SHIFT,
        (order_id_base as u64) >> COUNTER_BITS
    );

    let sent = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let retries = Arc::new(AtomicU64::new(0));

    {
        let sent = sent.clone();
        let errors = errors.clone();
        let retries = retries.clone();
        let start = Instant::now();
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(2));
            let elapsed = start.elapsed().as_secs();
            let total = sent.load(AOrdering::Relaxed);
            let tps = if elapsed > 0 { total as f64 / elapsed as f64 } else { 0.0 };
            println!(
                "[{:3}s] sent={} errors={} retries={} avg_tps={:.0}",
                elapsed,
                format_thousands(total),
                format_thousands(errors.load(AOrdering::Relaxed)),
                format_thousands(retries.load(AOrdering::Relaxed)),
                tps
            );
        });
    }

    let run_elapsed = if args.pool_size > 0 {
        // ── 池子模式：预生成（引擎未启动，可用满资源）→ POOL_READY → 等 go → 重放 ──
        let mut pool = generate_pool(&args);
        println!("POOL_READY frames={}", pool.len());
        std::io::stdout().flush().unwrap();
        if !args.go_file.is_empty() {
            let go = Path::new(&args.go_file);
            let deadline = Instant::now() + Duration::from_secs(600);
            while !go.exists() {
                if Instant::now() > deadline {
                    eprintln!("[ocx-stress-producer] go-file 超时: {}", args.go_file);
                    std::process::exit(2);
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            println!("GO received, starting replay");
        } else if !args.go_aeron_dir.is_empty() {
            wait_go_aeron(&args)?;
        }
        let publication = connect(&args)?;
        replay_pool(
            &args,
            &publication,
            &mut pool,
            order_id_base,
            &sent,
            &retries,
        )
    } else {
        // ── 在线模式：边生成边发（与 Java 版一致） ──
        let publication = connect(&args)?;
        run_online(&args, &publication, order_id_base, &sent, &retries)
    };

    let elapsed_secs = run_elapsed.as_secs().max(1);
    let total = sent.load(AOrdering::Relaxed);
    println!("=== 压测完成 ===");
    println!("  总发送:   {} 条", format_thousands(total));
    println!("  总耗时:   {} 秒", elapsed_secs);
    println!("  实际 TPS: {}", format_thousands(total / elapsed_secs));
    println!("  发送失败: {}", format_thousands(errors.load(AOrdering::Relaxed)));
    println!("  背压退避: {}", format_thousands(retries.load(AOrdering::Relaxed)));
    Ok(())
}

/// 等待 Aeron 控制流 GO 信号（2026-08-30，替代 go 文件握手）：连接共享媒体驱动
/// （可能尚未启动，带重试），订阅 {@code STRESS_CONTROL_STREAM} 控制流，收到任意
/// 一条消息即放行重放。与脚本 {@code StressGoSignal} 配合，消除本地压测的文件 IO。
fn wait_go_aeron(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(600);
    // 媒体驱动可能还没启动：先等待 cnc.dat 存在
    let cnc_path = Path::new(&args.go_aeron_dir).join("cnc.dat");
    while !cnc_path.exists() {
        if Instant::now() > deadline {
            eprintln!("[ocx-stress-producer] 媒体驱动启动超时: {} (cnc.dat 未创建)", args.go_aeron_dir);
            std::process::exit(2);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let ctx = AeronContext::new()?;
    let dir = CString::new(args.go_aeron_dir.as_str())?;
    ctx.set_dir(&dir)?;
    let aeron = Aeron::new(&ctx)?;
    aeron.start()?;
    let channel = CString::new("aeron:ipc")?;
    // 媒体驱动/Archive 可能在 producer 之后才启动：订阅建立带重试
    let subscription = loop {
        if Instant::now() > deadline {
            eprintln!("[ocx-stress-producer] go-aeron 控制流订阅超时: {}", args.go_aeron_dir);
            std::process::exit(2);
        }
        match aeron.async_add_subscription(
            &channel,
            STRESS_CONTROL_STREAM,
            Handlers::no_available_image_handler(),
            Handlers::no_unavailable_image_handler(),
        ) {
            Ok(handle) => match handle.poll_blocking(Duration::from_secs(2)) {
                Ok(sub) => break sub,
                Err(_) => std::thread::sleep(Duration::from_millis(100)),
            },
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    };

    struct GoHandler {
        go: Cell<bool>,
    }
    impl AeronFragmentHandlerCallback for GoHandler {
        fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
            // 控制流同时承载引擎 READY 行：只认 GO 类型（[4B type][16B name] 行首 type=2）
            if buffer.len() >= 4
                && i32::from_le_bytes(buffer[0..4].try_into().unwrap_or([0; 4])) == CONTROL_TYPE_GO
            {
                self.go.set(true);
            }
        }
    }
    let handler = Handler::leak(GoHandler { go: Cell::new(false) });
    while !handler.go.get() {
        if Instant::now() > deadline {
            eprintln!("[ocx-stress-producer] go-aeron 等待超时");
            std::process::exit(2);
        }
        let _ = subscription.poll(Some(&handler), 8);
        std::thread::sleep(Duration::from_millis(10));
    }
    println!("GO received (aeron), starting replay");
    Ok(())
}

struct SystemTimeSeed;

impl SystemTimeSeed {
    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }
}

/// orderId Snowflake 化布局（2026-09-01，docs/02-data-structure.md §1.3 / §1.9）：
///   orderId  = (seg << 42) | (instance_id << COUNTER_BITS) | counter
///   seg      = 自 1970-01-01 起的小时数（单调时间高位，本进程启动时取一次）
///   counter  = 发号方内单调递增（1 起），**纯业务序号**（2026-09-01 起不再
///   内嵌槽位；引擎内部 orderId→slot 映射 + 槽位 freelist 落位）。
/// 总位宽 21+8+34=63，不越 i64 正数域。
///
/// 相比 08-31 三段式（随机 13 位启动纪元）：随机纪元是"单运行期 2^34 总量墙"
/// （10k TPS 约 19.9 天耗尽）；Snowflake 化把高位换成单调小时段，每段 2^34 单、
/// 段随自然时间/耗量提前推进（2^21 小时 ≈ 239 年），永不耗尽。压测进程重启
/// 同小时段内基址相同，依赖 Archive 清档（压测场景无历史单）；生产网关按
/// 同一规则持久化 (seg, counter) 水位续发（ocx-match-gateway OrderIdAllocator）。
const COUNTER_BITS: u32 = 34;
const ISSUER_BITS: u32 = 8;
const SEG_BITS: u32 = 21;
const ISSUER_MAX: u64 = (1u64 << ISSUER_BITS) - 1; // 255
const SEG_MASK: u64 = (1u64 << SEG_BITS) - 1;
const SEG_SHIFT: u32 = ISSUER_BITS + COUNTER_BITS; // 42

/// 当前小时段（自 1970-01-01 起；压测场景无历史单，时钟回拨按当前值即可）。
fn current_seg() -> u64 {
    (SystemTimeSeed::now() / 1_000_000_000 / 3_600) & SEG_MASK
}

/// 发号方基址：高位（时间段 << 8 | 发号方 ID）左移 COUNTER_BITS。
fn issuer_order_id_base(instance_id: u32) -> i64 {
    assert!(
        instance_id >= 1 && (instance_id as u64) <= ISSUER_MAX,
        "instance_id 必须在 1..={ISSUER_MAX}"
    );
    let prefix = (current_seg() << ISSUER_BITS) | instance_id as u64;
    (prefix << COUNTER_BITS) as i64
}

/// 单帧 offer；负返回（背压/未建连）退避重试，与 Java 版背压链语义一致。
fn offer(publication: &AeronExclusivePublication, buf: &[u8], retries: &AtomicU64) {
    let handler = Handlers::no_reserved_value_supplier_handler();
    while publication.offer(buf, handler) < 0 {
        retries.fetch_add(1, AOrdering::Relaxed);
        for _ in 0..1024 {
            std::hint::spin_loop();
        }
        std::thread::yield_now();
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_args() -> Args {
        Args {
            aeron_endpoint: "localhost:11000".into(),
            profile: "spot".into(),
            symbol: "BTCUSDT".into(),
            target_tps: 1_000_000,
            duration_secs: 60,
            base_price: 70000.0,
            spread: 50.0,
            batch: 100,
            side: 0,
            depth: "exp".into(),
            decay: 10.0,
            mid_vol: 1.0,
            tick: 0.01,
            order_lifetime_secs: 2.0,
            batch_cancel_ratio: 0.000001,
            fund_users: 0,
            market_ratio: 0.10,
            stop_ratio: 0.02,
            iceberg_ratio: 0.05,
            large_ratio: 0.02,
            price_regime: false,
            pool_size: 0,
            go_file: String::new(),
            go_aeron_dir: String::new(),
            aeron_ipc: false,
            aeron_dir: "/tmp/ocx-aeron".into(),
            instance_id: 1,
        }
    }

    #[test]
    fn batch_cancel_frame_encodes_status99_and_user_id() {
        let mut frame = OrderInFrame::new("BTCUSDT");
        let g = Gen {
            is_cancel: false,
            is_batch_cancel: true,
            cancel_target: 0,
            side: 0,
            type_code: 1,
            price: 0.0,
            qty: 0.0,
            merge_low: f64::NAN,
            visible_qty: f64::NAN,
            user_id: 4242,
        };
        encode_gen(&mut frame, &g);
        let bytes = frame.into_bytes();
        assert_eq!(bytes[83], 99, "status 字节（绝对偏移 83）应为 99");
        let uid = i64::from_le_bytes(bytes[20..28].try_into().unwrap());
        assert_eq!(uid, 4242, "userId 字节（绝对偏移 20..28）");
    }

    #[test]
    fn snowflake_order_id_layout() {
        let base = issuer_order_id_base(1);
        let base2 = issuer_order_id_base(2);
        assert!(base > 0 && base2 > 0, "id=0 不得出现（前缀非零）");
        assert_ne!(base, base2, "不同发号方基址必须不同");
        let prefix = (base as u64) >> COUNTER_BITS;
        let seg = (base as u64) >> SEG_SHIFT;
        assert_eq!(seg, current_seg(), "高位必须为当前小时段");
        // 段内前缀恒定 + 63 位正数域（覆盖 2^34 量级内的连续发号）
        for i in 1..=4_096i64 {
            let id = base + i;
            assert!(id > 0);
            assert_eq!((id as u64) >> COUNTER_BITS, prefix, "前缀必须恒定");
            assert_eq!((id as u64) >> SEG_SHIFT, seg, "时间段必须恒定（段内）");
            assert_eq!((id as u64 >> COUNTER_BITS) & 0xFF, 1, "issuer 位必须为发号方 ID");
        }
        // 跨发号方同 counter：全 id 不同
        for i in 0..4096i64 {
            assert_ne!(base + i, base2 + i);
        }
        // 63 位内不越 i64 正数域
        assert!(base > 0 && (base as u64) < (1u64 << 63));
    }

    #[test]
    fn cancel_target_keeps_prefix() {
        let base = issuer_order_id_base(7);
        let cursor = 1_000_000i64;
        let back = 42i64;
        let target = base + cursor - back + 1;
        assert_eq!(
            (target as u64) >> COUNTER_BITS,
            (base as u64) >> COUNTER_BITS,
            "撤单目标必须保持同一发号方前缀"
        );
        assert!(target > 0);
    }

    #[test]
    #[should_panic(expected = "instance_id")]
    fn instance_id_zero_rejected() {
        let _ = issuer_order_id_base(0);
    }

    #[test]
    #[should_panic(expected = "instance_id")]
    fn instance_id_over_max_rejected() {
        let _ = issuer_order_id_base(256);
    }

    /// 真实市场撤单模型：一单一撤（每目标至多一次）、目标必须是限价单、
    /// cancel_back = 池内距离（重放时命中同轮目标）、撤单数 ≈ 限价单数（每单必撤）。
    #[test]
    fn realistic_cancel_model_one_to_one() {
        let mut args = test_args();
        // 池子窗口 = pool/tps = 6s，寿命均值 2s（10 万帧）< 窗口 → 不截断
        args.pool_size = 300_000;
        args.target_tps = 50_000;
        let pool = generate_pool(&args);
        let mut limit_idx: std::collections::HashSet<i64> = std::collections::HashSet::new();
        let mut cancel_targets: std::collections::HashSet<i64> = std::collections::HashSet::new();
        let mut limit_cnt = 0usize;
        let mut cancel_cnt = 0usize;
        for (idx, f) in pool.iter().enumerate() {
            let status = f.buf[83];
            let ty = f.buf[81];
            if status == 2 {
                let target = i64::from_le_bytes(f.buf[12..20].try_into().unwrap());
                assert!(f.cancel_back > 0 && (f.cancel_back as i64) < args.pool_size as i64);
                assert_eq!(
                    f.cancel_back as i64,
                    idx as i64 - target,
                    "cancel_back 必须等于池内距离"
                );
                assert!(
                    cancel_targets.insert(target),
                    "一单一撤：目标 {} 被重复撤",
                    target
                );
                assert!(limit_idx.contains(&target), "撤单目标 {} 必须是限价单", target);
                cancel_cnt += 1;
            } else if ty == 1 {
                limit_idx.insert(idx as i64);
                limit_cnt += 1;
            }
        }
        // 撤单帧数 ≈ 限价单数（每限价单登记一次撤单；±2% 容差，池尾截断影响）
        let expect = limit_cnt as f64;
        let actual = cancel_cnt as f64;
        assert!(
            (actual - expect).abs() / expect < 0.02,
            "撤单比例偏离：limit={} cancel={} expect≈{:.0}",
            limit_cnt,
            cancel_cnt,
            expect
        );
    }

    /// 在线模式到期语义：全部限价单被撤且只撤一次，撤单按寿命窗口有序发出。
    #[test]
    fn online_cancel_expiry_ordering() {
        let mut args = test_args();
        args.market_ratio = 0.0;
        args.stop_ratio = 0.0;
        args.iceberg_ratio = 0.0;
        args.large_ratio = 0.0;
        let max_frame = 1_000_000;
        let mut model = OrderModel::new(&args);
        let mut pending: std::collections::HashSet<i64> = std::collections::HashSet::new();
        let mut cancelled: std::collections::HashSet<i64> = std::collections::HashSet::new();
        for frame in 0..=max_frame {
            if let Some((target, _user)) = model.next_cancel(frame) {
                assert!(pending.remove(&target), "撤单目标 {} 必须是此前登记的限价单", target);
                assert!(cancelled.insert(target), "重复撤 {}", target);
                continue;
            }
            let g = model.next(&args, frame, frame, max_frame);
            assert!(!g.is_cancel, "next 不再直接生成撤单");
            if g.type_code == 1 {
                pending.insert(frame);
            }
        }
        assert!(!cancelled.is_empty(), "应有撤单发生");
        // 指数长尾：少数寿命超循环范围的订单未到期属正常；比例须很小（<1%）
        assert!(
            pending.len() < (max_frame / 100) as usize,
            "长尾未撤比例过高: pending={}",
            pending.len()
        );
    }
}

/// 池子重放：热路径每帧只写 8 字节单号（撤单再写 8 字节目标），其余纯 offer。
fn replay_pool(
    args: &Args,
    publication: &AeronExclusivePublication,
    pool: &mut [PoolFrame],
    order_id_base: i64,
    sent: &AtomicU64,
    retries: &AtomicU64,
) -> Duration {
    let len = pool.len() as i64;
    let start = Instant::now();
    let interval = Duration::from_nanos(
        (1_000_000_000.0 / args.target_tps as f64 * args.batch as f64) as u64,
    );
    let mut next_batch = Instant::now();
    let mut cursor: i64 = 0;
    // IPC 直连：预分配 264B 入站行缓冲（8B 时刻 + 帧区），池子仍按 101B 帧存，重放时包行
    let mut row = [0u8; INBOUND_ROW_SIZE];

    loop {
        let now = Instant::now();
        if now < next_batch {
            let d = next_batch - now;
            if d > Duration::from_micros(100) {
                std::thread::sleep(d);
            }
            continue;
        }
        next_batch += interval;
        if args.duration_secs > 0 && start.elapsed() >= Duration::from_secs(args.duration_secs) {
            break;
        }
        for _ in 0..args.batch {
            let slot = (cursor % len) as usize;
            let f = &mut pool[slot];
            let id = order_id_base + cursor + 1;
            if f.cancel_back >= 0 {
                // 撤单目标 = 本轮该槽位订单的绝对单号
                let target = order_id_base + cursor - f.cancel_back as i64 + 1;
                f.buf[12..20].copy_from_slice(&target.to_le_bytes());
            } else {
                f.buf[12..20].copy_from_slice(&id.to_le_bytes());
            }
            sent.fetch_add(1, AOrdering::Relaxed);
            if args.aeron_ipc {
                row[INBOUND_TS_LENGTH..INBOUND_TS_LENGTH + FRAME_LEN].copy_from_slice(&f.buf);
                offer(publication, &row[..INBOUND_TS_LENGTH + FRAME_LEN], retries);
            } else {
                offer(publication, &f.buf, retries);
            }
            cursor += 1;
        }
    }
    start.elapsed()
}

/// 在线模式：与 Java 版一致的生成 + 发送。
fn run_online(
    args: &Args,
    publication: &AeronExclusivePublication,
    order_id_base: i64,
    sent: &AtomicU64,
    retries: &AtomicU64,
) -> Duration {
    let mut model = OrderModel::new(args);
    let start = Instant::now();
    let interval = Duration::from_nanos(
        (1_000_000_000.0 / args.target_tps as f64 * args.batch as f64) as u64,
    );
    let mut next_batch = Instant::now();
    let mut frame = OrderInFrame::new(&args.symbol);
    let mut row = [0u8; INBOUND_ROW_SIZE];

    loop {
        let now = Instant::now();
        if now < next_batch {
            let d = next_batch - now;
            if d > Duration::from_micros(100) {
                std::thread::sleep(d);
            }
            continue;
        }
        next_batch += interval;
        if args.duration_secs > 0 && start.elapsed() >= Duration::from_secs(args.duration_secs) {
            break;
        }
        model.advance_mid(args);
        for _ in 0..args.batch {
            let cur = sent.fetch_add(1, AOrdering::Relaxed) as i64;
            let id = order_id_base + cur;
            // 到期撤单优先（一单一撤，2026-08-31 真实市场模型）；否则生成普通订单
            let g = match model.next_cancel(cur) {
                Some((target, user)) => cancel_gen(target, user),
                None => model.next(args, id, cur, i64::MAX),
            };
            if g.is_cancel {
                if g.cancel_target == 0 {
                    continue; // 兼容分支（登记目标恒非零，理论不可达）
                }
                frame.set_id(g.cancel_target);
            } else {
                frame.set_id(id);
            }
            encode_gen(&mut frame, &g);
            if args.aeron_ipc {
                row[INBOUND_TS_LENGTH..INBOUND_TS_LENGTH + FRAME_LEN].copy_from_slice(frame.as_slice());
                offer(publication, &row[..INBOUND_TS_LENGTH + FRAME_LEN], retries);
            } else {
                offer(publication, frame.as_slice(), retries);
            }
        }
    }
    start.elapsed()
}
