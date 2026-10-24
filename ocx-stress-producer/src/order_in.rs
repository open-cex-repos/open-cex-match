//! OrderIn 定长 SBE 帧编码（与 Java IoCodec/OrderInEncoder 逐字节兼容）。
//!
//! 帧布局（全部小端，SBE 头 + 89 字节固定块，无变长字段）：
//! ```text
//! [0..4)   MAGIC "OCX2"
//! [4..6)   blockLength = 89 (u16)
//! [6..8)   templateId = 1 (u16)
//! [8..10)  schemaId = 102 (u16)
//! [10..12) version = 1 (u16，与 ocx-io-schema.xml messageSchema version 同步)
//! [12..20) id (i64)
//! [20..28) userId (i64)
//! [28..44) symbol[16] ASCII 右补零
//! [44..52) price.mantissa (i64)   [52] price.exponent (i8)
//! [53..61) origQty.mantissa (i64) [61] origQty.exponent (i8)
//! [62..70) executedQty.mantissa   [70] executedQty.exponent
//! [71..79) mergeLowPrice.mantissa [79] mergeLowPrice.exponent
//! [80]     side (u8) 0=BUY 1=SELL
//! [81]     typeCode (u8) 1=LIMIT 2=MARKET 3-6=条件
//! [82]     timeInForce (u8) 0=GTC
//! [83]     status (u8) 0 正常 / 2 撤单
//! [84..92) createTime (i64)
//! [92..100) visibleQty.mantissa [100] visibleQty.exponent
//! ```

pub const FRAME_LEN: usize = 101;

const OFF_ID: usize = 12;
const OFF_USER_ID: usize = 20;
const OFF_SYMBOL: usize = 28;
const OFF_PRICE_M: usize = 44;
const OFF_PRICE_E: usize = 52;
const OFF_QTY_M: usize = 53;
const OFF_QTY_E: usize = 61;
const OFF_EXEC_M: usize = 62;
const OFF_EXEC_E: usize = 70;
const OFF_MERGE_M: usize = 71;
const OFF_MERGE_E: usize = 79;
const OFF_SIDE: usize = 80;
const OFF_TYPE: usize = 81;
const OFF_TIF: usize = 82;
const OFF_STATUS: usize = 83;
const OFF_CREATE: usize = 84;
const OFF_VIS_M: usize = 92;
const OFF_VIS_E: usize = 100;

/// 固定 101 字节 OrderIn 帧，原地改字段（零分配）。
#[derive(Clone)]
pub struct OrderInFrame {
    buf: [u8; FRAME_LEN],
}

impl OrderInFrame {
    pub fn new(symbol: &str) -> Self {
        let mut buf = [0u8; FRAME_LEN];
        buf[0..4].copy_from_slice(b"OCX2");
        buf[4..6].copy_from_slice(&89u16.to_le_bytes());
        buf[6..8].copy_from_slice(&1u16.to_le_bytes());
        buf[8..10].copy_from_slice(&102u16.to_le_bytes());
        // schema 版本纪律：与 ocx-io-schema.xml version 严格同步；
        // 变更时须同步修改黄金帧 fixture（ocx-codec/src/test/resources/sbe-golden/）。
        buf[10..12].copy_from_slice(&1u16.to_le_bytes());
        let sym = symbol.as_bytes();
        let n = sym.len().min(16);
        buf[OFF_SYMBOL..OFF_SYMBOL + n].copy_from_slice(&sym[..n]);
        OrderInFrame { buf }
    }

    #[inline]
    fn set_i64(&mut self, off: usize, v: i64) {
        self.buf[off..off + 8].copy_from_slice(&v.to_le_bytes());
    }

    #[inline]
    fn set_decimal(&mut self, m_off: usize, e_off: usize, mantissa: i64, exponent: i8) {
        self.set_i64(m_off, mantissa);
        self.buf[e_off] = exponent as u8;
    }

    #[inline]
    pub fn set_id(&mut self, v: i64) {
        self.set_i64(OFF_ID, v);
    }

    #[inline]
    pub fn set_user_id(&mut self, v: i64) {
        self.set_i64(OFF_USER_ID, v);
    }

    #[inline]
    pub fn set_price(&mut self, mantissa: i64, exponent: i8) {
        self.set_decimal(OFF_PRICE_M, OFF_PRICE_E, mantissa, exponent);
    }

    #[inline]
    pub fn set_orig_qty(&mut self, mantissa: i64, exponent: i8) {
        self.set_decimal(OFF_QTY_M, OFF_QTY_E, mantissa, exponent);
    }

    #[inline]
    pub fn set_merge_low(&mut self, mantissa: i64, exponent: i8) {
        self.set_decimal(OFF_MERGE_M, OFF_MERGE_E, mantissa, exponent);
    }

    #[inline]
    pub fn set_visible_qty(&mut self, mantissa: i64, exponent: i8) {
        self.set_decimal(OFF_VIS_M, OFF_VIS_E, mantissa, exponent);
    }

    #[inline]
    pub fn clear_price(&mut self) {
        self.set_decimal(OFF_PRICE_M, OFF_PRICE_E, 0, 0);
    }

    #[inline]
    pub fn clear_executed(&mut self) {
        self.set_decimal(OFF_EXEC_M, OFF_EXEC_E, 0, 0);
    }

    #[inline]
    pub fn clear_merge_low(&mut self) {
        self.set_decimal(OFF_MERGE_M, OFF_MERGE_E, 0, 0);
    }

    #[inline]
    pub fn clear_visible_qty(&mut self) {
        self.set_decimal(OFF_VIS_M, OFF_VIS_E, 0, 0);
    }

    #[inline]
    pub fn set_side(&mut self, side: u8) {
        self.buf[OFF_SIDE] = side;
    }

    #[inline]
    pub fn set_type_code(&mut self, t: u8) {
        self.buf[OFF_TYPE] = t;
    }

    #[inline]
    pub fn set_time_in_force(&mut self, t: u8) {
        self.buf[OFF_TIF] = t;
    }

    #[inline]
    pub fn set_status(&mut self, s: u8) {
        self.buf[OFF_STATUS] = s;
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    /// 取走底层字节数组（池子预生成用）。
    pub fn into_bytes(self) -> [u8; FRAME_LEN] {
        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // 共享黄金帧 fixture：Java（IoCodec/OrderInEncoder，mvn test）与 Rust
    // （本文件，cargo test）共同断言同一份字节真值，防双端编码漂移。
    // 路径相对本文件：src/order_in.rs -> 仓库根 -> ocx-codec/src/test/resources。
    const GOLDEN_LIMIT_BUY: &str =
        include_str!("../../ocx-codec/src/test/resources/sbe-golden/orderin-limit-buy.hex");
    const GOLDEN_ICEBERG: &str =
        include_str!("../../ocx-codec/src/test/resources/sbe-golden/orderin-iceberg.hex");
    const GOLDEN_MARKET_SELL: &str =
        include_str!("../../ocx-codec/src/test/resources/sbe-golden/orderin-market-sell.hex");

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{:02x}", x)).collect()
    }

    #[test]
    fn golden_limit_buy_matches_java() {
        // 与 ocx-codec OrderInGoldenFrameTest 同源：id=7 userId=99 BTCUSDT
        // price=70000.12345678 qty=1.5
        let mut f = OrderInFrame::new("BTCUSDT");
        f.set_id(7);
        f.set_user_id(99);
        f.set_price(7_000_012_345_678, -8);
        f.set_orig_qty(150_000_000, -8);
        f.clear_executed();
        f.clear_merge_low();
        f.set_side(0);
        f.set_type_code(1);
        f.set_time_in_force(0);
        f.set_status(0);
        f.set_i64(OFF_CREATE, 0);
        f.clear_visible_qty();
        assert_eq!(hex(f.as_slice()), GOLDEN_LIMIT_BUY.trim());
    }

    #[test]
    fn golden_iceberg_matches_java() {
        // 与 ocx-codec OrderInGoldenFrameTest 同源：id=8 visibleQty=0.6
        let mut f = OrderInFrame::new("BTCUSDT");
        f.set_id(8);
        f.set_user_id(99);
        f.set_price(7_000_012_345_678, -8);
        f.set_orig_qty(150_000_000, -8);
        f.clear_executed();
        f.clear_merge_low();
        f.set_side(0);
        f.set_type_code(1);
        f.set_time_in_force(0);
        f.set_status(0);
        f.set_i64(OFF_CREATE, 0);
        f.set_visible_qty(60_000_000, -8);
        assert_eq!(hex(f.as_slice()), GOLDEN_ICEBERG.trim());
    }

    #[test]
    fn golden_market_sell_matches_java() {
        // 与 ocx-codec OrderInGoldenFrameTest 同源：id=9 side=SELL type=MARKET
        // price=null
        let mut f = OrderInFrame::new("BTCUSDT");
        f.set_id(9);
        f.set_user_id(99);
        f.clear_price();
        f.set_orig_qty(150_000_000, -8);
        f.clear_executed();
        f.clear_merge_low();
        f.set_side(1);
        f.set_type_code(2);
        f.set_time_in_force(0);
        f.set_status(0);
        f.set_i64(OFF_CREATE, 0);
        f.clear_visible_qty();
        assert_eq!(hex(f.as_slice()), GOLDEN_MARKET_SELL.trim());
    }
}
