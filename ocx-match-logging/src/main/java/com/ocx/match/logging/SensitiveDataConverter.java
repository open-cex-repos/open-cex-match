package com.ocx.match.logging;

import ch.qos.logback.classic.pattern.MessageConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

import java.util.regex.Pattern;

/**
 * Logback MessageConverter — 日志脱敏，在排版层拦截所有 log 输出。
 * <p>
 * 使用方式：在 logback-spring.xml 中将默认 %msg 替换为 %maskedMsg（见 logback-spring.xml 配置）。
 * 覆盖范围：所有 log.info/error/debug — Controller、Service、Kafka Consumer、异常堆栈中嵌入的敏感串。
 * <p>
 * 脱敏规则 (金融行业惯例)：
 * <pre>
 * 手机号:    138****5678     (保留前3后4)
 * 身份证:    320102********1234  (保留前6后4)
 * 银行卡:    6222******7890  (保留前4后4)
 * Email:     u***@domain.com    (保留首字母+域名)
 * 密码字段:    "password":"****" (完全遮盖)
 * 加密私钥:    "secretKey":"***"  (完全遮盖)
 * 钱包地址:    0x1234...5678     (保留前6后4)
 * Token:     eyJh***xxx      (保留前4后3)
 * </pre>
 *
 * @author Gu / GStack Engineering
 */
public class SensitiveDataConverter extends MessageConverter {

    /** 手机号: 13812345678 → 138****5678 — 用数字边界(?<!\d)...(?!\d)避免 \b 跨中英文歧义 */
    private static final Pattern MOBILE =
            Pattern.compile("(?<!\\d)(1[3-9]\\d)\\d{4}(\\d{4})(?!\\d)");

    /** 身份证字段: 仅匹配带字段名的 JSON，避免误伤 18/19 位雪花订单号/用户ID。保留前 6 后 4 */
    private static final Pattern ID_CARD =
            Pattern.compile(
                    "(\"(?:idCard|identityNo|idNo|certNo)\"\\s*:\\s*\")(\\d{6})\\d{8}(\\d{4})(\")",
                    Pattern.CASE_INSENSITIVE);

    /** 银行卡字段: 仅匹配带字段名的 JSON，避免误伤普通长数字。保留前 4 后 4 */
    private static final Pattern BANK_CARD =
            Pattern.compile(
                    "(\"(?:bankCard|bankCardNo|cardNo|cardNumber)\"\\s*:\\s*\")(\\d{4})\\d{4,11}(\\d{4})(\")",
                    Pattern.CASE_INSENSITIVE);

    /** Email: user@domain.com → u***@domain.com */
    private static final Pattern EMAIL =
            Pattern.compile("(?<![\\w.-])(\\w)[\\w.-]*(@[\\w.-]+\\.[a-zA-Z]{2,})(?![\\w.@-])");

    /** 密码: "password"/"pwd"/"passwd"/"payPassword":"xxx" → "****" (完全遮盖) */
    private static final Pattern PASSWORD_JSON =
            Pattern.compile(
                    "(\"(?:password|pwd|passwd|payPassword)\"\\s*:\\s*\")([^\"]*)(\")",
                    Pattern.CASE_INSENSITIVE);

    /** SecretKey 等密钥字段: 完全遮盖 */
    private static final Pattern SECRET_KEY =
            Pattern.compile(
                    "(\"(?:secretKey|secret|privateKey|private_key|accessKey|access_key|apiKey|api_key)\"\\s*:\\s*\")([^\"]*)(\")",
                    Pattern.CASE_INSENSITIVE);

    /** Token: eyJhbGciOiJIUzI1NiJ9.xxx → eyJh***xxx */
    private static final Pattern TOKEN =
            Pattern.compile(
                    "(?<![\\w.-])(eyJ[a-zA-Z0-9_-]{3})[a-zA-Z0-9_-]+([a-zA-Z0-9_-]{3})(?![\\w.-])");

    /** 加密钱包地址: 0x1234567890abcdef... → 0x1234...5678 (40+ 位 hex) */
    private static final Pattern WALLET_ADDR =
            Pattern.compile(
                    "(?<![a-fA-F0-9])(0x[a-fA-F0-9]{4})[a-fA-F0-9]{28,}([a-fA-F0-9]{4})(?![a-fA-F0-9])");

    private static final Pattern[] PATTERNS = {
        PASSWORD_JSON, // 1. 密码字段 → 完全遮盖
        SECRET_KEY, // 2. 密钥字段 → 完全遮盖
        WALLET_ADDR, // 3. 钱包地址
        ID_CARD, // 4. 身份证 (先于银行卡，避免误匹配)
        BANK_CARD, // 5. 银行卡
        MOBILE, // 6. 手机号
        TOKEN, // 7. JWT Token
        EMAIL, // 8. Email
    };

    private static final String[] REPLACEMENTS = {
        "$1****$3", // password
        "$1***$3", // secretKey (3 星区分于密码的 4 星)
        "$1...$2", // 钱包: 0x1234...5678
        "$1$2********$3$4", // 身份证字段: "idCard":"320102********1234"
        "$1$2******$3$4", // 银行卡字段: "bankCard":"6222******7890"
        "$1****$2", // 手机号: 138****5678
        "$1***$2", // Token: eyJh***xxx
        "$1***$2", // Email: u***@domain.com
    };

    /**
     * 手机号正则的必要条件：消息中存在恰好 11 位且以 1[3-9] 开头的连续数字段。
     * 手机号正则带 (?<!\d)/(?!\d) 边界，长数字串（如 19 位 orderId）内部不可能匹配，
     * 单次扫描即可短路——拒绝洪峰下每笔 WARN 都含长 orderId，实测 regex 占 AsyncAppender
     * ~23% CPU（250K 拒绝/s），此快路径把高音量日志的 regex 开销降为 O(n) 字符扫描。
     */
    private static boolean hasPhoneLikeRun(String s) {
        int n = s.length();
        int i = 0;
        while (i < n) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                i++;
                continue;
            }
            int start = i;
            while (i < n && s.charAt(i) >= '0' && s.charAt(i) <= '9') {
                i++;
            }
            if (i - start == 11) {
                char first = s.charAt(start);
                char second = s.charAt(start + 1);
                if (first == '1' && second >= '3' && second <= '9') {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 每个正则的"保守必要条件"快速检查（与 PATTERNS 一一对应）。
     * <p>
     * 正则若要匹配，其前置条件必真（如 Password 正则要求出现 '"'，Token 正则要求
     * 出现 "eyJ"）。用 O(n) 的 indexOf/char 扫描短路掉不可能命中的正则，
     * 避免每条日志都跑满 8 个正则（JFR 实测 regex 匹配占撮合引擎 CPU ~30%）。
     * <p>
     * 安全性：条件都是必要条件而非充分条件——条件为假则正则必然不匹配，
     * 因此短路不会造成任何漏脱敏，行为与逐条全跑完全一致。
     */
    private static boolean guard(int i, String msg) {
        switch (i) {
            case 0:
            case 1:
            case 3:
            case 4: // password/secretKey/idCard/bankCard: 要求 JSON 引号
                return msg.indexOf('"') >= 0;
            case 2: // wallet: 要求 "0x"
                return msg.indexOf("0x") >= 0;
            case 5: // mobile: 必须含数字
                return hasPhoneLikeRun(msg);
            case 6: // token: 要求 "eyJ"
                return msg.indexOf("eyJ") >= 0;
            case 7: // email: 要求 '@'
                return msg.indexOf('@') >= 0;
            default:
                return true;
        }
    }

    @Override
    public String convert(ILoggingEvent event) {
        String msg = super.convert(event);
        if (msg == null || msg.isEmpty()) {
            return msg;
        }
        for (int i = 0; i < PATTERNS.length; i++) {
            if (!guard(i, msg)) {
                continue; // 保守短路：必要条件不成立，正则必不匹配，跳过
            }
            msg = PATTERNS[i].matcher(msg).replaceAll(REPLACEMENTS[i]);
        }
        return msg;
    }
}
