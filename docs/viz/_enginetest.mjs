// 校验：抽取 HTML 内嵌 JS 引擎，在 Node 中以 stub 跑通整条渲染路径，并比对 Python 参考实现。
import fs from "fs";
const html = fs.readFileSync("orderbook_soa.html", "utf8");
const m = html.match(/<script>([\s\S]*?)<\/script>/);
if (!m) { console.error("NO SCRIPT"); process.exit(1); }
let code = m[1];
// 末尾的 applyTo(0);render(); 保留即可（render 用 stub ctx）。并暴露最终态用于比对。
code += `\nglobalThis.__finalBook = (function(){ const b=new MiniBook(); for(let k=0;k<Math.min(STEPS.length,20);k++){ if(STEPS[k].mutate) STEPS[k].mutate(b);} return b; })();\n`;

// ---- stub DOM / canvas ----
const ctxStub = new Proxy({}, {
  get(t, p) {
    if (p === "roundRect") return function(){};
    if (typeof p === "string" && /^(scale|setTransform|clearRect|fillRect|beginPath|rect|fill|stroke|moveTo|lineTo|fillText|save|restore|arc|closePath|measureText)$/.test(p))
      return function(){ return p==="measureText"?{width:10}:undefined; };
    return t[p];
  },
  set(t, p, v) { t[p] = v; return true; }
});
const elStub = () => ({ style:{}, textContent:"", disabled:false, onclick:null, addEventListener(){} });
const cvStub = { width:0, height:0, style:{}, getContext(){ return ctxStub; }, addEventListener(){} };
globalThis.document = { getElementById(id){ return id==="cv"?cvStub:elStub(); } };
globalThis.window = { devicePixelRatio:1, addEventListener(){} };

// 运行
try { (0, eval)(code); }
catch (e) { console.error("JS 运行错误:", e); process.exit(2); }
console.log("✓ JS 语法/渲染路径跑通，无运行时错误");

const b = globalThis.__finalBook;
console.log("JS  最终态: free_chain=", JSON.stringify(b.free_chain()),
            " used=", b.used,
            " exec=", JSON.stringify(b.execQtys),
            " cum=", JSON.stringify(b.cumQuoteQtys),
            " lv=", JSON.stringify(Object.keys(b.sell.lvIdx).map(Number).sort((x,y)=>x-y)));

// ---- Python 参考（等价于 WALKTHROUGH 的累计变更，用 matchTaker 一次性撮合对比）----
import { execSync } from "child_process";
const pyBin = "/Users/felix/.workbuddy/binaries/python/envs/default/bin/python";
const out = execSync(pyBin + " _pyref.py", { cwd: "." }).toString().trim();
const [pyFree, pyUsed, pyExec, pyCum, pyLv] = JSON.parse(out);
console.log("PY  最终态: free_chain=", JSON.stringify(pyFree), "used=", pyUsed, "exec=", JSON.stringify(pyExec),
            "cum=", JSON.stringify(pyCum), "lvPrices=", JSON.stringify(pyLv));

const eq = (a,bb)=>JSON.stringify(a)===JSON.stringify(bb);
const ok = eq(b.free_chain(),pyFree) && b.used===pyUsed && eq(b.execQtys,pyExec) &&
          eq(b.cumQuoteQtys,pyCum) && eq(Object.values(b.sell.lvPrice).map(Number).sort((x,y)=>x-y), pyLv);
console.log(ok ? "\n✅ 引擎与 Python 参考完全等价（与 docs/code 一致）" : "\n❌ 引擎不一致！");
process.exit(ok?0:3);
