import { execSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dir = dirname(fileURLToPath(import.meta.url));
const htmlPath = '/Users/felix/Windows-E-workspace/felix/open-cex-repos/open-cex-match/docs/viz/orderbook_soa.html';
const url = 'file://' + htmlPath;
const specPath = join(__dir, 'orderbook_spec.js');

function ab(args, inherit){
  return execSync('agent-browser ' + args, { encoding:'utf8', stdio: inherit ? 'inherit' : 'pipe' });
}
function check(name, cond, detail){
  if (cond) console.log('  PASS  ' + name);
  else { failed = true; console.log('  FAIL  ' + name + (detail ? '  :: got "' + detail + '"' : '')); }
}

let failed = false;
try {
  console.log('[open] ' + url);
  ab('open ' + JSON.stringify(url), true);
  ab('wait --load load', true);
  execSync('sleep 0.6');

  // 探针：确认页面主世界的全局符号可被 eval 访问
  const probe = ab('eval ' + JSON.stringify('JSON.stringify({book:typeof book,STEPS:typeof STEPS,goto:typeof goto,colX:typeof colX,T:typeof T,FIELDS:typeof FIELDS})'));
  console.log('[probe] ' + probe.trim());

  // 1) 浏览器内全套断言（像素 + 引擎状态）
  console.log('\n=== 浏览器内自动化断言 (agent-browser eval) ===');
  const out = ab('eval "$(cat ' + specPath + ')"');
  let raw = out.trim();
  // agent-browser 对 eval 返回对象有两种包裹形态：
  //  (a) 直接返回 JSON 对象字符串： {...}
  //  (b) 外层多套一层引号：        "{...}"
  // 统一处理：先尝试整体 JSON.parse；若失败，去掉首尾引号再 parse；再失败，截出最外层 {...}
  let rep;
  try {
    rep = JSON.parse(raw);
  } catch (e1) {
    try {
      if (raw.startsWith('"') && raw.endsWith('"')) raw = JSON.parse(raw);
      rep = JSON.parse(raw);
    } catch (e2) {
      const m = raw.match(/\{[\s\S]*\}/);
      if (!m) throw new Error('无法从 agent-browser 输出解析测试报告: ' + raw.slice(0, 200));
      rep = JSON.parse(m[0]);
    }
  }
  console.log('PASS: ' + rep.pass.length + '   FAIL: ' + rep.fail.length);
  if (rep.fail.length){
    failed = true;
    console.log('FAILURES:');
    rep.fail.forEach(f => console.log('  - ' + f));
  }
  console.log('逐步骤高亮/状态:');
  rep.steps.forEach(s => console.log('  S' + s.i + ' hlSlot=' + s.hlSlot + ' scan=' + s.scan +
    ' free_head=' + s.free_head + ' fcLen=' + s.fcLen + ' counter="' + s.counter + '"'));

  // 2) 真实 UI 交互（点击/键盘）
  // 注意：#controls 位于画布下方（y≈680），默认视口较矮时按钮在折叠区外，
  // 裸 click 会因落点偏移而失效；需先 scrollintoview 把控件滚入视口，真实鼠标点击才会命中。
  console.log('\n=== UI 交互测试 (真实点击/按键) ===');
  ab('eval ' + JSON.stringify('goto(0);'));
  ab('scrollintoview "#controls"', true);
  ab('click "#next"');
  ab('click "#next"');
  let c = ab('get text "#counter"').trim();
  check('UI-click-next-x2 -> 步骤 3 / 47', c === '步骤 3 / 47', c);
  ab('press "ArrowRight"');
  c = ab('get text "#counter"').trim();
  check('UI-press-ArrowRight -> 步骤 4 / 47', c === '步骤 4 / 47', c);
  ab('scrollintoview "#reset"', true);
  ab('click "#reset"');
  c = ab('get text "#counter"').trim();
  check('UI-click-reset -> 步骤 1 / 47', c === '步骤 1 / 47', c);

  // 3) 截图存档（供人工核对）
  console.log('\n=== 截图存档 ===');
  ab('eval ' + JSON.stringify('goto(3);'), true);
  ab('screenshot ' + JSON.stringify(join(__dir, 'shot_step3_put9001.png')), true);
  ab('eval ' + JSON.stringify('goto(27);'), true);
  ab('screenshot ' + JSON.stringify(join(__dir, 'shot_step27_buyRest.png')), true);
  ab('eval ' + JSON.stringify('goto(35);'), true);
  ab('screenshot ' + JSON.stringify(join(__dir, 'shot_step35_partial.png')), true);
  ab('eval ' + JSON.stringify('goto(45);'), true);
  ab('screenshot ' + JSON.stringify(join(__dir, 'shot_step45_batch.png')), true);
  console.log('  已保存 shot_step3_put9001 / shot_step27_buyRest / shot_step35_partial / shot_step45_batch');
} catch (e) {
  failed = true;
  console.error('RUNNER ERROR: ' + (e && e.message ? e.message : e));
} finally {
  ab('close', true);
}
console.log('\nRESULT: ' + (failed ? 'FAIL' : 'ALL PASS'));
process.exit(failed ? 1 : 0);
