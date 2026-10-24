// 浏览器内测试 spec：在页面主世界执行，用 canvas.getImageData 做像素级断言。
// 注意：本文件不得包含反引号或 $( 序列（runner 以 JSON 字符串传给 agent-browser eval）。
(function(){
  var dpr = window.devicePixelRatio || 1;
  var cv = document.getElementById('cv');
  var ctx = cv.getContext('2d');
  function sample(cssX, cssY){
    var d = ctx.getImageData(Math.round(cssX*dpr), Math.round(cssY*dpr), 1, 1).data;
    return [d[0], d[1], d[2], d[3]];
  }
  function rowCy(i){ return rowY(i) + T.rowH/2; }
  function isYellow(p){ return p[0] > 190 && p[1] > 190 && p[2] < 110; }
  function isMorePurple(p, base){
    var baseExcess = base[1] - Math.min(base[0], base[2]);
    var purpExcess = p[1] - Math.min(p[0], p[2]);
    return purpExcess < baseExcess - 3;
  }
  function bandYellowAt(s){
    var n = 0;
    var x = colX(s) - 1;
    for (var yy = T.y; yy < T.y + T.headerH + 13*(T.rowH+T.rowGap); yy += 3){
      if (isYellow(sample(x, yy))) n++;
    }
    return n;
  }
  function hsOf(st){ // 与页面 hlOf 同语义：静态 hlSlot 优先，动态 hlOrder 查 orderIndex
    if (st.hlSlot !== null && st.hlSlot !== undefined) return st.hlSlot;
    if (st.hlOrder !== null && st.hlOrder !== undefined && book.orderIndex[st.hlOrder] !== undefined)
      return book.orderIndex[st.hlOrder];
    return null;
  }
  function pickIdle(skip){
    for (var s=0; s<CAP; s++){ if (s !== skip && book.orderIds[s] === 0) return s; }
    return (skip + 1) % CAP;
  }
  function pickNonHl(hlRows){
    var cand = ['next','prev','times','userIds','visibleQtys'];
    for (var k=0;k<cand.length;k++){ if (hlRows.indexOf(cand[k]) < 0) return cand[k]; }
    return 'next';
  }
  function sidePrices(side){ return Object.keys(side.lvIdx).map(function(k){ return Number(k); }).sort(function(a,b){ return a-b; }); }
  var rep = { pass: [], fail: [], steps: [] };
  function check(name, cond, detail){
    if (cond) rep.pass.push(name);
    else rep.fail.push(name + (detail ? ' :: ' + detail : ''));
  }
  var i, st, fc, rec, yc, other, ycO, idle, fIdx, base, purp, ring, yy2, anyY, s2, hs;
  for (i=0; i<STEPS.length; i++){
    goto(i);
    st = STEPS[i];
    hs = hsOf(st);
    fc = book.free_chain();
    rec = { i:i, hlSlot:hs, hlRows:(st.hlRows?st.hlRows.join(','):null), scan:!!st.scan,
            free_head:book.free_head, fcLen:fc.length, counter:document.getElementById('counter').textContent };
    rep.steps.push(rec);
    if (hs != null){
      yc = bandYellowAt(hs);
      check('S'+i+'-yellow@col'+hs, yc > 0, 'yc='+yc);
      other = (hs + 3) % CAP;
      ycO = bandYellowAt(other);
      check('S'+i+'-yellowOnly@col'+hs, ycO === 0, 'other='+other+' ycO='+ycO);
    } else {
      anyY = 0;
      for (s2=0; s2<CAP; s2++) anyY += bandYellowAt(s2);
      check('S'+i+'-noYellow', anyY === 0, 'anyY='+anyY);
    }
    if (st.hlRows){
      idle = pickIdle(hs == null ? -1 : hs);
      fIdx = FIELDS.indexOf(st.hlRows[0]);
      var sx = colX(idle) + T.colW - 14;
      var sy = rowCy(fIdx);
      goto(0); var baseC = sample(sx, sy);
      goto(i); var purpC = sample(sx, sy);
      check('S'+i+'-purpleBlend', isMorePurple(purpC, baseC), 'base='+baseC+' purp='+purpC);
    }
    if (st.scan){
      ring = 0;
      // 右侧面板列 x=RIGHT_X，簿面板 box.x=RIGHT_X-8；环左缘采样 RIGHT_X-7，纵向扫整个右列。
      var chh = parseInt(cv.style.height, 10) || 600;
      for (yy2=16; yy2 < chh-8; yy2+=4){ if (isYellow(sample(RIGHT_X-7, yy2))) ring++; }
      check('S'+i+'-scanRing'+(st.scanBook||'SELL'), ring > 0, 'ring='+ring);
    }
  }
  // ── 第一幕（act1 到 idx19）引擎断言 ──
  goto(0);
  var initFc = []; for (var q=0; q<CAP; q++) initFc.push(q);
  check('R2-initFreeChain', JSON.stringify(book.free_chain()) === JSON.stringify(initFc), JSON.stringify(book.free_chain()));
  check('R2-initFreeHead', book.free_head === 0);
  goto(19);
  var expFc = [3,0,2]; for (var q2=4; q2<CAP; q2++) expFc.push(q2);
  function arrCap(v){ var a=Array(CAP).fill(0); for (var k2=0;k2<v.length;k2++) a[k2]=v[k2]; return a; }
  var fin = { fc: book.free_chain(), exec: book.execQtys, cum: book.cumQuoteQtys, used: book.used,
              lv: sidePrices(book.sell) };
  check('R-finalFreeChain', JSON.stringify(fin.fc) === JSON.stringify(expFc), JSON.stringify(fin.fc));
  check('R-finalExec', JSON.stringify(fin.exec) === JSON.stringify(arrCap([1.5,0,0,0.5])), JSON.stringify(fin.exec));
  check('R-finalCum', JSON.stringify(fin.cum) === JSON.stringify(arrCap([105000,0,0,35000])), JSON.stringify(fin.cum));
  check('R-finalUsed', fin.used === 1, 'used='+fin.used);
  check('R-finalLv', JSON.stringify(fin.lv) === JSON.stringify([70001]), JSON.stringify(fin.lv));
  // orderIndex 判重/登记/清除/拒绝 引擎级断言（第一幕）
  goto(1);
  check('R-oi-missNoSideEffect', book.free_head===0 && Object.keys(book.orderIndex).length===0,
        'fh='+book.free_head+' oi='+JSON.stringify(Object.keys(book.orderIndex)));
  goto(3);
  check('R-oi-put9001', book.orderIndex[9001]===0 && Object.keys(book.orderIndex).length===1,
        'oi='+JSON.stringify(Object.keys(book.orderIndex)));
  goto(11);
  check('R-oi-gethitIntact', book.orderIndex[9003]===2 && book.orderIds[2]===9003,
        'slot='+book.orderIndex[9003]+' ord2='+book.orderIds[2]);
  goto(13);
  check('R-oi-remove9003', book.contains(9003)===false && book.orderIds[2]===0,
        'contains='+book.contains(9003)+' ord2='+book.orderIds[2]);
  function snapBook(){ return JSON.stringify({fc:book.free_chain(), oi:Object.keys(book.orderIndex).sort(),
      ord:book.orderIds, ex:book.execQtys, cum:book.cumQuoteQtys,
      sell:sidePrices(book.sell), buy:sidePrices(book.buy)}); }
  goto(18); var sA=snapBook();
  goto(19); var sB=snapBook();
  check('R-oi-rejectNoChange', sA===sB, '18='+sA+' 19='+sB);
  check('R-oi-rejectStillInBook', book.contains(9002)===true && book.orderIndex[9002]===1,
        'contains9002='+book.contains(9002)+' slot='+book.orderIndex[9002]);
  // ── 第二幕（act2）引擎级断言：TIF / 双簿 resting / 部分成交 / 批量撤单 ──
  goto(24); var a1=snapBook();
  goto(25); var a2=snapBook();
  check('R2-fokRejectNoChange', a1===a2, '24='+a1.slice(0,80)+' 25='+a2.slice(0,80));
  goto(25); a1=snapBook();
  goto(26); a2=snapBook();
  check('R2-postOnlyRejectNoChange', a1===a2, '25 vs 26 differ');
  goto(27);
  check('R2-postOnlyRestBuy', book.contains(9007)===true && sidePrices(book.buy).indexOf(70000)>=0,
        'contains9007='+book.contains(9007)+' buy='+JSON.stringify(sidePrices(book.buy)));
  goto(35);
  var s35=book.orderIndex[9103];
  check('R2-partialFillStays', !!s35 && Math.abs(book.execQtys[s35]-0.4)<1e-9 &&
        Math.abs((book.origQtys[s35]-book.execQtys[s35])-1.1)<1e-9,
        'slot='+s35+' exec='+(s35?book.execQtys[s35]:-1)+' rem='+(s35?(book.origQtys[s35]-book.execQtys[s35]):-1));
  goto(38);
  check('R2-restingBuy', book.contains(9009)===true && sidePrices(book.buy).indexOf(70003)>=0,
        'oi='+JSON.stringify(Object.keys(book.orderIndex).sort())+' buy='+JSON.stringify(sidePrices(book.buy)));
  goto(44);
  check('R2-u100Present', book.contains(9121)&&book.contains(9122)&&book.contains(9123),
        'oi='+JSON.stringify(Object.keys(book.orderIndex).sort()));
  goto(45);
  check('R2-batchCancelDone', !book.contains(9121)&&!book.contains(9122)&&!book.contains(9123)&&book.contains(9111),
        'oi='+JSON.stringify(Object.keys(book.orderIndex).sort()));
  goto(STEPS.length-1);
  check('R2-endUsed', book.used===3, 'used='+book.used);
  check('R2-endBuyPrices', JSON.stringify(sidePrices(book.buy))===JSON.stringify([70000]),
        JSON.stringify(sidePrices(book.buy)));
  check('R2-endSellPrices', JSON.stringify(sidePrices(book.sell))===JSON.stringify([70003,70004]),
        JSON.stringify(sidePrices(book.sell)));
  check('R2-endContain', book.contains(9007)&&book.contains(9105)&&book.contains(9111),
        'oi='+JSON.stringify(Object.keys(book.orderIndex).sort()));
  return rep;
})()
