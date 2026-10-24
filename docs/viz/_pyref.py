import sys; sys.path.insert(0, ".")
import json, orderbook_soa as M
b = M.MiniBook()
for oid, price, qty in [(9001, 70000, 1.5), (9002, 70001, 2.0), (9003, 70000, 1.0), (9004, 70000, 0.5)]:
    b.addOrder(oid, 0, M.DIR_SELL, price, qty, "t")
b.removeSlot(9003)
b.matchTaker(9005, 0, M.DIR_BUY, 70000, 2.0)
print(json.dumps([b.free_chain(), b.used, b.execQtys, b.cumQuoteQtys, sorted(b.lvPrice.values())]))
