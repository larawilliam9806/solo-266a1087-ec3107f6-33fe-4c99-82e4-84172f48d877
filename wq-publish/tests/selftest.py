"""关键流程自测（不依赖 pytest，直接 python -I tests/selftest.py 运行）。

覆盖：
1. CSV 校验：中文表头、重复 (样本,项目)、负数/非数字/空值/缺列/缺批次号
2. 端到端：上传 → 预览(增/改/删/不变分类) → 发布 → 登记版本/行数 → 下游文件哈希一致
3. 并发：两个页面从同一版本发起，仅一个发布成功，另一个 409 须重新预览
4. 崩溃恢复：文件缺失、文件被改坏、遗留临时文件、游离文件、改名后未提交
5. 撤回语义：整批替换后旧行消失；会话取消/重复发布防护
6. 十进制精度：大整数/长尾小数全链路无损，数值不同的修订必显示变化
7. 旧版 REAL 库迁移：既有版本按已保存数值供数，遗留未确认预览作废并要求重传
"""
import hashlib
import io
import json
import os
import shutil
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from wq_publish.app import create_app
from wq_publish.storage import LIVE_FILENAME, PublishConflictError
from wq_publish.validation import render_csv, validate_csv

PASS, FAIL = 0, 0
EXAMPLE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                           "examples")


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name} {detail}")


def read_example(fname):
    with open(os.path.join(EXAMPLE_DIR, fname), "rb") as fh:
        return fh.read()


def upload(client, fname, batch_no="B1"):
    return client.post(
        "/preview",
        data={"batch_no": batch_no,
              "csv_file": (io.BytesIO(read_example(fname)), fname)},
        content_type="multipart/form-data",
    )


# ---------------- 1. 校验单元测试 ----------------
print("== 1. CSV 校验 ==")
rows, errs = validate_csv(read_example("batch_v1.csv"), "B1")
check("合法 CSV 通过并解析出 5 行", errs == [] and len(rows) == 5, str(errs))

rows, errs = validate_csv(read_example("batch_invalid.csv"), "B1")
check("重复(样本,项目)被拒", any("重复" in e for e in errs), str(errs))
check("负浓度被拒", any("负数" in e for e in errs), str(errs))
check("空浓度被拒", any("浓度为空" in e for e in errs), str(errs))
check("空项目被拒", any("检测项目为空" in e for e in errs), str(errs))

bad = "样本号,浓度\nS1,1.0\n".encode()
rows, errs = validate_csv(bad, "B1")
check("缺少检测项目列被拒", rows is None and any("表头" in e for e in errs), str(errs))

rows, errs = validate_csv(read_example("batch_v1.csv"), " ")
check("空批次号被拒", any("批次号" in e for e in errs), str(errs))

for text in ["S1,氨氮,nan", "S1,氨氮,inf", "S1,氨氮,abc"]:
    raw = f"样本号,检测项目,浓度\n{text}\n".encode()
    rows, errs = validate_csv(raw, "B1")
    check(f"非法浓度 {text!r} 被拒", rows is None and len(errs) > 0, str(errs))

raw_zero = "样本号,检测项目,浓度\nS1,氨氮,0\n".encode()
rows, errs = validate_csv(raw_zero, "B1")
check("零浓度（非负边界）通过", errs == [] and rows[0]["concentration"] == "0", str(errs))

# 1b 十进制精度：解析结果必须原样保留上传文本，不做 float 舍入
raw_precision = (
    "样本号,检测项目,浓度\n"
    "P1,氨氮,9007199254740993\n"
    "P2,COD,0.10000000000000001\n"
    "P3,总磷,123456789012345678901234567890\n"
    "P4,铅,0.00000000000000000001\n"
).encode()
rows, errs = validate_csv(raw_precision, "B1")
check("高精度 CSV 通过校验", errs == [] and len(rows) == 4, str(errs))
got = {r["sample_no"]: r["concentration"] for r in (rows or [])}
check("大整数 9007199254740993 不被舍入为 9007199254740992",
      got.get("P1") == "9007199254740993", str(got))
check("长尾小数 0.10000000000000001 不被折叠为 0.1",
      got.get("P2") == "0.10000000000000001", str(got))
check("超大整数与超小小数逐字符保留",
      got.get("P3") == "123456789012345678901234567890"
      and got.get("P4") == "0.00000000000000000001", str(got))
check("规范渲染 CSV 浓度列与上传文本一致",
      "9007199254740993" in render_csv(rows)
      and "0.10000000000000001" in render_csv(rows))

# ---------------- 2-5. 端到端（隔离临时目录） ----------------
tmp = tempfile.mkdtemp(prefix="wq-selftest-")
data_dir = os.path.join(tmp, "data")
down_dir = os.path.join(tmp, "downstream")
app = create_app(data_dir=data_dir, downstream_dir=down_dir)
client = app.test_client()

print("== 2. 首次发布 v1 ==")
resp = upload(client, "batch_v1.csv", "BATCH-001")
check("预览页 200 且显示增删改", resp.status_code == 200
      and "新增".encode() in resp.data and "撤回".encode() in resp.data)
import re
m = re.search(rb'/publish/([0-9a-f]+)', resp.data)
check("预览页含发布令牌", m is not None)
token1 = m.group(1).decode()

resp = client.post(f"/publish/{token1}")
check("确认发布后 302 回首页", resp.status_code == 302)
reg = app.storage.current_registry()
check("登记为 v1、5 行", reg["current_version"] == 1 and reg["row_count"] == 5,
      f"got {dict(reg) if reg else None}")
live = os.path.join(down_dir, LIVE_FILENAME)
check("下游 CSV 已生成", os.path.exists(live))
file_hash = hashlib.sha256(open(live, "rb").read()).hexdigest()
check("文件哈希与登记一致", file_hash == reg["content_hash"])
check("下游目录无临时文件残留",
      not [n for n in os.listdir(down_dir) if n.startswith(".tmp-")])
resp = client.get("/published.csv")
check("已发布 CSV 可下载", resp.status_code == 200 and b"S001" in resp.data)
resp = client.post(f"/publish/{token1}")
check("同一会话不可重复发布（返回 409 过期页）", resp.status_code == 409
      and app.storage.get_session(token1)["status"] == "published")

print("== 3. 修订预览的增/改/删/不变分类 ==")
resp = upload(client, "batch_v2_revision.csv", "BATCH-002")
body = resp.data.decode()
# v2 相对 v1：S004氨氮新增；S001氨氮、S002COD 修改；S002氨氮 撤回；S001总磷/S003总磷 不变
check("新增 1 行", "+1" in body and "S004" in body)
check("修改 2 行", "~2" in body and "1.25" in body and "35.0" in body)
check("撤回 1 行（S002 氨氮）", "−1" in body and "S002" in body)
check("不变 2 行", "不变项 2" in body)
token2 = re.search(rb'/publish/([0-9a-f]+)', resp.data).group(1).decode()
client.post(f"/publish/{token2}")
reg = app.storage.current_registry()
check("发布后登记为 v2、5 行", reg["current_version"] == 2 and reg["row_count"] == 5)
current = app.storage.current_rows()
check("撤回生效：S002 氨氮已不存在",
      not any(r["sample_no"] == "S002" and r["analyte"] == "氨氮" for r in current))
check("修改生效：S001 氨氮=1.30（保留上传文本 1.30）",
      any(r["sample_no"] == "S001" and r["analyte"] == "氨氮"
          and r["concentration"] == "1.30" for r in current))
file_hash = hashlib.sha256(open(live, "rb").read()).hexdigest()
check("v2 文件哈希与登记一致", file_hash == reg["content_hash"])

print("== 4. 同版本并发修订：只允许一个成功 ==")
r_a = upload(client, "batch_v1.csv", "CONCUR-A")
r_b = upload(client, "batch_v2_revision.csv", "CONCUR-B")
tok_a = re.search(rb'/publish/([0-9a-f]+)', r_a.data).group(1).decode()
tok_b = re.search(rb'/publish/([0-9a-f]+)', r_b.data).group(1).decode()
ok = client.post(f"/publish/{tok_a}")
check("A 发布成功 → v3", ok.status_code == 302
      and app.storage.current_registry()["current_version"] == 3)
conflict = client.post(f"/publish/{tok_b}")
check("B 发布被拒并返回 409 过期预览页", conflict.status_code == 409
      and "该预览已过期".encode() in conflict.data)
sess_b = app.storage.get_session(tok_b)
check("B 会话被标记 obsolete", sess_b["status"] == "obsolete")
reg_before = app.storage.current_registry()["current_version"]
retry = upload(client, "batch_v2_revision.csv", "CONCUR-B-RETRY")
tok_b2 = re.search(rb'/publish/([0-9a-f]+)', retry.data).group(1).decode()
client.post(f"/publish/{tok_b2}")
check("B 重新预览后可发布 → v4",
      app.storage.current_registry()["current_version"] == reg_before + 1)
check("A 的发布未被覆盖：v4 文件行数=5",
      app.storage.current_registry()["row_count"] == 5)

print("== 5. 崩溃后重启自动恢复 ==")
# 5a 下游文件被删除
os.unlink(live)
app2 = create_app(data_dir=data_dir, downstream_dir=down_dir)
reg = app2.storage.current_registry()
check("5a 文件缺失后重建且哈希与登记一致",
      os.path.exists(live)
      and hashlib.sha256(open(live, "rb").read()).hexdigest() == reg["content_hash"])

# 5b 文件被外部改坏
with open(live, "ab") as fh:
    fh.write(b"CORRUPTED\n")
app3 = create_app(data_dir=data_dir, downstream_dir=down_dir)
reg = app3.storage.current_registry()
check("5b 损坏文件被按登记重写",
      hashlib.sha256(open(live, "rb").read()).hexdigest() == reg["content_hash"])

# 5c 遗留半写临时文件
tmp_orphan = os.path.join(down_dir, ".tmp-publish-deadbeef.csv")
open(tmp_orphan, "wb").write(b"partial")
create_app(data_dir=data_dir, downstream_dir=down_dir)
check("5c 遗留临时文件被清理", not os.path.exists(tmp_orphan))

# 5d 下游目录出现未登记游离文件
stranger = os.path.join(down_dir, "stranger.csv")
open(stranger, "wb").write(b"x")
create_app(data_dir=data_dir, downstream_dir=down_dir)
check("5d 游离文件被移入 quarantine",
      not os.path.exists(stranger)
      and any(f.endswith("stranger.csv")
              for f in os.listdir(os.path.join(data_dir, "quarantine"))))

# 5e 模拟“文件已改名但事务未提交”：文件内容比登记新，重启应回滚到登记版本
with app3.storage._connect() as conn:
    old_csv = app3.storage._render_version(conn, reg["current_version"])
newer = render_csv([{"sample_no": "ZX", "analyte": "氨氮", "concentration": 9.9}])
app3.storage._atomic_write(newer)
check("预置：文件已被换成未登记的新版本",
      open(live, "rb").read().decode() != old_csv)
app4 = create_app(data_dir=data_dir, downstream_dir=down_dir)
check("5e 改名后未提交 → 恢复为登记的完整旧版",
      open(live, "rb").read().decode() == old_csv)
events = app4.storage.recent_events(50)
check("恢复动作写入事件日志", any(e["kind"] == "recover" for e in events))

print("== 6. 会话取消 ==")
r = upload(client, "batch_v1.csv", "CANCEL-ME")
tok = re.search(rb'/publish/([0-9a-f]+)', r.data).group(1).decode()
v_before = app.storage.current_registry()["current_version"]
client.post(f"/cancel/{tok}")
check("取消后发布被拒绝（409 且会话 cancelled）",
      client.post(f"/publish/{tok}").status_code == 409
      and app.storage.get_session(tok)["status"] == "cancelled")
check("取消不影响当前版本",
      app.storage.current_registry()["current_version"] == v_before)

# ---------------- 7. 十进制精度全链路 ----------------
print("== 7. 浓度精度：上传/预览/版本存储/发布文件/启动恢复 全链路无损 ==")
ptmp = tempfile.mkdtemp(prefix="wq-precision-")
pdata, pdown = os.path.join(ptmp, "data"), os.path.join(ptmp, "downstream")
papp = create_app(data_dir=pdata, downstream_dir=pdown)
pclient = papp.test_client()

precision_csv = (
    "样本号,检测项目,浓度(mg/L)\n"
    "P1,氨氮,9007199254740993\n"
    "P2,COD,0.10000000000000001\n"
).encode()
resp = pclient.post(
    "/preview",
    data={"batch_no": "PRECISION-1",
          "csv_file": (io.BytesIO(precision_csv), "precision.csv")},
    content_type="multipart/form-data",
)
check("高精度文件预览成功", resp.status_code == 200)
check("预览页原样显示 9007199254740993",
      b"9007199254740993" in resp.data and b"9007199254740992" not in resp.data)
check("预览页原样显示 0.10000000000000001",
      b"0.10000000000000001" in resp.data)
ptoken = re.search(rb'/publish/([0-9a-f]+)', resp.data).group(1).decode()
pclient.post(f"/publish/{ptoken}")

live_text = open(os.path.join(pdown, LIVE_FILENAME), "rb").read().decode()
check("发布文件逐字符保留 9007199254740993",
      "9007199254740993" in live_text and "9007199254740992" not in live_text)
check("发布文件逐字符保留 0.10000000000000001",
      "0.10000000000000001" in live_text)
stored = papp.storage.current_rows()
sval = {r["sample_no"]: r["concentration"] for r in stored}
check("版本库以精确文本存储（非 float）",
      sval.get("P1") == "9007199254740993"
      and sval.get("P2") == "0.10000000000000001", str(sval))
with papp.storage._connect() as conn:
    col_type = conn.execute(
        "SELECT type FROM pragma_table_info('results') WHERE name='concentration'"
    ).fetchone()["type"]
check("results.concentration 列为 TEXT", col_type.upper() == "TEXT", col_type)

# 数值不同的修订必须判为“修改”，即便差异小到 float 无法区分
revision_csv = (
    "样本号,检测项目,浓度(mg/L)\n"
    "P1,氨氮,9007199254740994\n"
    "P2,COD,0.10000000000000002\n"
).encode()
resp = pclient.post(
    "/preview",
    data={"batch_no": "PRECISION-2",
          "csv_file": (io.BytesIO(revision_csv), "revision.csv")},
    content_type="multipart/form-data",
)
body = resp.data.decode()
check("两个不同读数均判为修改（~2）", "~2" in body, body[:200])
check("修改区同时呈现旧值 9007199254740993 与新值 9007199254740994",
      "9007199254740993" in body and "9007199254740994" in body)
check("修改区同时呈现旧值 0.10000000000000001 与新值 0.10000000000000002",
      "0.10000000000000001" in body and "0.10000000000000002" in body)
rtoken = re.search(rb'/publish/([0-9a-f]+)', resp.data).group(1).decode()
pclient.post(f"/publish/{rtoken}")
sval2 = {r["sample_no"]: r["concentration"] for r in papp.storage.current_rows()}
check("修订发布后新读数精确入库",
      sval2.get("P1") == "9007199254740994"
      and sval2.get("P2") == "0.10000000000000002", str(sval2))

# 等价写法（0.100 与 0.1 数值相等）应判为“不变”，不产生虚假修改
equiv_csv = (
    "样本号,检测项目,浓度(mg/L)\n"
    "P1,氨氮,9007199254740994\n"
    "P2,COD,0.10000000000000002\n"
).encode()
resp = pclient.post(
    "/preview",
    data={"batch_no": "PRECISION-3",
          "csv_file": (io.BytesIO(equiv_csv), "equiv.csv")},
    content_type="multipart/form-data",
)
body = resp.data.decode()
check("数值未变（文本不同也）判为不变：~0 且不变项 2",
      "~0" in body and "不变项 2" in body, body[:200])

# 启动恢复：删除文件后按登记重建，精度不丢失
os.unlink(os.path.join(pdown, LIVE_FILENAME))
papp2 = create_app(data_dir=pdata, downstream_dir=pdown)
rebuilt = open(os.path.join(pdown, LIVE_FILENAME), "rb").read().decode()
reg = papp2.storage.current_registry()
check("恢复重建的文件保留全部精度且哈希与登记一致",
      "9007199254740994" in rebuilt and "0.10000000000000002" in rebuilt
      and hashlib.sha256(rebuilt.encode()).hexdigest() == reg["content_hash"])

# 常规重启不得误伤新建库中尚未确认的预览会话（仅旧版升级才作废遗留预览）
rtmp = tempfile.mkdtemp(prefix="wq-restart-")
rdata, rdown = os.path.join(rtmp, "data"), os.path.join(rtmp, "downstream")
rapp = create_app(data_dir=rdata, downstream_dir=rdown)
rresp = rapp.test_client().post(
    "/preview",
    data={"batch_no": "RESTART-1",
          "csv_file": (io.BytesIO(precision_csv), "precision.csv")},
    content_type="multipart/form-data",
)
rtok = re.search(rb'/publish/([0-9a-f]+)', rresp.data).group(1).decode()
rapp_restart = create_app(data_dir=rdata, downstream_dir=rdown)
check("重启后未确认预览仍为 pending（不被误作废）",
      rapp_restart.storage.get_session(rtok)["status"] == "pending")
shutil.rmtree(rtmp, ignore_errors=True)
shutil.rmtree(ptmp, ignore_errors=True)

# ---------------- 8. 旧版 REAL 数据库迁移 ----------------
print("== 8. 旧版 REAL 库升级：按已保存数值供数，遗留预览作废并要求重传 ==")
ltmp = tempfile.mkdtemp(prefix="wq-legacy-")
ldata, ldown = os.path.join(ltmp, "data"), os.path.join(ltmp, "downstream")
os.makedirs(ldata, exist_ok=True)
os.makedirs(ldown, exist_ok=True)
db_path = os.path.join(ldata, "wq_publish.db")
old_csv = (
    "sample_no,analyte,concentration\n"
    "L1,氨氮,9007199254740992.0\n"
    "L2,COD,0.1\n"
)
with open(os.path.join(ldown, LIVE_FILENAME), "w", encoding="utf-8", newline="") as fh:
    fh.write(old_csv)
old_hash = hashlib.sha256(old_csv.encode()).hexdigest()
# 手工构造升级前的 REAL 结构（含一条未确认预览，载荷里是舍入后的 float）
conn = sqlite3.connect(db_path)
conn.executescript(
    """
    CREATE TABLE registry (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        current_version INTEGER NOT NULL, batch_no TEXT NOT NULL,
        row_count INTEGER NOT NULL, published_at TEXT NOT NULL,
        content_hash TEXT NOT NULL
    );
    CREATE TABLE results (
        id INTEGER PRIMARY KEY AUTOINCREMENT, version INTEGER NOT NULL,
        batch_no TEXT NOT NULL, sample_no TEXT NOT NULL, analyte TEXT NOT NULL,
        concentration REAL NOT NULL CHECK (concentration >= 0),
        UNIQUE (version, sample_no, analyte)
    );
    CREATE TABLE revision_sessions (
        token TEXT PRIMARY KEY, batch_no TEXT NOT NULL, base_version INTEGER NOT NULL,
        payload TEXT NOT NULL, created_at TEXT NOT NULL, status TEXT NOT NULL
        CHECK (status IN ('pending','published','obsolete','cancelled'))
    );
    CREATE TABLE event_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL,
        kind TEXT NOT NULL, detail TEXT NOT NULL
    );
    """
)
conn.execute(
    "INSERT INTO registry VALUES (1,1,'OLD-BATCH',2,'2026-01-01T00:00:00+00:00',?)",
    (old_hash,),
)
conn.executemany(
    "INSERT INTO results(version,batch_no,sample_no,analyte,concentration)"
    " VALUES (1,'OLD-BATCH',?,?,?)",
    [("L1", "氨氮", 9007199254740992.0), ("L2", "COD", 0.1)],
)
legacy_payload = json.dumps([
    {"sample_no": "L1", "analyte": "氨氮", "concentration": 9007199254740992.0},
    {"sample_no": "L2", "analyte": "COD", "concentration": 0.1},
])
conn.execute(
    "INSERT INTO revision_sessions VALUES ('legacytoken','OLD-BATCH',1,?,"
    "'2026-01-01T00:00:00+00:00','pending')",
    (legacy_payload,),
)
conn.commit()
conn.close()

lapp = create_app(data_dir=ldata, downstream_dir=ldown)
lclient = lapp.test_client()
with lapp.storage._connect() as c2:
    col_type = c2.execute(
        "SELECT type FROM pragma_table_info('results') WHERE name='concentration'"
    ).fetchone()["type"]
check("迁移后浓度列变为 TEXT", col_type.upper() == "TEXT", col_type)
rows = lapp.storage.current_rows()
lval = {r["sample_no"]: r["concentration"] for r in rows}
check("既有版本按已保存数值供数（不推测丢失精度）",
      lval.get("L1") == "9007199254740992.0" and lval.get("L2") == "0.1", str(lval))
rebuilt = open(os.path.join(ldown, LIVE_FILENAME), "rb").read().decode()
check("旧下游 CSV 与已保存数值一致，无需改写（哈希仍匹配）",
      rebuilt == old_csv and lapp.storage.current_registry()["content_hash"] == old_hash)
sess = lapp.storage.get_session("legacytoken")
check("升级前未确认预览已标记 obsolete", sess["status"] == "obsolete")
check("作废原因提示重新上传", bool(sess.get("obsolete_reason")) and "重新上传" in sess["obsolete_reason"])
resp = lclient.post("/publish/legacytoken")
check("遗留预览发布被拒（409）且页面提示重新上传",
      resp.status_code == 409 and "重新上传".encode() in resp.data)
try:
    lapp.storage.publish("legacytoken")
    raised = False
except PublishConflictError:
    raised = True
check("存储层直接发布遗留会话同样拒绝", raised)
events = lapp.storage.recent_events(50)
check("迁移动作写入事件日志", any(e["kind"] == "migrate" for e in events))

# 重新上传精确读数后可正常发布，且相对旧值正确显示“修改”
new_csv = (
    "样本号,检测项目,浓度(mg/L)\n"
    "L1,氨氮,9007199254740993\n"
    "L2,COD,0.10000000000000001\n"
).encode()
resp = lclient.post(
    "/preview",
    data={"batch_no": "NEW-BATCH",
          "csv_file": (io.BytesIO(new_csv), "new.csv")},
    content_type="multipart/form-data",
)
body = resp.data.decode()
check("重新上传后旧舍入值与精确读数判为修改（~2）", "~2" in body, body[:200])
ntoken = re.search(rb'/publish/([0-9a-f]+)', resp.data).group(1).decode()
resp = lclient.post(f"/publish/{ntoken}")
check("重新上传的精确批次发布成功", resp.status_code == 302)
lval2 = {r["sample_no"]: r["concentration"] for r in lapp.storage.current_rows()}
check("新版本精确保存原始读数",
      lval2.get("L1") == "9007199254740993"
      and lval2.get("L2") == "0.10000000000000001", str(lval2))
shutil.rmtree(ltmp, ignore_errors=True)

shutil.rmtree(tmp, ignore_errors=True)
print(f"\n结果：{PASS} 通过，{FAIL} 失败")
sys.exit(1 if FAIL else 0)
