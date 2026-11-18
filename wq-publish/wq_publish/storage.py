"""SQLite 登记 + 下游目录原子发布 + 启动一致性恢复。

设计要点
--------
* SQLite 是版本/行数登记的唯一真相源；下游目录中的 CSV 只是它的镜像。
* 发布在单个 ``BEGIN IMMEDIATE`` 事务内完成：先写同目录临时文件并 fsync，
  再 ``os.replace`` 原子改名（下游只会看到完整的旧文件或新文件），
  最后提交事务登记新版本。
* 提交后进程若在改名完成前崩溃：重启时按数据库登记重新生成 CSV，
  并清理遗留临时文件；登记里不存在的游离文件会被移入 quarantine 隔离。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from .validation import format_concentration, render_csv

LIVE_FILENAME = "published_results.csv"

# 升级前未确认预览会话的统一作废原因（这些会话的浓度已经过 float 舍入，
# 丢失的原始精度无法从已保存数值推测，必须重新上传）
LEGACY_SESSION_REASON = "该预览创建于浓度精度升级之前，浓度可能已被浮点舍入，请重新上传 CSV 后再发布"
STALE_SESSION_REASON = "预览会话超过 24 小时未确认，已自动作废"


class PublishConflictError(Exception):
    """确认发布时基线版本已过期，需要重新预览。"""


def _as_decimal(value) -> Decimal:
    """把浓度（十进制文本/Decimal/历史 float）转为 Decimal 再比较。

    历史 float 先经 ``str`` 再转 Decimal，按“已保存数值”的最短往返表示
    比较，不推测任何已丢失的原始精度。
    """
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):
        return Decimal(str(value))
    return Decimal(value)


def _is_publishable_concentration(text: str) -> bool:
    """发布前兜底校验：浓度必须是有限、非负的十进制文本。"""
    try:
        value = Decimal(text)
    except (InvalidOperation, ValueError):
        return False
    return value.is_finite() and value >= 0


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Storage:
    def __init__(self, data_dir: str, downstream_dir: str):
        self.data_dir = os.path.abspath(data_dir)
        self.downstream_dir = os.path.abspath(downstream_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(self.downstream_dir, exist_ok=True)
        self.db_path = os.path.join(self.data_dir, "wq_publish.db")
        self.live_path = os.path.join(self.downstream_dir, LIVE_FILENAME)
        self.quarantine_dir = os.path.join(self.data_dir, "quarantine")
        os.makedirs(self.quarantine_dir, exist_ok=True)
        self._init_db()
        self.recover()

    # ---------- 基础 ----------
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=15)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=15000")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS registry (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    current_version INTEGER NOT NULL,
                    batch_no TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    published_at TEXT NOT NULL,
                    content_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version INTEGER NOT NULL,
                    batch_no TEXT NOT NULL,
                    sample_no TEXT NOT NULL,
                    analyte TEXT NOT NULL,
                    concentration TEXT NOT NULL,
                    UNIQUE (version, sample_no, analyte)
                );
                CREATE INDEX IF NOT EXISTS idx_results_version
                    ON results(version);
                CREATE TABLE IF NOT EXISTS revision_sessions (
                    token TEXT PRIMARY KEY,
                    batch_no TEXT NOT NULL,
                    base_version INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('pending','published','obsolete','cancelled')),
                    obsolete_reason TEXT
                );
                CREATE TABLE IF NOT EXISTS event_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL
                );
                """
            )
            self._migrate_legacy_schema(conn)

    def _migrate_legacy_schema(self, conn: sqlite3.Connection) -> None:
        """升级旧库：REAL 浓度列迁移为保留完整精度的 TEXT，并作废遗留预览。

        既有版本只按数据库中**已保存的数值**供数（旧值原本就是 float，
        按最短往返文本 repr 写出，与升级前发布的 CSV 逐字符一致），
        不推测任何在上传时已丢失的原始精度。
        升级前创建、尚未确认的预览会话，其载荷里的浓度是舍入后的 float，
        一律作废并提示重新上传，不得按舍入值发布。
        """
        legacy = conn.execute(
            "SELECT 1 FROM pragma_table_info('results') WHERE name='concentration' "
            "AND lower(type) LIKE '%real%'"
        ).fetchone()
        upgraded_from_legacy = legacy is not None
        if upgraded_from_legacy:
            old_rows = conn.execute(
                "SELECT id, version, batch_no, sample_no, analyte, concentration "
                "FROM results ORDER BY id"
            ).fetchall()
            conn.execute("ALTER TABLE results RENAME TO results_legacy")
            conn.execute(
                """
                CREATE TABLE results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version INTEGER NOT NULL,
                    batch_no TEXT NOT NULL,
                    sample_no TEXT NOT NULL,
                    analyte TEXT NOT NULL,
                    concentration TEXT NOT NULL,
                    UNIQUE (version, sample_no, analyte)
                )
                """
            )
            conn.executemany(
                "INSERT INTO results(id, version, batch_no, sample_no, analyte,"
                " concentration) VALUES (?,?,?,?,?,?)",
                [
                    (r["id"], r["version"], r["batch_no"], r["sample_no"],
                     r["analyte"], repr(float(r["concentration"])))
                    for r in old_rows
                ],
            )
            conn.execute("DROP TABLE results_legacy")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_results_version ON results(version)"
            )
            self._log(
                conn, "migrate",
                f"浓度存储由 REAL 迁移为精确十进制文本（{len(old_rows)} 行既有版本"
                "按已保存数值保留，不推测丢失的原始精度）",
            )

        # 旧版 revision_sessions 没有 obsolete_reason 列，补上以便区分作废原因
        cols = {r["name"] for r in conn.execute(
            "SELECT name FROM pragma_table_info('revision_sessions')"
        ).fetchall()}
        if "obsolete_reason" not in cols:
            conn.execute(
                "ALTER TABLE revision_sessions ADD COLUMN obsolete_reason TEXT"
            )

        # 仅在真正从旧版（REAL 浮点）升级时才作废未确认预览：这些会话的载荷是
        # 舍入后的 float，不能按舍入值发布。新建库/常规重启的会话浓度是精确文本，
        # 绝不能误伤（它们另由 24 小时过期与发布时兜底校验保护）。
        if upgraded_from_legacy:
            pending = conn.execute(
                "SELECT token FROM revision_sessions WHERE status='pending'"
            ).fetchall()
            if pending:
                conn.executemany(
                    "UPDATE revision_sessions SET status='obsolete', "
                    "obsolete_reason=? WHERE token=? AND status='pending'",
                    [(LEGACY_SESSION_REASON, s["token"]) for s in pending],
                )
                for s in pending:
                    self._log(
                        conn, "migrate",
                        f"升级前未确认的预览会话 {s['token'][:8]} 已作废："
                        "载荷浓度为舍入值，需重新上传",
                    )

    def _log(self, conn: sqlite3.Connection, kind: str, detail: str) -> None:
        conn.execute(
            "INSERT INTO event_log(ts, kind, detail) VALUES (?,?,?)",
            (_now(), kind, detail),
        )

    def log(self, kind: str, detail: str) -> None:
        with self._connect() as conn:
            self._log(conn, kind, detail)

    # ---------- 启动恢复 ----------
    def recover(self) -> list[str]:
        """重启后核对下游文件与登记的一致性，返回本次恢复动作说明。"""
        actions: list[str] = []
        with self._connect() as conn:
            reg = conn.execute("SELECT * FROM registry WHERE id = 1").fetchone()

            # 1) 清理崩溃遗留的临时文件（可能只写了一半，绝不能被下游读到）
            for name in os.listdir(self.downstream_dir):
                if name.startswith(".tmp-publish-"):
                    path = os.path.join(self.downstream_dir, name)
                    try:
                        os.unlink(path)
                    except OSError:
                        pass
                    msg = f"清理遗留临时文件 downstream/{name}"
                    actions.append(msg)
                    self._log(conn, "recover", msg)

            # 2) 登记为空：正式文件（若存在）属于无登记游离文件，隔离并保留
            if reg is None:
                if os.path.exists(self.live_path):
                    dest = self._quarantine(LIVE_FILENAME)
                    msg = f"数据库无任何发布登记，将下游游离文件移入 {os.path.basename(dest)}"
                    actions.append(msg)
                    self._log(conn, "recover", msg)
                return actions

            expected_csv = self._render_version(conn, reg["current_version"])
            expected_hash = hashlib.sha256(expected_csv.encode("utf-8")).hexdigest()

            # 3) 文件缺失/哈希不符：以登记为准原子重建
            need_rewrite = True
            if os.path.exists(self.live_path):
                with open(self.live_path, "rb") as fh:
                    actual_hash = hashlib.sha256(fh.read()).hexdigest()
                need_rewrite = actual_hash != expected_hash

            if need_rewrite:
                self._atomic_write(expected_csv)
                msg = (
                    f"下游 CSV 与登记 v{reg['current_version']} 不一致，"
                    "已按数据库登记重建（恢复为完整新版本）"
                )
                actions.append(msg)
                self._log(conn, "recover", msg)
            else:
                self._log(
                    conn,
                    "recover",
                    f"启动核对通过：下游 CSV 与登记 v{reg['current_version']} 一致",
                )

            # 4) 同目录下游文件白名单：非登记文件一律隔离
            for name in os.listdir(self.downstream_dir):
                full = os.path.join(self.downstream_dir, name)
                if os.path.isfile(full) and name != LIVE_FILENAME:
                    dest = self._quarantine(name)
                    msg = f"下游目录存在未登记文件 {name}，已移入 {os.path.basename(dest)}"
                    actions.append(msg)
                    self._log(conn, "recover", msg)

            # 5) 超过 24 小时仍未确认的预览会话作废，避免无限期堆积
            cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(
                timespec="seconds"
            )
            stale = conn.execute(
                "SELECT token FROM revision_sessions "
                "WHERE status='pending' AND created_at < ?",
                (cutoff,),
            ).fetchall()
            for s in stale:
                conn.execute(
                    "UPDATE revision_sessions SET status='obsolete', "
                    "obsolete_reason=? WHERE token=?",
                    (STALE_SESSION_REASON, s["token"]),
                )
                self._log(
                    conn, "recover",
                    f"预览会话 {s['token'][:8]} 超过 24 小时未确认，已作废",
                )
        return actions

    def _quarantine(self, name: str) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        dest = os.path.join(self.quarantine_dir, f"{stamp}-{name}")
        os.replace(os.path.join(self.downstream_dir, name), dest)
        return dest

    def _atomic_write(self, csv_text: str) -> None:
        """同目录临时文件 + fsync + 原子改名，保证下游只看到完整文件。"""
        tmp_path = os.path.join(
            self.downstream_dir, f".tmp-publish-{uuid.uuid4().hex}.csv"
        )
        fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
                fh.write(csv_text)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self.live_path)
            self._fsync_dir(self.downstream_dir)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    @staticmethod
    def _fsync_dir(path: str) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    # ---------- 读 ----------
    def current_registry(self):
        with self._connect() as conn:
            return conn.execute("SELECT * FROM registry WHERE id = 1").fetchone()

    def current_rows(self) -> list[dict]:
        with self._connect() as conn:
            reg = conn.execute("SELECT * FROM registry WHERE id = 1").fetchone()
            if reg is None:
                return []
            return self._version_rows(conn, reg["current_version"])

    def _version_rows(self, conn: sqlite3.Connection, version: int) -> list[dict]:
        cur = conn.execute(
            "SELECT sample_no, analyte, concentration FROM results "
            "WHERE version = ? ORDER BY sample_no, analyte",
            (version,),
        )
        return [dict(r) for r in cur.fetchall()]

    def _render_version(self, conn: sqlite3.Connection, version: int) -> str:
        return render_csv(self._version_rows(conn, version))

    def recent_events(self, limit: int = 20):
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM event_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()

    def get_session(self, token: str):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM revision_sessions WHERE token = ?", (token,)
            ).fetchone()
            if row is None:
                return None
            data = dict(row)
            data["rows"] = json.loads(data["payload"])
            return data

    def create_session(self, batch_no: str, base_version: int, rows: list[dict]) -> str:
        token = uuid.uuid4().hex
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO revision_sessions(token, batch_no, base_version, payload,"
                " created_at, status) VALUES (?,?,?,?,?,?)",
                (
                    token,
                    batch_no,
                    base_version,
                    json.dumps(rows, ensure_ascii=False),
                    _now(),
                    "pending",
                ),
            )
        return token

    def mark_obsolete(self, token: str, current_version: int,
                      reason: str | None = None) -> None:
        """预览在确认前已被其他发布超越：作废该会话，强制重新预览。"""
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE revision_sessions SET status='obsolete', "
                "obsolete_reason=COALESCE(?, obsolete_reason) "
                "WHERE token=? AND status='pending'",
                (reason, token),
            )
            if cur.rowcount:
                self._log(
                    conn, "conflict",
                    f"会话 {token[:8]} 已过期（当前 v{current_version}），"
                    "拒绝覆盖，需重新预览",
                )

    def cancel_session(self, token: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE revision_sessions SET status='cancelled' "
                "WHERE token=? AND status='pending'",
                (token,),
            )
            if cur.rowcount:
                self._log(conn, "cancel", f"放弃修订会话 {token[:8]}")
                return cur.rowcount > 0
        return False

    # ---------- 预览差异 ----------
    def preview_diff(self, new_rows: list[dict]) -> dict:
        """与当前已发布版本比对，返回 增/改/删/不变 四类差异。

        浓度按 Decimal 精确比较：只要十进制数值不同即判为「修改」，
        不受浮点精度影响（例如 0.10000000000000001 与 0.1 视为不同读数）。
        展示值仍保留各自的原始文本。
        """
        old = {(r["sample_no"], r["analyte"]): r["concentration"]
               for r in self.current_rows()}
        new = {(r["sample_no"], r["analyte"]): r["concentration"] for r in new_rows}
        inserts, updates, deletes, unchanged = [], [], [], []
        for key, val in new.items():
            if key not in old:
                inserts.append({"sample_no": key[0], "analyte": key[1], "new": val})
            elif _as_decimal(old[key]) != _as_decimal(val):
                updates.append(
                    {"sample_no": key[0], "analyte": key[1],
                     "old": old[key], "new": val}
                )
            else:
                unchanged.append({"sample_no": key[0], "analyte": key[1], "value": val})
        for key, val in old.items():
            if key not in new:
                deletes.append({"sample_no": key[0], "analyte": key[1], "old": val})
        inserts.sort(key=lambda d: (d["sample_no"], d["analyte"]))
        updates.sort(key=lambda d: (d["sample_no"], d["analyte"]))
        deletes.sort(key=lambda d: (d["sample_no"], d["analyte"]))
        unchanged.sort(key=lambda d: (d["sample_no"], d["analyte"]))
        return {
            "inserts": inserts, "updates": updates,
            "deletes": deletes, "unchanged": unchanged,
        }

    # ---------- 确认发布（核心事务） ----------
    def publish(self, token: str) -> tuple[int, int]:
        """确认整批替换。返回 (新版本号, 行数)。

        并发控制：BEGIN IMMEDIATE 立即拿写锁，事务内再次核对会话基线版本，
        若期间已有别人发布（current_version > base_version），本会话作废，
        抛出 PublishConflictError，调用方须重新预览。
        """
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            session = conn.execute(
                "SELECT * FROM revision_sessions WHERE token=?", (token,)
            ).fetchone()
            if session is None:
                raise PublishConflictError("修订会话不存在或已失效，请重新上传预览")
            if session["status"] != "pending":
                label = {"published": "已发布", "obsolete": "已过期",
                         "cancelled": "已取消"}.get(session["status"], session["status"])
                raise PublishConflictError(f"该修订会话{label}，请重新上传预览")

            reg = conn.execute("SELECT * FROM registry WHERE id=1").fetchone()
            current_version = reg["current_version"] if reg else 0
            if session["base_version"] != current_version:
                conn.execute(
                    "UPDATE revision_sessions SET status='obsolete' WHERE token=?",
                    (token,),
                )
                self._log(
                    conn, "conflict",
                    f"会话 {token[:8]} 基于 v{session['base_version']} 发起，"
                    f"当前已为 v{current_version}，拒绝覆盖，需重新预览",
                )
                raise PublishConflictError(
                    f"版本已变化：您基于 v{session['base_version']} 预览，"
                    f"当前已是 v{current_version}，请重新预览后再发布"
                )

            rows = json.loads(session["payload"])
            # 防御：只有以精确十进制文本承载的新会话才允许发布。
            # 遗留会话（浓度为 float 或无法按有限非负十进制解析）已在迁移时
            # 作废；此处再兜底拒绝，绝不按舍入值发布。
            if not all(
                isinstance(r.get("concentration"), str)
                and _is_publishable_concentration(r["concentration"])
                for r in rows
            ):
                conn.execute(
                    "UPDATE revision_sessions SET status='obsolete', "
                    "obsolete_reason=? WHERE token=?",
                    (LEGACY_SESSION_REASON, token),
                )
                self._log(
                    conn, "conflict",
                    f"会话 {token[:8]} 载荷缺少精确十进制浓度，拒绝发布，需重新上传",
                )
                raise PublishConflictError(LEGACY_SESSION_REASON)
            new_version = current_version + 1
            csv_text = render_csv(rows)
            content_hash = hashlib.sha256(csv_text.encode("utf-8")).hexdigest()

            # 先原子替换文件（旧文件在 replace 前始终完整可用）
            self._atomic_write(csv_text)

            # 再落库登记；若此后进程崩溃，重启 recover() 以登记为准重建/核对
            conn.executemany(
                "INSERT INTO results(version, batch_no, sample_no, analyte, concentration)"
                " VALUES (?,?,?,?,?)",
                [
                    (new_version, session["batch_no"], r["sample_no"],
                     r["analyte"], r["concentration"])
                    for r in rows
                ],
            )
            conn.execute(
                "INSERT INTO registry(id, current_version, batch_no, row_count,"
                " published_at, content_hash) VALUES (1,?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET current_version=excluded.current_version,"
                " batch_no=excluded.batch_no, row_count=excluded.row_count,"
                " published_at=excluded.published_at, content_hash=excluded.content_hash",
                (new_version, session["batch_no"], len(rows), _now(), content_hash),
            )
            conn.execute(
                "UPDATE revision_sessions SET status='published' WHERE token=?",
                (token,),
            )
            self._log(
                conn, "publish",
                f"发布 v{new_version}：批次 {session['batch_no']}，{len(rows)} 行，"
                f"sha256={content_hash[:12]}…",
            )
            return new_version, len(rows)

    # ---------- 页面辅助 ----------
    @staticmethod
    def display_concentration(value) -> str:
        """浓度展示文本：精确数据原样返回，历史浮点按最短往返表示呈现。"""
        return format_concentration(value)
