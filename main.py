import asyncio
import hashlib
import json
import os
import random
import re
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import aiohttp
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, StarTools, register

PLUGIN_NAME = "kook_monitor"
KOOK_API_BASE = "https://www.kookapp.cn/api/v3"

# 昵称 / 频道名缓存有效期（秒）
NAME_CACHE_TTL = 3600
BROWSE_TTL = 600
DIRECTORY_TTL = 30
# 浏览代号：1（第 1 台服务器）或 1.2（第 1 台服务器的第 2 个频道）；退订代号：1、2、3。
# KOOK 的真实 ID 是十几位长数字，不会与 1~3 位的短代号混淆。
BROWSE_CODE = re.compile(r"\d{1,3}(?:\.\d{1,3})?")
UNSUB_CODE = re.compile(r"\d{1,3}")

# KOOK WebSocket 信令（官方文档 developer.kookapp.cn/doc/websocket）
# 注意：字段名是 "s" 而不是 Discord 风格的 "op"
SIG_EVENT = 0  # server -> client 事件下发（带 sn）
SIG_HELLO = 1  # server -> client 握手结果
SIG_PING = 2  # client -> server 心跳
SIG_PONG = 3  # server -> client 心跳回包
SIG_RECONNECT = 5  # server -> client 要求重连
SIG_RESUME_ACK = 6  # server -> client 恢复成功

# 通知文案里 {time} 的显示时区：固定北京时间（不用 ZoneInfo，精简镜像常缺 tzdata）
DISPLAY_TZ = timezone(timedelta(hours=8))

DEFAULT_JOIN_TEMPLATE = (
    "🎮【KOOK 开黑提醒】\n{user} 进入了「{guild} / {channel}」！快来上车~"
)
DEFAULT_LEAVE_TEMPLATE = "👋【KOOK 开黑提醒】\n{user} 离开了「{guild} / {channel}」。"


class KookRateLimited(RuntimeError):
    pass


class KookAuthError(RuntimeError):
    pass


class _SafeDict(dict):
    """format_map 用：未知占位符原样保留，而不是抛 KeyError"""

    def __missing__(self, key):
        return "{" + key + "}"


@register(
    PLUGIN_NAME,
    "q4q3q9q9",
    "KOOK 语音频道进出监控，有人进入/离开语音频道时，经 AstrBot 自动推送通知到已接入平台的群聊或私聊",
    "1.7.0",
)
class KookMonitorPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.kook_token = str(config.get("kook_token", "") or "").strip()
        self.target_channel_id = str(config.get("target_channel_id", "") or "").strip()

        try:
            self.leave_delay = max(0, int(config.get("leave_delay", 120)))
        except (TypeError, ValueError):
            logger.warning(
                "[KOOK Monitor] leave_delay 不是有效数字，已使用默认值 120 秒"
            )
            self.leave_delay = 120
        self.notify_join: bool = bool(config.get("notify_join", True))
        self.notify_leave: bool = bool(config.get("notify_leave", True))
        self.admin_only: bool = bool(config.get("admin_only", True))
        self.allow_private_sub: bool = bool(config.get("allow_private_sub", False))
        self.join_template = self._load_template("join_template", DEFAULT_JOIN_TEMPLATE)
        self.leave_template = self._load_template(
            "leave_template", DEFAULT_LEAVE_TEMPLATE
        )

        # 订阅规则：{会话umo: [规则, ...]}
        # [] = 全部；纯频道 ID = 旧版规则；服务器ID/* 或 服务器ID/频道ID = 新规则。
        # 存放在 data/plugin_data/kook_monitor/bindings.json，initialize 时加载。
        self.bindings: dict[str, list[str]] = {}
        self._bindings_file: Path | None = None
        # 订阅代次：{会话umo: 代次}，订阅被删除或收窄时 +1（仅内存，重启后队列本来就清空）
        self._sub_epoch: dict[str, int] = {}

        self.leave_debounce_tasks: dict[tuple[str, str, str], asyncio.Task] = {}
        self._user_name_cache: dict[tuple[str, str], tuple[str, float]] = {}
        self._channel_name_cache: dict[str, tuple[str, float]] = {}
        self._guild_names: dict[str, str] = {}
        self._guild_names_expire = 0.0
        self._directory_lock = asyncio.Lock()
        self._directory_expire = 0.0
        self._directory: list[tuple[str, str, list[dict], str]] = []
        self._http_retry_at: dict[str, float] = {}
        # 浏览 / 退订代号：每个会话共用最近一次列表 {umo: (过期时间, {代号: 值})}。
        # 代号映射到列表显示时的具体值，而非「当前第几项」，列表之后变动也不会误操作。
        self._browse_options: dict[str, tuple[float, dict[str, list[str]]]] = {}
        self._unsubscribe_options: dict[str, tuple[float, dict[str, str]]] = {}

        self.current_sn = 0
        self.session_id: str | None = None
        self._pending_events: dict[int, dict] = {}
        self._events: asyncio.Queue[dict] = asyncio.Queue(maxsize=256)
        self._gateway_generation = 0
        # (generation, user_id, channel_id, guild_id, joined, targets, ts_ms)
        self._notifications: asyncio.Queue[tuple] = asyncio.Queue(maxsize=256)
        self._outboxes: dict[str, asyncio.Queue] = {}
        self._send_tasks: set[asyncio.Task] = set()
        self._pong = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._http: aiohttp.ClientSession | None = None
        self._running = False
        # 长连接状态，供 /kook_test 自检展示：(图标, 文字)
        self._gateway_state: tuple[str, str] = ("⏸", "监控未启动")

    async def initialize(self):
        if self._running:
            return
        self._load_bindings()
        if self.target_channel_id:
            # 该配置项已在页面上隐藏，但老用户填过的值仍然生效，必须明确提示
            logger.warning(
                "[KOOK Monitor] 全局频道过滤正在生效：只监控频道 %s，其他频道的进出都会被忽略。"
                "如不需要，请清空插件配置中的 target_channel_id。",
                self.target_channel_id,
            )
        if not self.kook_token:
            logger.warning("[KOOK Monitor] 未配置 kook_token，监控未启动。")
            self._gateway_state = ("⏸", "监控未启动（未填写 Token）")
            return

        self._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        self._running = True
        self._gateway_state = ("⏳", "正在连接 KOOK")
        self._tasks = [
            asyncio.create_task(self._kook_gateway_loop()),
            asyncio.create_task(self._process_events()),
            asyncio.create_task(self._process_notifications()),
        ]

    async def terminate(self):
        self._running = False
        tasks = [*self._tasks, *self.leave_debounce_tasks.values(), *self._send_tasks]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()
        self.leave_debounce_tasks.clear()
        if self._http is not None:
            await self._http.close()
            self._http = None

    # ------------------ 订阅数据持久化 ------------------

    @staticmethod
    def _clean_bindings(raw) -> dict[str, list[str]]:
        if not isinstance(raw, dict):
            raise ValueError("订阅数据不是对象")
        return {
            str(umo): list(dict.fromkeys(str(rule) for rule in rules))
            for umo, rules in raw.items()
            if isinstance(rules, list)
        }

    def _load_bindings(self):
        """加载 bindings.json，并把旧版存放在插件配置里的订阅合并进来。

        旧版字段（target_sessions / target_users / bindings / bindings_json）仍保留在 schema 中
        （页面隐藏），否则 AstrBot 加载配置时会在插件初始化前把它们删掉。
        只有新文件写入成功后才清空旧字段；写入失败时旧数据原样保留，下次启动再迁移。
        """
        try:
            self._bindings_file = (
                Path(StarTools.get_data_dir(PLUGIN_NAME)) / "bindings.json"
            )
        except Exception:
            logger.exception("[KOOK Monitor] 无法获取插件数据目录，订阅将无法持久化")
            self._bindings_file = None

        bindings: dict[str, list[str]] = {}
        if self._bindings_file and self._bindings_file.exists():
            try:
                bindings = self._clean_bindings(
                    json.loads(self._bindings_file.read_text(encoding="utf-8"))
                )
            except Exception:
                # 文件损坏时不覆盖它，留给用户手动修复
                logger.exception(
                    "[KOOK Monitor] 读取 %s 失败，本次以空订阅运行且不会写回该文件",
                    self._bindings_file,
                )
                self._bindings_file = None

        legacy: dict[str, list[str]] = {}
        stored = self.config.get("bindings_json", "")
        if isinstance(stored, str) and stored.strip():
            try:
                legacy.update(self._clean_bindings(json.loads(stored)))
            except Exception:
                logger.error(
                    "[KOOK Monitor] 旧配置 bindings_json 无法解析，已跳过（原值保留未清除）"
                )
                stored = None  # 标记为不可清除
        old = self.config.get("bindings")
        if isinstance(old, dict):
            for umo, rules in self._clean_bindings(old).items():
                legacy[umo] = list(dict.fromkeys([*legacy.get(umo, []), *rules]))
        for key in ("target_sessions", "target_users"):
            for umo in self.config.get(key, None) or []:
                legacy.setdefault(str(umo), [])

        if not legacy:
            self.bindings = bindings
        elif self._legacy_fingerprint(legacy) in self._read_imported_marks():
            # 这批旧数据此前已经迁移过，只是上次清理旧配置没成功（例如配置文件不可写）。
            # 不再重复导入，否则用户迁移后的退订会在重启时被旧数据恢复。
            self.bindings = bindings
            logger.info(
                "[KOOK Monitor] 旧版订阅此前已迁移，跳过导入并重新尝试清理旧配置"
            )
            self._clear_legacy(stored is not None)
        else:
            for umo, rules in legacy.items():
                if umo not in bindings:
                    bindings[umo] = rules
                elif bindings[umo] and rules:
                    bindings[umo] = list(dict.fromkeys([*bindings[umo], *rules]))
                else:
                    bindings[umo] = []  # 任一方是「全部」则取「全部」
            if self._save_targets(bindings):
                # 先记下「这批旧数据已导入」，再清理旧配置；清理失败也不会重复导入
                self._mark_imported(self._legacy_fingerprint(legacy))
                self._clear_legacy(stored is not None)
                logger.info(
                    "[KOOK Monitor] 已把 %d 个旧版订阅迁移到 bindings.json", len(legacy)
                )
            else:
                self.bindings = bindings  # 先在内存中生效，旧配置保留待下次迁移
        logger.info("[KOOK Monitor] 已加载 %d 个订阅会话", len(self.bindings))

    @staticmethod
    def _legacy_fingerprint(legacy: dict[str, list[str]]) -> str:
        canonical = json.dumps(
            {umo: sorted(rules) for umo, rules in legacy.items()},
            sort_keys=True,
            ensure_ascii=False,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @property
    def _marks_file(self) -> Path | None:
        if self._bindings_file is None:
            return None
        return self._bindings_file.with_name("legacy_imported.json")

    def _read_imported_marks(self) -> set[str]:
        path = self._marks_file
        if path is None or not path.exists():
            return set()
        try:
            return set(json.loads(path.read_text(encoding="utf-8")))
        except Exception:
            logger.exception("[KOOK Monitor] 读取迁移记录 %s 失败", path)
            return set()

    def _mark_imported(self, fingerprint: str):
        path = self._marks_file
        if path is None:
            return
        marks = sorted(self._read_imported_marks() | {fingerprint})
        if not self._atomic_write_json(path, marks):
            logger.warning(
                "[KOOK Monitor] 迁移记录写入失败，若旧配置也未能清理，重启后可能重复导入"
            )

    def _clear_legacy(self, clear_bindings_json: bool):
        if clear_bindings_json:
            self.config["bindings_json"] = ""
        self.config["bindings"] = ""
        self.config["target_sessions"] = []
        self.config["target_users"] = []
        self._save_config()

    @staticmethod
    def _atomic_write_json(path: Path, data) -> bool:
        """写临时文件 → fsync → 原子替换。失败时清理临时文件并返回 False。"""
        temporary_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=path.parent,
                delete=False,
                suffix=".tmp",
            ) as temporary:
                temporary_path = Path(temporary.name)
                json.dump(data, temporary, indent=2, ensure_ascii=False)
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_path, path)
            temporary_path = None
            return True
        except Exception:
            logger.exception("[KOOK Monitor] 写入 %s 失败", path)
            return False
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    @staticmethod
    def _narrowed(old: list[str] | None, new: list[str] | None) -> bool:
        """订阅范围是否被收窄（删除、全部→指定、移除了某条规则）。"""
        if old is None:
            return False
        if new is None:
            return True
        if not old:
            return bool(new)
        if not new:
            return False
        return bool(set(old) - set(new))

    def _save_targets(self, bindings: dict[str, list[str]]) -> bool:
        """先写盘，成功后才替换内存中的订阅；失败时保留原状态并返回 False。

        某个会话的订阅被删除或收窄时，递增它的订阅代次：退订前已排队的通知带着旧代次，
        即使随后马上重新订阅，也不会再发给这个会话。"""
        if self._bindings_file is None:
            logger.error("[KOOK Monitor] 订阅文件不可用，无法保存订阅")
            return False
        if not self._atomic_write_json(self._bindings_file, bindings):
            return False
        for umo, old in self.bindings.items():
            if self._narrowed(old, bindings.get(umo)):
                self._sub_epoch[umo] = self._sub_epoch.get(umo, 0) + 1
        self.bindings = bindings
        return True

    def _save_config(self):
        save = getattr(self.config, "save_config", None)
        if callable(save):
            try:
                save()
            except Exception:
                logger.exception("[KOOK Monitor] 插件配置落盘失败")

    # ------------------ 文案模板 ------------------

    def _load_template(self, key: str, default: str) -> str:
        tpl = str(self.config.get(key, "") or "").strip()
        if not tpl:
            return default
        tpl = tpl.replace("\\n", "\n")  # 单行输入框里写的 "\n" 转成真正的换行
        try:
            # 用示例值试渲染一次，提前发现 {user.xxx}、{0}、未闭合花括号等写法错误
            self._format(tpl, "示例用户", "0", "示例服务器", "示例频道", "0", None)
        except Exception as exc:  # noqa: BLE001 - 模板写法千奇百怪，任何错误都回退默认文案
            logger.warning(
                "[KOOK Monitor] 配置项 %s 模板格式有误（%s: %s），已改用默认文案。"
                "如需输出花括号本身，请写成 {{ 和 }}。",
                key,
                type(exc).__name__,
                exc,
            )
            return default
        return tpl

    @staticmethod
    def _format(tpl, user, user_id, guild, channel, channel_id, ts_ms) -> str:
        """ts_ms 为 KOOK 事件里的毫秒时间戳（joined_at / exited_at），缺省取当前时间。
        统一按北京时间显示，不依赖服务器/容器时区（Docker 默认是 UTC）。"""
        if ts_ms:
            moment = datetime.fromtimestamp(ts_ms / 1000, tz=DISPLAY_TZ)
        else:
            moment = datetime.now(tz=DISPLAY_TZ)
        return tpl.format_map(
            _SafeDict(
                user=user,
                user_id=user_id,
                guild=guild,
                channel=channel,
                channel_id=channel_id,
                time=moment.strftime("%H:%M"),
            )
        )

    def _render(self, joined: bool, *args) -> str:
        tpl = self.join_template if joined else self.leave_template
        try:
            return self._format(tpl, *args)
        except Exception as exc:  # noqa: BLE001 - 兜底：渲染失败也要把通知发出去
            logger.warning("[KOOK Monitor] 文案渲染失败，改用默认文案: %s", exc)
            default = DEFAULT_JOIN_TEMPLATE if joined else DEFAULT_LEAVE_TEMPLATE
            return self._format(default, *args)

    @staticmethod
    def _event_ts(event_data: dict, body: dict, key: str) -> int | None:
        """取事件发生时间（毫秒）：优先 body 里的 joined_at / exited_at，其次 msg_timestamp"""
        for value in (body.get(key), event_data.get("msg_timestamp")):
            if isinstance(value, (int, float)) and value > 0:
                return int(value)
        return None

    # ------------------ KOOK HTTP API ------------------

    async def _kook_get(self, endpoint: str, **params):
        if self._http is None:
            raise RuntimeError("请先在插件配置中设置 KOOK Token 并重载插件")
        now = time.monotonic()
        if now < max(
            self._http_retry_at.get(endpoint, 0), self._http_retry_at.get("*", 0)
        ):
            raise KookRateLimited("KOOK 接口请求过于频繁，请稍后重试。")
        async with self._http.get(
            f"{KOOK_API_BASE}/{endpoint}",
            headers={"Authorization": f"Bot {self.kook_token}"},
            params=params,
        ) as response:
            headers = response.headers
            if response.status == 429 or headers.get("X-Rate-Limit-Remaining") == "0":
                try:
                    delay = max(1.0, float(headers.get("X-Rate-Limit-Reset", "5")))
                except ValueError:
                    delay = 5.0
                key = "*" if "X-Rate-Limit-Global" in headers else endpoint
                self._http_retry_at[key] = max(
                    self._http_retry_at.get(key, 0), time.monotonic() + delay
                )
                if response.status == 429:
                    raise KookRateLimited("KOOK 接口请求过于频繁，请稍后重试。")
            if response.status == 401:
                raise KookAuthError("KOOK Token 无效")
            response.raise_for_status()
            result = await response.json(content_type=None)
        if result.get("code") == 401:
            raise KookAuthError("KOOK Token 无效")
        if result.get("code") != 0:
            raise RuntimeError(f"KOOK {endpoint} 返回错误码 {result.get('code')}")
        return result["data"]

    # ------------------ KOOK Gateway 长连接与心跳 ------------------

    def _reset_gateway(self):
        self.session_id = None
        self.current_sn = 0
        self._pending_events.clear()
        self._gateway_generation += 1
        for queue in (self._events, self._notifications):
            while not queue.empty():
                queue.get_nowait()
        for task in self.leave_debounce_tasks.values():
            task.cancel()
        self.leave_debounce_tasks.clear()
        for task in self._send_tasks:
            task.cancel()
        self._outboxes.clear()

    async def _kook_gateway_loop(self):
        gateway_url = None
        backoff = 1
        resume_failures = 0
        while self._running:
            connected = False
            try:
                if gateway_url is None:
                    gateway_url = (await self._kook_get("gateway/index", compress=0))[
                        "url"
                    ]
                url = gateway_url
                resuming = self.session_id is not None
                if resuming:
                    parts = urlsplit(url)
                    params = parse_qsl(parts.query, keep_blank_values=True)
                    params.extend(
                        [
                            ("resume", "1"),
                            ("sn", str(self.current_sn)),
                            ("session_id", self.session_id),
                        ]
                    )
                    url = urlunsplit(parts._replace(query=urlencode(params)))
                async with self._http.ws_connect(url) as ws:
                    async with asyncio.timeout(6):
                        hello = await ws.receive_json()
                    signal = hello.get("s")
                    data = hello.get("d") or {}
                    if signal == SIG_RECONNECT or (
                        signal == SIG_HELLO and data.get("code") == 40103
                    ):
                        # 40103 = token 过期，官方要求重新建立连接
                        self._reset_gateway()
                        gateway_url = None
                    elif signal == SIG_HELLO and data.get("code") in (
                        40100,
                        40101,
                        40102,
                    ):
                        logger.error(
                            "[KOOK Monitor] 握手失败 code=%s，请检查 kook_token 后重载插件。监控已停止。",
                            data["code"],
                        )
                        self._gateway_state = ("⏸", "监控已停止（Token 无效）")
                        return
                    elif resuming and signal in (SIG_EVENT, SIG_RESUME_ACK, SIG_HELLO):
                        if signal == SIG_HELLO and data.get("code") != 0:
                            raise RuntimeError(f"恢复握手失败 code={data.get('code')}")
                        if (
                            signal != SIG_HELLO
                            or data.get("session_id") == self.session_id
                        ):
                            connected = True
                            await self._read_gateway(ws, hello)
                        else:
                            self._reset_gateway()
                            self.session_id = data["session_id"]
                            connected = True
                            await self._read_gateway(ws)
                    elif signal == SIG_HELLO and data.get("code") == 0:
                        self.session_id = data["session_id"]
                        connected = True
                        logger.info("[KOOK Monitor] Gateway 握手成功")
                        await self._read_gateway(ws)
                    else:
                        raise RuntimeError(f"意外的 Gateway 握手信令 {signal}")

                    if self.session_id is None:
                        gateway_url = None
            except asyncio.CancelledError:
                raise
            except KookAuthError:
                logger.error(
                    "[KOOK Monitor] KOOK Token 无效，监控已停止。请检查 kook_token 后重载插件。"
                )
                self._gateway_state = ("⏸", "监控已停止（Token 无效）")
                return
            except Exception as exc:
                # URL 可能含凭据，避免直接输出 WebSocket 异常正文。
                logger.warning(
                    "[KOOK Monitor] Gateway 连接中断 (%s)，准备重连", type(exc).__name__
                )
                if not connected:
                    if self.session_id is not None:
                        resume_failures += 1
                        if resume_failures >= 3:
                            logger.warning("[KOOK Monitor] 多次恢复失败，重新建立会话")
                            self._reset_gateway()
                    if self.session_id is None:
                        gateway_url = None

            if connected or self.session_id is None:
                resume_failures = 0
            if connected:
                backoff = 1
            self._gateway_state = ("⏳", "与 KOOK 的连接已断开，正在自动重连")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    async def _read_gateway(self, ws, first_frame=None):
        self._gateway_state = ("✅", "已连接 KOOK，正在监听语音频道")
        self._pong.clear()
        heartbeat = asyncio.create_task(self._heartbeat_loop(ws))
        try:
            if first_frame is not None and self._dispatch_signal(first_frame):
                return
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    if self._dispatch_signal(json.loads(msg.data)):
                        break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    def _dispatch_signal(self, payload: dict) -> bool:
        """处理一帧信令。事件按 sn 暂存、按序入队，current_sn 只推进到连续处理的位置。
        返回 True 表示需要断开（随后按 session_id 续传或重建会话）。"""
        sig = payload.get("s")
        if sig == SIG_EVENT:
            sn = payload["sn"]
            if sn <= self.current_sn:
                return False
            if sn - self.current_sn > self._events.maxsize:
                self._pending_events.clear()
                return True
            self._pending_events.setdefault(sn, payload["d"])
            while self.current_sn + 1 in self._pending_events:
                next_sn = self.current_sn + 1
                try:
                    self._events.put_nowait(self._pending_events[next_sn])
                except asyncio.QueueFull:
                    # 保留已接收的事件，待重连后从最后入队的序号补收。
                    self._pending_events.clear()
                    return True
                del self._pending_events[next_sn]
                self.current_sn = next_sn
        elif sig == SIG_PONG:
            self._pong.set()
        elif sig == SIG_RECONNECT:
            self._reset_gateway()
            return True
        elif sig == SIG_RESUME_ACK:
            self.session_id = (payload.get("d") or {}).get(
                "session_id"
            ) or self.session_id
            logger.info("[KOOK Monitor] Gateway 会话恢复成功")
        return False

    async def _heartbeat_loop(self, ws):
        try:
            while True:
                await asyncio.sleep(30 + random.randint(-5, 5))
                self._pong.clear()
                async with asyncio.timeout(6):
                    await ws.send_json({"s": SIG_PING, "sn": self.current_sn})
                    await self._pong.wait()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("[KOOK Monitor] Gateway 心跳失败 (%s)", type(exc).__name__)
            await ws.close()

    async def _process_events(self):
        while True:
            event = await self._events.get()
            try:
                await self._handle_kook_event(event)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[KOOK Monitor] 处理 KOOK 事件失败")

    # ------------------ 名称查询 ------------------

    def _cache_get(self, cache, key) -> str | None:
        entry = cache.get(key)
        if entry and entry[1] > time.monotonic():
            return entry[0]
        cache.pop(key, None)
        return None

    async def _fetch_user_name(self, user_id: str, guild_id: str) -> str:
        key = (guild_id, user_id)
        cached = self._cache_get(self._user_name_cache, key)
        if cached is not None:
            return cached
        fallback = f"用户({user_id})"
        try:
            data = await self._kook_get("user/view", user_id=user_id, guild_id=guild_id)
            name = data.get("nickname") or data.get("username") or fallback
            self._user_name_cache[key] = (name, time.monotonic() + NAME_CACHE_TTL)
            return name
        except Exception:
            return fallback

    async def _fetch_channel_name(self, channel_id: str) -> str:
        cached = self._cache_get(self._channel_name_cache, channel_id)
        if cached is not None:
            return cached
        try:
            data = await self._kook_get("channel/view", target_id=channel_id)
            name = data.get("name", "")
            self._channel_name_cache[channel_id] = (
                name,
                time.monotonic() + NAME_CACHE_TTL,
            )
            return name
        except Exception:
            return ""

    async def _list_kook_items(self, endpoint: str, **params):
        page = 1
        while True:
            data = await self._kook_get(endpoint, page=page, page_size=50, **params)
            for item in data["items"]:
                yield item
            if page >= int(data["meta"]["page_total"]):
                return
            page += 1

    async def _fetch_guilds(self, *, refresh=False):
        if not refresh and time.monotonic() < self._guild_names_expire:
            return self._guild_names
        guilds = {
            str(item["id"]): item["name"]
            async for item in self._list_kook_items("guild/list")
        }
        self._guild_names = guilds
        self._guild_names_expire = time.monotonic() + NAME_CACHE_TTL
        return guilds

    async def _is_in_channel(
        self, user_id: str, guild_id: str, channel_id: str
    ) -> bool | None:
        """向 KOOK 核实用户当前是否仍在该语音频道。无法确认（接口失败/限流）时返回 None。"""
        try:
            data = await self._kook_get(
                "channel-user/get-joined-channel", guild_id=guild_id, user_id=user_id
            )
        except Exception as exc:
            logger.warning(
                "[KOOK Monitor] 核实用户所在频道失败 (%s)，照常发送退出通知",
                type(exc).__name__,
            )
            return None
        items = data.get("items") or []
        return any(
            str(item.get("id")) == channel_id
            for item in items
            if isinstance(item, dict)
        )

    # ------------------ 事件处理与延迟防抖逻辑 ------------------

    async def _handle_kook_event(self, event_data: dict):
        extra = event_data.get("extra") or {}
        if not isinstance(extra, dict):
            return
        event_type = extra.get("type")
        if event_type not in ("joined_channel", "exited_channel"):
            return

        body = extra.get("body") or {}
        if not isinstance(body, dict):
            return
        user_id = str(body.get("user_id") or "")
        channel_id = str(body.get("channel_id") or "")
        # channel_type 为 GROUP 时，target_id 即服务器(guild) ID，可用于查询服务器内昵称
        guild_id = (
            str(event_data.get("target_id") or "")
            if event_data.get("channel_type") == "GROUP"
            else ""
        )

        if not (guild_id and user_id and channel_id):
            return
        # 过滤指定频道（若配置了）
        if self.target_channel_id and channel_id != self.target_channel_id:
            return

        if event_type == "joined_channel":
            ts = self._event_ts(event_data, body, "joined_at")
            await self._on_user_joined(user_id, channel_id, guild_id, ts)
        else:
            ts = self._event_ts(event_data, body, "exited_at")
            await self._on_user_exited(user_id, channel_id, guild_id, ts)

    async def _on_user_joined(
        self, user_id: str, channel_id: str, guild_id: str, ts_ms
    ):
        # 防抖：刚退出「同一个」频道、还在倒计时中 → 视为闪断，两边都不提醒。
        key = (guild_id, channel_id, user_id)
        task = self.leave_debounce_tasks.pop(key, None)
        if task is not None:
            task.cancel()
            return

        if self.notify_join:
            self._queue_notification(
                user_id,
                channel_id,
                guild_id,
                True,
                self._targets(guild_id, channel_id),
                ts_ms,
            )

    async def _on_user_exited(
        self, user_id: str, channel_id: str, guild_id: str, ts_ms
    ):
        if not self.notify_leave:
            return
        key = (guild_id, channel_id, user_id)
        old_task = self.leave_debounce_tasks.pop(key, None)
        if old_task is not None:
            old_task.cancel()
        targets = self._targets(guild_id, channel_id)
        if targets:
            self.leave_debounce_tasks[key] = asyncio.create_task(
                self._delayed_leave_notify(key, targets, ts_ms)
            )

    async def _delayed_leave_notify(
        self, key: tuple[str, str, str], targets: list[str], ts_ms
    ):
        """延迟 N 秒后发出退出通知（期间回到同一频道则取消）。
        核实期间任务仍登记在 leave_debounce_tasks 中，卸载插件或用户回到频道都能取消它。"""
        guild_id, channel_id, user_id = key
        try:
            await asyncio.sleep(self.leave_delay)
            # 发送前向 KOOK 核实：断线期间漏收「重新进入」事件时，人其实还在频道里
            if await self._is_in_channel(user_id, guild_id, channel_id):
                logger.info(
                    "[KOOK Monitor] 用户 %s 仍在频道 %s，跳过退出通知",
                    user_id,
                    channel_id,
                )
                return
            self._queue_notification(
                user_id, channel_id, guild_id, False, targets, ts_ms
            )
        finally:
            if self.leave_debounce_tasks.get(key) is asyncio.current_task():
                del self.leave_debounce_tasks[key]

    def _queue_notification(
        self, user_id, channel_id, guild_id, joined, targets, ts_ms=None
    ):
        if not targets:
            return
        try:
            self._notifications.put_nowait(
                (
                    self._gateway_generation,
                    user_id,
                    channel_id,
                    guild_id,
                    joined,
                    targets,
                    ts_ms,
                )
            )
        except asyncio.QueueFull:
            logger.warning("[KOOK Monitor] 通知队列已满，丢弃一条新通知")

    async def _process_notifications(self):
        while True:
            item = await self._notifications.get()
            generation, user_id, channel_id, guild_id, joined, targets, ts_ms = item
            try:
                if not any(
                    self._still_targeted(umo, epoch, guild_id, channel_id)
                    for umo, epoch in targets
                ):
                    continue
                user, guild, channel = await self._describe_voice(
                    user_id, channel_id, guild_id
                )
                if generation != self._gateway_generation:
                    continue
                message = self._render(
                    joined, user, user_id, guild, channel, channel_id, ts_ms
                )
                for umo, epoch in targets:
                    if self._still_targeted(umo, epoch, guild_id, channel_id):
                        self._queue_send(umo, epoch, message, guild_id, channel_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[KOOK Monitor] 处理语音频道通知失败")

    async def _describe_voice(self, user_id: str, channel_id: str, guild_id: str):
        user, channel, guilds = await asyncio.gather(
            self._fetch_user_name(user_id, guild_id),
            self._fetch_channel_name(channel_id),
            self._fetch_guilds(),
            return_exceptions=True,
        )
        if not isinstance(user, str):
            user = f"用户({user_id})"
        guild = (
            guilds.get(guild_id, f"服务器 {guild_id}")
            if isinstance(guilds, dict)
            else f"服务器 {guild_id}"
        )
        channel = (
            channel
            if isinstance(channel, str) and channel
            else f"语音频道 {channel_id}"
        )
        return user, guild, channel

    def _matches(self, umo: str, guild_id: str, channel_id: str) -> bool:
        rules = self.bindings.get(umo)
        return rules is not None and (
            not channel_id
            or not rules
            or channel_id in rules
            or f"{guild_id}/*" in rules
            or f"{guild_id}/{channel_id}" in rules
        )

    def _targets(self, guild_id: str, channel_id: str) -> list[tuple[str, int]]:
        """事件发生时的推送目标，连同各自当时的订阅代次一起记录。"""
        return [
            (umo, self._sub_epoch.get(umo, 0))
            for umo in self.bindings
            if self._matches(umo, guild_id, channel_id)
        ]

    def _still_targeted(
        self, umo: str, epoch: int, guild_id: str, channel_id: str
    ) -> bool:
        """发送前复核：订阅代次未变（期间没有退订/收窄），且当前仍匹配。"""
        return self._sub_epoch.get(umo, 0) == epoch and self._matches(
            umo, guild_id, channel_id
        )

    def _queue_send(
        self, umo: str, epoch: int, message: str, guild_id: str, channel_id: str
    ):
        queue = self._outboxes.get(umo)
        if queue is None:
            queue = asyncio.Queue(maxsize=32)
            self._outboxes[umo] = queue
            task = asyncio.create_task(self._send_to_target(umo, queue))
            self._send_tasks.add(task)
            task.add_done_callback(self._send_tasks.discard)
        try:
            queue.put_nowait((epoch, message, guild_id, channel_id))
        except asyncio.QueueFull:
            logger.warning("[KOOK Monitor] 目标 %s 的发送队列已满，丢弃一条新通知", umo)

    async def _send_to_target(self, umo: str, queue: asyncio.Queue):
        try:
            while not queue.empty():
                epoch, message, guild_id, channel_id = queue.get_nowait()
                if not self._still_targeted(umo, epoch, guild_id, channel_id):
                    continue
                try:
                    async with asyncio.timeout(10):
                        matched = await self.context.send_message(
                            umo, MessageChain().message(message)
                        )
                    if matched is False:
                        logger.warning(
                            "[KOOK Monitor] 找不到推送目标 %s 的平台实例（平台适配器 ID 可能改过名或已删除）",
                            umo,
                        )
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("[KOOK Monitor] 推送至 %s 失败", umo)
        finally:
            if self._outboxes.get(umo) is queue:
                del self._outboxes[umo]

    # ------------------ 权限 ------------------

    def _admin_allowed(self, event: AstrMessageEvent) -> bool:
        return not self.admin_only or event.is_admin()

    def _subscribe_denied(self, event: AstrMessageEvent) -> str | None:
        """浏览/订阅的权限检查。返回拒绝原因，允许时返回 None。
        群聊：admin_only 开启时仅管理员；私聊：管理员，或开启了 allow_private_sub。"""
        if event.is_private_chat():
            if self.allow_private_sub or event.is_admin():
                return None
            return "当前未开放私聊订阅，请联系管理员。"
        if self._admin_allowed(event):
            return None
        return "仅管理员可以执行该操作。"

    # ------------------ 订阅规则校验 ------------------

    async def _validate_rules(self, args: list[str]) -> list[str]:
        if not args or any(
            not value.isascii() or not value.isdecimal() for value in args
        ):
            raise ValueError(
                "用法：/kook_sub 服务器ID [频道ID…]，或先用 /kook_browse 获取代号。"
            )
        guild_id, *channels = args
        guilds = await self._fetch_guilds(refresh=True)
        if guild_id not in guilds:
            raise ValueError(
                f"KOOK 机器人未加入服务器 {guild_id}，请核对 ID 或先邀请机器人。"
            )
        if not channels:
            return [f"{guild_id}/*"]

        rules = []
        for channel_id in dict.fromkeys(channels):
            if self.target_channel_id and channel_id != self.target_channel_id:
                raise ValueError(f"频道 {channel_id} 被全局频道过滤排除。")
            try:
                channel = await self._kook_get("channel/view", target_id=channel_id)
            except KookRateLimited:
                raise
            except Exception as exc:
                raise ValueError(
                    f"无法验证频道 {channel_id}。请核对 ID、机器人是否已加入服务器及是否能查看频道。"
                ) from exc
            actual_guild = str(channel["guild_id"])
            if actual_guild not in guilds:
                raise ValueError(
                    f"KOOK 机器人未加入频道 {channel_id} 所在的服务器 {actual_guild}。"
                )
            if actual_guild != guild_id:
                raise ValueError(f"频道 {channel_id} 不属于服务器 {guild_id}。")
            if channel["type"] != 2 or channel.get("is_category"):
                raise ValueError(f"频道 {channel_id} 不是语音频道。")
            rules.append(f"{actual_guild}/{channel_id}")
        return rules

    async def _browse_directory(self):
        async with self._directory_lock:
            if time.monotonic() < self._directory_expire:
                return self._directory

            guilds = await self._fetch_guilds(refresh=True)
            directory = []
            for guild_id, name in guilds.items():
                channels = []
                warning = ""
                try:
                    async for channel in self._list_kook_items(
                        "channel/list", guild_id=guild_id, type=2
                    ):
                        if channel.get("type") == 2 and not channel.get("is_category"):
                            channels.append(channel)
                except KookRateLimited as exc:
                    warning = str(exc)
                except Exception:
                    logger.exception(
                        "[KOOK Monitor] 查询服务器 %s 的频道列表失败", guild_id
                    )
                    warning = "频道列表未能完整加载，请稍后重试。"
                directory.append((guild_id, name, channels, warning))
            self._directory = directory
            self._directory_expire = time.monotonic() + (
                5 if any(warning for _, _, _, warning in directory) else DIRECTORY_TTL
            )
            return directory

    @staticmethod
    def _split_reply(lines: list[str]):
        chunk = []
        length = 0
        for line in lines:
            if chunk and length + len(line) + 1 > 900:
                yield "\n".join(chunk)
                chunk = []
                length = 0
            chunk.append(line)
            length += len(line) + 1
        if chunk:
            yield "\n".join(chunk)

    @staticmethod
    def _fresh_options(store: dict, umo: str) -> dict | None:
        """取会话最近一次列表；过期则清掉并返回 None。"""
        snapshot = store.get(umo)
        if snapshot is None:
            return None
        if time.monotonic() >= snapshot[0]:
            del store[umo]
            return None
        return snapshot[1]

    def _resolve_browse_option(
        self, event: AstrMessageEvent, args: list[str]
    ) -> list[str]:
        if len(args) != 1 or not BROWSE_CODE.fullmatch(args[0]):
            return args
        options = self._fresh_options(self._browse_options, event.unified_msg_origin)
        if options is not None and args[0] in options:
            return options[args[0]]
        raise ValueError(
            "代号无效或已过期，请先发送 /kook_browse 查看最新列表（10 分钟内有效）。"
        )

    @staticmethod
    def _merge_rules(
        current: list[str], new_rules: list[str]
    ) -> tuple[list[str], list[str]]:
        """把新规则并入已有订阅，保证每一项都是实际生效、互不重叠的范围。
        返回 (合并后的规则, 给用户的说明)。

        - 已订阅整台服务器，再订阅其中的频道：不新增，提示「已包含」；
        - 订阅整台服务器时，替换掉这台服务器下已有的单个频道订阅；
        - 1.3/1.4 遗留的纯频道 ID 规则不知道所属服务器，保持原样。"""
        updated = list(current)
        notes: list[str] = []
        for rule in new_rules:
            guild_id, _, channel_id = rule.partition("/")
            whole = f"{guild_id}/*"
            if rule in updated:
                notes.append("所选项目已经在订阅中，未重复添加。")
            elif channel_id == "*":
                replaced = [r for r in updated if r.startswith(f"{guild_id}/")]
                updated = [r for r in updated if r not in replaced]
                updated.append(whole)
                if replaced:
                    notes.append(
                        f"已改为订阅整台服务器，原来该服务器下的 {len(replaced)} 个频道订阅已合并。"
                    )
            elif whole in updated:
                notes.append("该频道已包含在整台服务器的订阅中，未重复添加。")
            else:
                updated.append(rule)
        return updated, list(dict.fromkeys(notes))

    async def _describe_rules(self, rules: list[str]) -> list[str]:
        try:
            guilds = await self._fetch_guilds()
        except Exception:
            guilds = {}
        lines = []
        for rule in rules:
            guild_id, sep, channel_id = rule.partition("/")
            if sep:
                label = f"{guilds.get(guild_id, '服务器')}（{guild_id}）"
                if channel_id == "*":
                    lines.append(f"{label} / 全部语音频道")
                    continue
            else:
                label, channel_id = "旧版频道订阅", rule
            name = await self._fetch_channel_name(channel_id)
            lines.append(f"{label} / {name or '语音频道'}（{channel_id}）")
        return lines

    # ------------------ 指令 ------------------

    @filter.command("kook_browse")
    async def browse(self, event: AstrMessageEvent):
        """列出机器人所在服务器及其语音频道，生成临时订阅代号。"""
        denied = self._subscribe_denied(event)
        if denied:
            yield event.plain_result(denied)
            return
        try:
            directory = await self._browse_directory()
        except KookRateLimited as exc:
            yield event.plain_result(str(exc))
            return
        except Exception:
            logger.exception("[KOOK Monitor] 查询浏览列表失败")
            yield event.plain_result(
                "无法查询 KOOK 服务器列表，请检查插件配置或稍后重试。"
            )
            return
        if not directory:
            yield event.plain_result("KOOK 机器人还没有加入任何服务器。")
            return

        options: dict[str, list[str]] = {}
        lines = ["KOOK 订阅目录 · 代号在本会话内 10 分钟有效："]
        for index, (guild_id, name, channels, warning) in enumerate(directory, start=1):
            guild_code = str(index)
            options[guild_code] = [guild_id]
            lines.append(f"【{name}】全部语音频道 · {guild_code}")
            for channel_index, channel in enumerate(channels, start=1):
                channel_id = str(channel["id"])
                code = f"{guild_code}.{channel_index}"
                options[code] = [guild_id, channel_id]
                suffix = (
                    "（被全局频道过滤排除）"
                    if self.target_channel_id and channel_id != self.target_channel_id
                    else ""
                )
                lines.append(f"  {channel['name']} · {code}{suffix}")
            if warning:
                lines.append(f"  {warning}")
            elif not channels:
                lines.append("  暂无可列出的语音频道。")

        example = next((code for code in options if "." in code), "1")
        lines.append(
            f"发送 /kook_sub 代号 订阅，例如 /kook_sub {example}；"
            "订阅整台服务器发服务器前面的数字。"
        )
        now = time.monotonic()
        self._browse_options = {
            key: entry for key, entry in self._browse_options.items() if entry[0] > now
        }
        self._browse_options[event.unified_msg_origin] = (now + BROWSE_TTL, options)

        for chunk in self._split_reply(lines):
            yield event.plain_result(chunk)

    @filter.command("kook_sub")
    async def subscribe(self, event: AstrMessageEvent):
        """订阅 KOOK 语音频道或整台服务器。"""
        denied = self._subscribe_denied(event)
        if denied:
            yield event.plain_result(denied)
            return
        args = event.message_str.split()[1:]
        try:
            args = self._resolve_browse_option(event, args)
            rules = await self._validate_rules(args) if args else []
        except (ValueError, KookRateLimited) as exc:
            yield event.plain_result(str(exc))
            return
        except Exception:
            logger.exception("[KOOK Monitor] 验证订阅失败")
            yield event.plain_result(
                "无法查询 KOOK 服务器列表，请检查插件配置或稍后重试。"
            )
            return

        umo = event.unified_msg_origin
        current = self.bindings.get(umo)
        notes: list[str] = []
        if not rules:
            updated = []  # 不带参数 = 全部服务器
        elif not current:
            # 新订阅，或从「全部服务器」缩小为所选项目
            updated, _ = self._merge_rules([], rules)
        else:
            updated, notes = self._merge_rules(current, rules)

        suffix = (
            f"（注意：全局频道过滤正在生效，只监控频道 {self.target_channel_id}）"
            if self.target_channel_id
            else ""
        )
        if current is not None and updated == current:
            if updated:
                lines = [*notes, "订阅没有变化，当前订阅："]
                lines.extend(
                    f"· {label}" for label in await self._describe_rules(updated)
                )
            else:
                lines = ["订阅没有变化：当前已订阅全部服务器的语音频道。"]
            for chunk in self._split_reply(lines):
                yield event.plain_result(chunk)
            return
        if not self._save_targets({**self.bindings, umo: updated}):
            yield event.plain_result("保存失败，原订阅仍然有效。")
            return

        if updated:
            labels = await self._describe_rules(updated)
            lines = [*notes, f"订阅已保存{suffix}，当前订阅："]
            lines.extend(f"· {label}" for label in labels)
            for chunk in self._split_reply(lines):
                yield event.plain_result(chunk)
            return

        lines = [f"订阅已保存：全部服务器的语音频道{suffix}"]
        try:
            guilds = await self._fetch_guilds(refresh=True)
        except Exception:
            logger.warning("[KOOK Monitor] 订阅已保存，但无法查询当前服务器列表")
            lines.append("暂时无法列出服务器名称，可以稍后使用 /kook_browse 查看。")
        else:
            if guilds:
                lines.append("机器人当前所在的服务器：")
                lines.extend(f"· {name}" for name in guilds.values())
            else:
                lines.append("机器人当前还没有加入任何服务器。")
        for chunk in self._split_reply(lines):
            yield event.plain_result(chunk)

    @filter.command("kook_unsub")
    async def unsubscribe(self, event: AstrMessageEvent):
        """退订一个项目。当前有多项时，先列出代号供选择。私聊退订始终允许。"""
        if not event.is_private_chat() and not self._admin_allowed(event):
            yield event.plain_result("仅管理员可以执行该操作。")
            return

        umo = event.unified_msg_origin
        args = event.message_str.split()[1:]
        rules = self.bindings.get(umo)
        if rules is None:
            yield event.plain_result("当前会话还没有订阅。")
            return
        if not args and len(rules) > 1:
            # 代号映射到此刻显示的具体规则，之后列表变动也不会误删其他规则。
            displayed_rules = rules.copy()
            labels = await self._describe_rules(displayed_rules)
            options = {
                str(index): rule for index, rule in enumerate(displayed_rules, start=1)
            }
            now = time.monotonic()
            self._unsubscribe_options = {
                owner: entry
                for owner, entry in self._unsubscribe_options.items()
                if entry[0] > now
            }
            self._unsubscribe_options[umo] = (now + BROWSE_TTL, options)
            lines = ["当前有多项订阅，选择要退订的一项（10 分钟内有效）："]
            lines.extend(
                f"{code}. {label}" for code, label in zip(options, labels, strict=True)
            )
            lines.append("发送 /kook_unsub 序号 退订对应项，例如 /kook_unsub 1。")
            for chunk in self._split_reply(lines):
                yield event.plain_result(chunk)
            return

        bindings = self.bindings.copy()
        if args:
            options = self._fresh_options(self._unsubscribe_options, umo)
            rule = None
            if len(args) == 1 and UNSUB_CODE.fullmatch(args[0]) and options:
                rule = options.get(args[0])
            if rule is None:
                yield event.plain_result(
                    "序号无效或已过期，请重新发送 /kook_unsub 查看列表。"
                )
                return
            if rule not in rules:
                yield event.plain_result("该项已经退订过了。")
                return
            remaining = [item for item in rules if item != rule]
            if remaining:
                bindings[umo] = remaining
            else:
                del bindings[umo]
        else:
            del bindings[umo]

        if self._save_targets(bindings):
            if umo not in bindings:
                self._unsubscribe_options.pop(umo, None)
            if umo in bindings:
                yield event.plain_result(
                    f"已退订所选项，还剩 {len(bindings[umo])} 项订阅。"
                )
            else:
                yield event.plain_result("已取消当前会话的全部 KOOK 订阅。")
        else:
            yield event.plain_result("保存失败，原订阅仍然有效。")

    @filter.command("kook_list")
    async def list_binding(self, event: AstrMessageEvent):
        """查看当前会话的订阅范围。"""
        rules = self.bindings.get(event.unified_msg_origin)
        if rules is None:
            yield event.plain_result(
                "当前会话还没有订阅。发送 /kook_browse 查看可订阅频道。"
            )
            return
        suffix = (
            f"全局仅监控频道 {self.target_channel_id}。"
            if self.target_channel_id
            else ""
        )
        if not rules:
            text = "当前订阅：全部已加入服务器的语音频道。"
            yield event.plain_result(f"{text}\n{suffix}" if suffix else text)
            return

        labels = await self._describe_rules(rules)
        lines = ["当前订阅：", *(f"· {label}" for label in labels)]
        if suffix:
            lines.append(suffix)
        for chunk in self._split_reply(lines):
            yield event.plain_result(chunk)

    async def _self_check(self, event: AstrMessageEvent) -> list[str]:
        """自检报告：Token、长连接、机器人所在服务器、当前会话订阅、全局过滤。"""
        scope = "你" if event.is_private_chat() else "本群"
        lines = ["🔧 KOOK 开黑提醒 · 自检"]

        if not self.kook_token:
            lines.append(
                "❌ 未填写 Token：请到插件配置页填写 KOOK 机器人 Token 后重载插件"
            )
        elif self._http is None:
            lines.append("❌ 插件尚未初始化：请在 AstrBot 插件页重载本插件")
        else:
            try:
                guilds = await self._fetch_guilds(refresh=True)
            except KookAuthError:
                lines.append(
                    "❌ Token 无效：请到 KOOK 开发者中心核对或重置，填回插件配置后重载插件"
                )
            except KookRateLimited:
                lines.append("⚠️ KOOK 接口暂时限流，无法验证 Token，请稍后再试")
            except Exception as exc:
                logger.warning(
                    "[KOOK Monitor] 自检时无法连接 KOOK (%s)", type(exc).__name__
                )
                lines.append(
                    "❌ 无法连接 KOOK：请检查运行 AstrBot 的机器能否访问 kookapp.cn"
                )
            else:
                lines.append("✅ Token 有效")
                if guilds:
                    lines.append(f"✅ 机器人在 {len(guilds)} 个服务器中")
                else:
                    lines.append(
                        "❌ 机器人还没加入任何服务器：请用开发者中心的邀请链接把它拉进你的 KOOK 服务器"
                    )

        icon, text = self._gateway_state
        lines.append(f"{icon} {text}")

        rules = self.bindings.get(event.unified_msg_origin)
        if rules is None:
            lines.append(f"⚠️ {scope}还没有订阅：发送 /kook_browse 开始")
        elif not rules:
            lines.append(f"✅ {scope}已订阅：全部服务器")
        else:
            labels = await self._describe_rules(rules)
            lines.append(f"✅ {scope}已订阅：" + "、".join(labels))

        if self.target_channel_id:
            lines.append(f"⚠️ 全局频道过滤正在生效：只监控频道 {self.target_channel_id}")
        return lines

    @filter.command("kook_test")
    async def test_notify(self, event: AstrMessageEvent):
        """自检插件状态，并向当前会话（发指令的群或私聊）发一条测试消息预览提醒文案。
        只发到当前会话，不会发给其他订阅者；权限与在此处订阅相同。"""
        denied = self._subscribe_denied(event)
        if denied:
            yield event.plain_result(denied)
            return

        # 测试消息走主动推送（与真正的开黑提醒同一路径），先发；自检报告作为指令回复随后发出
        sample = ("测试用户", "0", "测试服务器", "测试频道", "0", None)
        text = "\n\n".join(
            [
                "🔔 KOOK 开黑提醒 · 测试消息\n以下是当前的提醒文案：",
                self._render(True, *sample),
                self._render(False, *sample),
            ]
        )
        push_error = None
        try:
            async with asyncio.timeout(10):
                matched = await self.context.send_message(
                    event.unified_msg_origin, MessageChain().message(text)
                )
            if matched is False:
                push_error = "找不到当前会话所属的平台"
        except Exception:
            logger.exception(
                "[KOOK Monitor] 测试推送失败: %s", event.unified_msg_origin
            )
            push_error = "机器人不能主动往这里发消息，开黑提醒也会发不出来。请检查平台的主动发言权限"

        lines = await self._self_check(event)
        if push_error:
            lines.append(f"❌ 测试消息发送失败：{push_error}")
        for chunk in self._split_reply(lines):
            yield event.plain_result(chunk)

    @filter.command("kook_help", alias={"kook help"})
    async def show_help(self, event: AstrMessageEvent):
        """显示命令菜单。"""
        private_state = "已开放" if self.allow_private_sub else "未开放"
        group_rule = "群订阅需要管理员" if self.admin_only else "群成员都可以订阅"
        yield event.plain_result(
            "\n".join(
                [
                    "🎮 KOOK 开黑提醒 · 命令菜单",
                    "",
                    "【订阅】",
                    "/kook_browse    查看可订阅的服务器和语音频道",
                    "/kook_sub 代号   订阅，例如 /kook_sub 1.2",
                    "/kook_sub       订阅全部服务器",
                    "/kook_list      查看当前订阅",
                    "/kook_unsub     取消订阅（多项时按序号选）",
                    "",
                    "【排查】",
                    "/kook_test      检查插件状态，并在这里发一条测试消息",
                    "/kook_help      显示本菜单",
                    "",
                    "在群里发 = 操作本群，私聊发 = 操作你自己。",
                    f"{group_rule}；私聊订阅当前：{private_state}",
                ]
            )
        )
