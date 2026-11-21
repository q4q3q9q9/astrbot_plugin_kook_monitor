# KOOK 语音频道进出监控（AstrBot 插件）— 开发者 / AI Agent 参考

> 本文件是给开发者和 AI Agent 看的完整说明（实现细节、全部配置与边界情况）。面向普通用户的简明说明见 [README.md](README.md)。

## 代码结构

全部逻辑在 `main.py`，按数据流分为几段：

1. **Gateway**（`_kook_gateway_loop` / `_read_gateway` / `_dispatch_signal` / `_heartbeat_loop`）：维护 KOOK WebSocket 长连接。事件按 `sn` 暂存、按序放入 `_events`；`current_sn` 只推进到连续处理到的位置，心跳回传它。断线后带 `session_id` + `sn` 续传（resume），连续 3 次续传失败、收到 `s=5` 或 HELLO `40103` 时调用 `_reset_gateway()` 清空状态重建会话。HELLO `40100~40102` 或 HTTP 401 视为 Token 无效，停止监控。
2. **事件处理**（`_process_events` → `_handle_kook_event` → `_on_user_joined` / `_on_user_exited`）：只做状态判断，不做网络请求。退出防抖任务按 `(guild_id, channel_id, user_id)` 存在 `leave_debounce_tasks`；倒计时结束后调用 `channel-user/get-joined-channel` 核实用户确实已离开，核实期间任务仍登记在表中，可被卸载或「回到频道」取消。
3. **通知组装**（`_process_notifications`）：在事件发生时就确定推送目标（`targets`，每项是 `(umo, 订阅代次)`），查昵称/服务器名/频道名后按模板渲染。`_gateway_generation` 用于丢弃会话重建前的旧通知。
4. **发送**（`_queue_send` / `_send_to_target`）：每个推送目标一个独立队列和发送任务，单次发送 10 秒超时，一个平台卡住不影响其他目标。
5. **订阅存储**（`_load_bindings` / `_save_targets`）：订阅存在 `data/plugin_data/kook_monitor/bindings.json`。`_save_targets` 先原子写盘，成功后才替换内存；失败时返回 False，指令回复「保存失败，原订阅仍然有效」。某个会话的订阅被删除或收窄时，`_sub_epoch` 中它的代次 +1；发送前用 `_still_targeted()` 复核代次，所以退订前已排队的通知，即使随后马上重新订阅也不会再发出。
6. **指令**：`/kook_browse`、`/kook_sub`、`/kook_unsub`、`/kook_list`、`/kook_test`、`/kook_help`（别名 `kook help`）。`/kook_test` 的自检报告来自 `_self_check()`，长连接状态取自 `_gateway_state`（在连接循环中更新）。浏览/退订代号按会话（`unified_msg_origin`）共享，映射到列表显示时的具体规则；`_merge_rules` 保证订阅项互不重叠。

修改须知：

- 配置项定义在 `_conf_schema.json`。**新增配置必须同时写进 schema**，否则 AstrBot 加载配置时会删掉不在 schema 里的键（见 AstrBot `check_config_integrity`）。
- 旧版字段 `target_sessions` / `target_users` / `bindings` / `bindings_json` 必须继续保留在 schema 里（已设 `invisible`），否则老用户升级时数据会在插件初始化前被删除。
- 订阅规则格式：`[]` = 全部服务器；`服务器ID/*` = 整台服务器；`服务器ID/频道ID` = 单个频道；纯 `频道ID` = 1.3/1.4 旧版规则，仍会匹配。

## 工作原理

- 插件自己持有一条 KOOK WebSocket 长连接，不依赖 AstrBot 自带的 KOOK 适配器（官方适配器不透传 type 255 系统事件）；
- 收到 `joined_channel` / `exited_channel` 后，调 `user/view` 补昵称（带 `guild_id` 取服务器内昵称）、`channel/view`（参数 `target_id`）补频道名、`guild/list` 补服务器名，均带 1 小时缓存；
- 通过 `context.send_message(unified_msg_origin, chain)` 推送到订阅的群或私聊；
- 退出防抖：退出后延迟 N 秒（默认 120）才发通知，期间回到**同一个**频道则进出都不提醒；换到其他频道不算闪断。倒计时结束后再向 KOOK 核实一次，核实接口失败时照常发送（宁可多发，不漏发）；
- HTTP 请求遵守 KOOK 限流头（`X-Rate-Limit-*`），遇到 429 会按 `X-Rate-Limit-Reset` 暂停对应接口。

## 安装

在 AstrBot WebUI 的插件页面安装（插件市场或填仓库地址），安装后在该插件的配置页面填写配置项。

手动安装：把整个 `astrbot_plugin_kook_monitor` 文件夹放入 AstrBot 的 `data/plugins/` 目录（Docker 部署时放入映射出来的目录），重启 AstrBot 或在 WebUI 插件页重载。

## 获取 KOOK 机器人 Token

插件只需要 Token，不需要机器人 ID：

1. 打开 [KOOK 开发者中心](https://developer.kookapp.cn) 并登录（需要实名认证）；
2. 点击「**新建应用**」，自定义机器人名称；
3. 进入应用后台，左侧栏点「**机器人**」：
   - 连接模式选择 **WebSocket**（不要选 Webhook）；
   - 复制页面上显示的 **Token**；
4. 同一页面下方点「**邀请链接**」，把机器人邀请进要监控的 KOOK 服务器——机器人不在服务器里就收不到任何事件。

**最小权限**：插件只监听事件、查询用户和频道信息，从不在 KOOK 内发消息，邀请时只需勾选「**查看频道**」。

> ⚠️ 某个语音频道若对机器人所在角色设置了「禁止查看」，该频道的进出事件将收不到，`/kook_browse` 也列不出它。

> ⚠️ Token 等同于机器人密码，请勿泄露或提交到公开仓库；泄露后可在开发者中心重置。

## 配置（AstrBot 插件配置页面）

页面先列基本设置（Token → 进入通知 → 离开通知 → 离开延迟 → 群订阅仅限管理员 → 允许普通用户私聊订阅），再列高级设置（进入文案 → 离开文案）。关闭进入/离开通知后，对应的文案和延迟项会自动隐藏（schema 的 `condition` 字段）。AstrBot 4.16 的配置页不支持密码遮罩，Token 以明文输入框显示。

| 配置项 | 说明 |
|---|---|
| `kook_token` | 必填。KOOK 机器人 Token |
| `notify_join` / `notify_leave` | 进入 / 离开通知开关 |
| `join_template` / `leave_template` | 进入 / 离开通知文案，留空用默认文案。占位符见下方 |
| `leave_delay` | 退出通知延迟秒数（防抖），默认 120 |
| `admin_only` | 只管群聊：开启后，群里的 `/kook_browse`、`/kook_sub`、`/kook_unsub`、`/kook_test` 仅 AstrBot 管理员可用（默认开启） |
| `allow_private_sub` | 只管私聊：是否允许非管理员在私聊里浏览、订阅和使用 `/kook_test`（默认关闭）。开启后任何能私聊机器人的人都能订阅并看到谁进出了语音频道；私聊取消订阅始终允许 |
| `target_channel_id` | 全局频道过滤（**页面已隐藏**）。老用户填过的值仍生效，优先于所有订阅范围；生效时启动日志告警，`/kook_sub`、`/kook_list` 回复和 `/kook_browse` 列表中都会提示 |
| `target_sessions` / `target_users` / `bindings` / `bindings_json` | 旧版订阅数据（页面隐藏）。启动时合并进 `bindings.json`，写盘成功后清空；写盘失败则保留，下次启动再迁移 |

### 自定义文案

| 占位符 | 含义 |
|---|---|
| `{user}` | 用户昵称（优先服务器内昵称，查不到时为「用户(ID)」） |
| `{guild}` | 服务器名（查不到时为「服务器 ID」） |
| `{channel}` | 语音频道名（查不到时为「语音频道 ID」） |
| `{user_id}` / `{channel_id}` | KOOK 用户 ID / 频道 ID |
| `{time}` | 事件发生时间（取 KOOK 事件自带的进入/退出时间，固定按北京时间显示，不受服务器/容器时区影响），格式 HH:MM |

默认文案：

```
🎮【KOOK 开黑提醒】
{user} 进入了「{guild} / {channel}」！快来上车~

👋【KOOK 开黑提醒】
{user} 离开了「{guild} / {channel}」。
```

未知占位符原样输出；输出花括号本身请写 `{{` 和 `}}`；模板写法有误（如 `{user.name}`、`{0}`、未闭合的 `{`）时，加载时自动回退默认文案并在日志中提示。发送 `/kook_test` 可在当前会话预览两条文案的实际效果。

## 指令

规则：**在群里发 = 操作这个群，私聊发 = 操作你自己**。

| 指令 | 作用 |
|---|---|
| `/kook_browse` | 列出机器人所在的服务器和语音频道，以及各自的订阅代号 |
| `/kook_sub` | 订阅全部服务器的语音频道，并列出机器人当前所在的服务器名称 |
| `/kook_sub 代号` | 订阅目录中代号对应的服务器或语音频道 |
| `/kook_sub 服务器ID` | 订阅这一台服务器的全部语音频道 |
| `/kook_sub 服务器ID 频道ID1 频道ID2` | 订阅指定服务器的一个或多个语音频道 |
| `/kook_list` | 查看当前会话的订阅范围 |
| `/kook_unsub` | 只有一项时直接退订；有多项时列出带序号的列表 |
| `/kook_unsub 序号` | 退订序号对应的一项，保留其他订阅 |
| `/kook_test` | 自检并测试：先向当前会话（发指令的群或私聊）主动推送一条测试消息预览文案，再以指令回复的形式发出自检报告（Token 是否有效、长连接状态、机器人所在服务器数、当前会话订阅、全局过滤）。只发到当前会话，无需先订阅；权限与在此处订阅相同。主动推送失败时报告末尾会给出 ❌ 原因 |
| `/kook_help` | 命令菜单，别名 `/kook help`；末行按配置显示群订阅规则和私聊订阅是否开放 |

- 带参数的 `/kook_sub` 是**追加**。从「全部服务器」状态首次指定服务器或频道时，范围缩小为新选的项目；不带参数的 `/kook_sub` 切回「全部服务器」。
- 订阅指定范围前会用当前 Token 核对：机器人是否在该服务器、频道是否属于该服务器、是否为语音频道。任一项失败都保留原订阅。
- 浏览代号为 `1`（整台服务器）或 `1.2`（第 1 台服务器的第 2 个频道），退订序号为 `1`、`2`……；KOOK 真实 ID 是十几位长数字，不会与 1~3 位的代号混淆。
- 代号按会话共享：群里任何人 `/kook_browse` 出的列表，管理员都可以直接用来订阅；以最近一次列表为准，10 分钟有效。每个代号映射到列表显示时的具体服务器/频道，订阅回复会列出实际结果；退订序号同理，列表显示后即使有人先退订了别的项，也不会误删，已退订的项会提示「已经退订过了」。
- 订阅项互不重叠：已订阅整台服务器时，再订阅其中的频道不会新增（回复「已包含」）；改订整台服务器时，替换该服务器下已有的频道订阅。1.3/1.4 遗留的纯频道 ID 规则不知道所属服务器，保持原样。
- 指令前缀以 AstrBot 唤醒词为准。手动填 ID 时，可在 KOOK 客户端「设置 → 高级设置」开启开发者模式后右键复制。
- 私聊推送依赖平台的主动私聊能力，多数平台要求订阅者先和机器人建立过私聊（如 OneBot/QQ 需先加好友，Telegram 需先给机器人发过消息）。

## 从旧版升级

| 旧版本 | 旧订阅存放位置 | 1.7.0 的处理 |
|---|---|---|
| 1.2 及更早 | 配置项 `target_sessions` / `target_users` | 迁移为「全部服务器」订阅 |
| 1.3 / 1.4 | 配置项 `bindings`（对象） | 迁移，旧的纯频道 ID 规则仍可匹配 |
| 1.5 / 1.6 | 配置项 `bindings_json`（JSON 字符串） | 迁移；无法解析时跳过并保留原值，日志报错 |

迁移目标是 `data/plugin_data/kook_monitor/bindings.json`。每批旧数据导入后，其指纹（SHA-256）记录在同目录的 `legacy_imported.json`；即使随后清理旧配置失败（如配置文件不可写），下次启动也不会重复导入，避免已退订的会话被恢复。如果该文件已存在，旧数据会与之合并（同一会话任一方是「全部」则取「全部」）。如果旧版本运行时数据已经被 AstrBot 清理（旧字段当时不在 schema 中），1.7.0 无法找回，需要重新订阅；也可以从备份中取出旧订阅，直接编辑 `bindings.json`。

## 多平台兼容性

插件不含平台专属逻辑：订阅以 `unified_msg_origin` 记录，推送统一走 `context.send_message()` → 各平台适配器的 `send_by_session()`；推送内容为纯文本（含 emoji）。**已在 OneBot v11（NapCat / aiocqhttp）下实测**，其余平台依据 AstrBot 源码中各适配器的 `send_by_session` 评估：

| 平台 | 状态 | 说明 |
|---|---|---|
| OneBot v11（NapCat 等） | ✅ 已实测 | 私聊需先加好友 |
| Telegram / Discord / Slack / 飞书 / 钉钉 / LINE / Satori / Misskey / Mattermost / KOOK | ✅ 应可用 | 适配器实现了主动发送；私聊一般需用户先和机器人对话过 |
| 企业微信（自建应用） | ✅ 应可用 | 客服模式不支持主动发送 |
| 企业微信智能机器人 | ⚠️ 需配置 | 需在 AstrBot 平台配置里填「消息推送 Webhook URL」 |
| QQ 官方机器人（qq_official / webhook） | ⚠️ 不推荐 | 群推送依赖内存中缓存的会话场景，AstrBot 重启后在该群有人与机器人互动前会被静默跳过（不报错）；另受 QQ 平台主动消息限制 |
| 微信公众号 | ❌ 不可用 | 适配器明确不支持主动消息 |

插件能识别的失败：适配器抛异常、发送超时（10 秒）、`send_message` 返回 `False`（找不到平台，通常是平台 ID 改名）。适配器内部静默跳过的情况插件无法感知。

## 注意事项

- 机器人在 KOOK 服务器里才能收到该服务器的事件；
- 事件队列、通知队列各上限 256，每个推送目标的发送队列上限 32；满了会丢弃新通知并记录告警；
- 若同时在 AstrBot 接了 KOOK 官方适配器（用于文字聊天），建议使用**不同的机器人应用/Token**，避免同一个 Token 两处挂连接；
- AstrBot 管理员的判定以 AstrBot 自身配置为准。

## License

[MIT](LICENSE)
