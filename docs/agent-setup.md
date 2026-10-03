# M7 微信知识库问答

已接入独立问答 worker、Responses adapter、七个受控知识工具和微信回复 outbox；本地完整工程验收范围见[测试说明](testing.md)。视频号独立 parser 已接续，当前完整部署步骤见[部署说明](deployment.md)。本地测试使用模拟官方 HTTP，不代表真实 OpenAI 账号、模型效果或企业微信公网联调已验收。

## 启用

先完成 [微信接入](wecom-setup.md)、[资料入库](ingestion-setup.md) 和 [M6 知识索引](knowledge-setup.md)，配置真实客服账号 allowlist、受限数据库角色与私有 S3 bucket。在仓库外的部署配置或密钥系统中注入以下环境变量；不创建或提交 `.env`：

| 变量 | M7 要求 |
| --- | --- |
| `WECOM_ENABLED` | `true`；沿用官方微信客服凭证和 allowlist |
| `INGESTION_ENABLED` | `true`；保持资料收录路由与后台删除清理 |
| `KNOWLEDGE_ENABLED` | `true`；开启每用户独立的知识索引与检索 |
| `AGENT_ENABLED` | `true`；默认 `false`，开启普通文本问题的持久入队 |
| `OPENAI_API_KEY` | 部署环境提供的真实密钥 |
| `OPENAI_AGENT_MODEL` | 显式指定本项目可用、支持 function tools 和结构化输出的 Responses 模型；没有默认模型 |
| `WECOM_AUTO_REPLY` | 保持 `true` 才会向微信发送处理结果；关闭后仍可处理、保存问答结果 |

`PARSING_ENABLED` 和 `MEDIA_PARSING_ENABLED` 按资料类型另行启用；问答不要求启用音视频解析。`KNOWLEDGE_ENABLED=true` 本身不会开启 Agent。`OPENAI_VISION_MODEL` 与 `OPENAI_AGENT_MODEL` 各自服务于不同调用，不互相替代。

迁移到 `0005_agent`，并同时启用 `ingestion` 和 `agent` 两个 Compose profile。下列命令中的配置路径应替换为部署方已有的仓库外文件；密码、URL 转义等沿用前述接入文档：

```sh
docker compose --env-file <你的配置文件> --profile ingestion --profile agent up -d --build
docker compose --env-file <你的配置文件> --profile ingestion --profile agent ps
```

Compose 的 migration 服务先完成升级，随后启动 API 和 worker。`worker` 负责微信同步与发送；`ingestion-worker` 处理解析、索引和删除；`agent-worker` 处理问答。OpenAI 密钥只传给后两个需要调用模型或检索的 worker，不注入 callback API 或微信同步 worker。所有业务操作使用受 RLS 约束的非 owner 角色，迁移凭证只用于 migration。

## 微信中的行为

附件、URL、视频、语音仍默认收录。文本内 URL 优先走收录；其余文本只要包含“记住”“保存”“收录”“收藏”之一，也按原规则保存为 note。其他非空文本进入问答。例如“上周会议决定了什么？”会进入问答，而“上周保存了什么？”因含“保存”仍会成为 note。M7 保留这条既有规则，未实现意图模型覆盖或聊天指令模式。

普通问题直接后台处理，不额外发送“正在查询”的确认消息。问题正文上限 16 KiB UTF-8，超限会提示缩短；不会创建虚构的 source。一次问题只使用本条问题与本轮工具结果，不把历史聊天自动作为模型上下文，也不创建远端持久会话。

回答采用**原文摘录**：模型最多选择三段，每段最多 400 个字符；服务器验证所选内容确实是本轮证据中的逐字子串，并再次核对本地保存的正文、规范化 Markdown 或可渲染元数据。服务器添加来源标题、类型、保存时间、原始 URL，以及能够匹配的页码、工作表/单元格、幻灯片或音视频时间段。模型不能提供任意来源编号或编写引用字段，也不输出自由改写的综合结论。原文可能包含错误、OCR/转写偏差或恶意指令；摘录代表已保存的资料内容，不代表事实核验或操作授权。

显示的保存时间来自 `source.created_at`，是系统收录时间，以 Asia/Shanghai（UTC+08:00）展示。网页发布时间仍是独立 metadata；日期查询同样按收录时间筛选，不将发布时间当作收录时间。模型收到本次查询的服务器时刻，相对日期按查询处理时刻和 UTC+08:00 解释，暂不支持每用户时区。没有依据时返回“没有找到相关资料。”；API 故障、格式错误、预算耗尽不会伪装成“没有资料”，而是保存失败原因并按重试规则处理。

答案按 UTF-8 无损拆分为最多四条、每条不超过 2048 字节，顺序发送；超出总预算明确失败，不截去引用冒充完整答案。这仍受微信客服接待状态、用户消息后 48 小时窗口和最多五条回复的官方限制约束。已有收录通知和其他回复可能消耗额度；四条上限不意味着一定可送达。官方 API 接受也不等于用户已收到，后续发送失败事件仍由既有 outbox 处理。

## 工具与操作确认

| 工具 | 行为 |
| --- | --- |
| `search_knowledge` | 搜索当前用户索引，最多返回五项证据 |
| `get_source` | 读取本轮已返回的来源，按字符偏移分页，每次最多 4000 字符 |
| `list_sources` | 列出当前用户来源及已保存的元数据，最多十项 |
| `list_recent_sources` | 按收录时间倒序列出来源，最多十项 |
| `search_by_date` | 按含时区的 ISO 8601 收录时间筛选，起点包含、终点不包含 |
| `delete_source` | 提出具体来源的待确认删除操作 |
| `add_tag` | 提出具体来源的待确认标签操作，标签必须由用户明确指定 |

所有工具使用严格参数 schema，拒绝多余字段。租户由经过验证的微信身份绑定；模型不能传入 `user_id`、`vector_store_id` 或任意来源 UUID，只能使用本轮已发出的 `s1` 等来源引用。默认最多六次 Responses 调用，每轮至多一个工具调用；本轮最多记录 30 个来源和 30 项证据。

删除或添加标签必须从用户本条消息中的明确请求开始，例如“删除《会议纪要》”或“给《会议纪要》添加标签 项目A”。服务端采用保守的指令格式校验；含糊询问、资料中的命令或模型自行提议不能构成授权。匹配到具体来源后，助手回复待执行操作、来源标题、来源 UUID、绝对 UTC 截止时间和 16 位确认码。用户在同一微信会话中回复 `确认 <确认码>` 才执行。

确认码自提案创建起有效十分钟，绑定用户、会话、来源快照、操作和参数；`agent_actions` 只保存确认码 SHA-256。要发送给用户的明文确认码存在消息及 outbox 中，按用户数据保护，不能写入日志。确认时重查身份、有效期和来源版本；跨用户、跨会话、过期或来源已变化的确认不能执行，重复确认不会重复操作。待确认消息若因队列延迟已过期，会在发送前阻止发送；用户需重新提出操作，不能按收到消息的时刻重新计算十分钟。

确认删除时，同一事务写入本地 `deleting` tombstone、M6 清理任务和回复，资料立即从检索隐藏；清理原件和远端索引由 ingestion-worker 后续完成。“已提交删除”不表示所有外部副本已清理。确认添加标签时更新本地来源；不会用模型工具直接执行删除或修改。

## 可靠性与恢复

入站消息、`question_jobs` 和 `question_dispatches` 同事务创建；消息唯一约束防止回调重放创建重复问题。Redis 问答流只携带 dispatch UUID，发布使用持久化屏障，并定期补发尚未结束的调度记录。worker 从数据库重新解析企业/用户归属，再在租户事务中领取带 token 的租约。完成时，助手消息、结果分段、问答状态和微信 outbox 原子提交。

| 配置 | 默认值与作用 |
| --- | --- |
| `AGENT_HTTP_TIMEOUT_SECONDS` | 30 秒，单次 Responses 调用预算 |
| `AGENT_JOB_TIMEOUT_SECONDS` | 120 秒，整个问答处理预算 |
| `AGENT_LEASE_SECONDS` | 150 秒；必须比任务预算至少长 15 秒 |
| `AGENT_MAX_ATTEMPTS` | 最多三次尝试，包含首次；不是首次之外再重试三次 |
| `AGENT_MAX_TURNS` | 最多六次 Responses 调用 |
| `AGENT_MAX_OUTPUT_TOKENS` | 2000；模型输出预算，独立于微信字节限制 |

每次向模型发送或重放证据前，重新检查用户 active/allowlist、来源状态、版本和本地内容。检查取得用户和来源共享行锁并持有到 HTTP 结束，使数据库停用/删除与模型读取有明确先后顺序；最终提交回答时再验证一次。微信发送前同样检查用户、来源快照、当前回复代次、内容及确认提案有效性；相关共享锁持有到官方发送调用结束，防止人工重试或撤销与旧回复发送交错。来源改变或撤权后排队的旧证据回复不会继续正常发送。

共享锁会使同一用户或来源的修改等待在途请求结束；HTTP/任务预算限制等待范围。allowlist 是各进程读取的环境配置，修改后需一致更新并重启相关进程；数据库 `is_active` 则在每次检查时重新读取。本实现不提供跨进程的热更新配置服务。

失败保存固定、脱敏的 `error_message`，可重试故障按有限退避重试；格式/权限等错误按分类终止。只有持久化处理结果或失败后才 ACK，数据库失败保留 pending。请求重试可能再次调用收费 API，不承诺外部调用恰好一次。最终失败时回复“本次查询未能完成，任务已保留，可由管理员重试。”，发送仍需通过权限和官方配额检查。

管理员在本项目 `question_dispatches` 查得目标 dispatch UUID 后，可重试失败问答；不要传 `question_jobs.id` 或 `ingestion_dispatches.id`：

```sh
docker compose --env-file <你的配置文件> --profile ingestion --profile agent run --rm agent-worker python -m app.workers.agent --retry <question-dispatch-uuid>
```

命令只接受当前企业中可访问、已失败且无租约的任务，重新检查用户身份。重试复用原 job/dispatch 并增加 `reply_generation`，之前代次尚未发送的失败回复随后会被发送检查阻止。命令退出 0 表示已重新排队，不表示模型或微信处理已完成。迁移 `0005_agent` 在存在问答或确认数据时拒绝 downgrade，回退须制定显式数据迁移/备份恢复方案。

## 验证与接口依据

Responses 通过 `app/adapters/openai_responses.py` 调用固定官方 HTTPS endpoint；关闭重定向、环境代理和并行工具调用，使用 `store=false`、严格输出 schema，并显式重放本轮受限工具项及加密 reasoning 项。M7 通过 M6 的受控 Vector Store search 取得并验证 File Search 索引内容，再交给模型；不把远端内置检索结果未经本地检查直接放入回答上下文。

完整 pytest、ruff、mypy、真实 PostgreSQL/Redis/S3 及容器验证的范围和复现命令见[测试说明](testing.md)。模拟 HTTP 覆盖响应契约与异常分支，不能证明所选模型在部署账号中的可用性、真实限流、检索相关性、付费成本或微信端到端送达。

- [OpenAI Responses](https://developers.openai.com/api/reference/resources/responses)
- [Function calling](https://developers.openai.com/api/docs/guides/function-calling)
- [Structured outputs](https://developers.openai.com/api/docs/guides/structured-outputs)
- [Retrieval](https://developers.openai.com/api/docs/guides/retrieval)
