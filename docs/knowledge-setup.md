# M6 知识索引与检索

M6 使用 OpenAI Vector Store / File Search 的检索层保存和搜索规范化 Markdown。Responses Agent 与微信自然语言回答由 [M7](agent-setup.md) 单独启用；开启索引不会开启聊天回答。

## 开启与观察

1. 先完成 [微信接入](wecom-setup.md) 和 [资料入库](ingestion-setup.md)，按需开启 M4/M5 解析。配置真实微信客服身份 allowlist 和私有 S3 bucket。
2. 在部署环境注入 `KNOWLEDGE_ENABLED=true` 和 `OPENAI_API_KEY`，不创建或提交 `.env`。索引独立于 `MEDIA_PARSING_ENABLED`，不需要视觉/聊天模型配置。凭证提供给 ingestion-worker；同时开启 M7 时 agent-worker 也需要该凭证，callback 与微信同步 worker 不接触 OpenAI 密钥。
3. 执行现有 Compose migration，然后启动 `ingestion` profile。使用现有 `app.workers.ingestion` 进程处理解析、索引和删除任务。
4. 发送“保存：测试笔记”。先收到“收到，正在整理”；正文存储成功时创建独立索引任务。OpenAI 文件状态 `completed` 后 source 才变成 `ready`，并通过既有 outbox 回复“已收录”。不能提取正文的资料继续保持 `metadata_only`，不谎报收录可检索。

`KNOWLEDGE_HTTP_TIMEOUT_SECONDS` 默认 20，单篇 Markdown 上限 `KNOWLEDGE_MAX_DOCUMENT_BYTES` 默认 8 MiB；轮询间隔默认 10 秒。Redis 投递去重/重新发布窗口为 60 秒，实际下一次轮询可能稍晚。`in_progress` 正常轮询不消耗错误重试次数；等待超过 `KNOWLEDGE_INDEX_TIMEOUT_SECONDS`（默认一小时）记录失败。HTTP 错误按既有有界退避重试；最终失败保留正文和 error_message。

## 历史资料与重试

回填只处理当前企业 allowlist 中已存储且尚未排队的正文，不重复下载、OCR、转写，也不向旧会话发送通知。每次最多 1000 项，可重复运行。以下命令沿用部署时的配置文件，将占位路径及 UUID 替换为实际值：

```sh
docker compose --env-file <你的配置文件> --profile ingestion run --rm ingestion-worker python -m app.workers.ingestion --backfill
```

管理员从本项目数据库 `ingestion_dispatches` 查询失败任务 UUID 后，使用原有命令重试。不要使用迁移/超级用户凭证运行 worker：

```sh
docker compose --env-file <你的配置文件> --profile ingestion run --rm ingestion-worker python -m app.workers.ingestion --retry <dispatch-uuid>
```

上传/创建成功但应答丢失时，重试按持久化的用户标记/文件名核对远端。明确的请求拒绝可以重新提交；超时、连接中断和不完整成功应答属于结果不确定，不能盲目重新创建。若完整扫描仍找不到对象，保留 `knowledge_*_outcome_unknown` 错误，允许以后重试核对；需要运维核查项目与远端结果。扫描出现多项匹配或不完整时同样失败，不自动选一项或清理未知对象。本实现不声称跨 PostgreSQL/OpenAI 的恰好一次事务。

M6 每个来源只维护一个不可变 Markdown 版本。重复索引返回同一 dispatch，人工重试恢复同一远端文件。修改正文后原位重建索引不在 M6 的编辑接口内；若检测到内容变化，明确失败而不会用旧版本冒充新内容。

若进程恰好在 pending 标记提交后、发出 HTTP 之前退出，远端确实不会有对象。普通重试仍保留不确定状态。此时先停止所有本项目 ingestion worker（包括其他副本），核实无在途请求，并在正确 OpenAI 项目中确认相关标记/文件没有遗漏；再运行显式修复命令。该命令通过受限业务角色再次完整核对远端，找到对象则恢复映射，只有运维已确认缺失时才释放 pending 并重新排队：

```sh
docker compose --env-file <你的配置文件> --profile ingestion stop ingestion-worker
docker compose --env-file <你的配置文件> --profile ingestion run --rm ingestion-worker python -m app.workers.ingestion --resolve-unknown <dispatch-uuid> --confirm-remote-absence
docker compose --env-file <你的配置文件> --profile ingestion up -d ingestion-worker
```

修复要求目标任务已失败且本租户无有效处理租约；仍在工作的副本或未核实的远端结果不能使用此命令，否则可能造成重复远端资源。普通 `--retry` 无此释放行为。人工重试刷新独立 `index_started_at`，正常轮询不延长索引截止时间。

## 服务接口与隔离

`KnowledgeService` 提供 `index_document`、`delete_document`、`search`、`get_source`、`list_sources`。索引和删除返回持久调度 UUID；检索返回正文片段及本地可信来源标题、类型、原 URL、创建日期和匹配片段定位。

调用者必须用已验证的微信身份构造内部 `TenantContext`。它不是客户端输入或认证凭据；M6 不开放接受任意 user_id 的 HTTP 路由。每次查询重查企业、active 与 allowlist，并在非 owner 的 PostgreSQL RLS 会话中执行。store ID 只读自用户记录；每用户独立 store，远端属性过滤 user_id，返回后再关联本地来源、当前 file ID、版本及 tombstone。检索结果无依据时返回空集合，不生成答案。

删除同事务写入 `deleting` 和任务后立即对 search/get/list 隐藏。后台先 detach，再删除 OpenAI File，再逐项删除 S3 原件/派生资产，最后标记 `deleted`。S3 清理核对资产记录及精确的用户/来源前缀，包含上传后数据库事务回滚而未留下资产记录的对象；存储凭证需具备该 bucket 的 ListBucket、GetObject、PutObject、DeleteObject 权限。分页扫描不完整或越界时失败，不将部分清理报告为完成。任一失败保留 tombstone；重试删除不存在对象视为成功。已授权删除任务在 allowlist 后续撤销时仍可清理。共享用户 store 保留，其他来源不受影响。

## 验证边界与官方来源

开发验收使用合成 OpenAI HTTP 响应和真实 PostgreSQL、Redis、S3，不能替代部署方真实 API 账户权限、限流与语义检索效果评测。默认 `KNOWLEDGE_ENABLED=false`。

- [Retrieval 与最终一致的删除](https://developers.openai.com/api/docs/guides/retrieval)
- [Vector Store search](https://developers.openai.com/api/reference/resources/vector_stores/methods/search)
- [Vector Store files](https://developers.openai.com/api/reference/resources/vector_stores/subresources/files)
- [Files 上传与删除](https://developers.openai.com/api/reference/resources/files)

接口核对日期：2026-10-02。M6 直接使用官方 Vector Store search 检索 File Search 索引，使素材进入 M7 模型之前即可完成本地权限/删除过滤。
