# 收录配置与恢复

收录层负责下载、原件保存、统一文档和任务恢复。默认只解析保存词文本及 UTF-8 `.txt/.md/.markdown` 文件；启用 `PARSING_ENABLED` 后增加 PDF、Office、图片、网页和公众号正文解析，启用 `MEDIA_PARSING_ENABLED` 后增加音视频解析。视频号卡片独立保存为 `metadata_only`，不下载隐藏视频。知识索引和问答已经实现，分别通过 `KNOWLEDGE_ENABLED`、`AGENT_ENABLED` 开启；完整配置见[部署说明](deployment.md)。

## 启用

1. 先按 [微信客服配置](wecom-setup.md) 设置官方应用、callback 和 allowlist。`INGESTION_ENABLED` 默认 false；空 allowlist 始终拒绝。
2. 由部署方预先建立私有 S3/MinIO bucket，关闭公开访问，启用所需备份/加密和生命周期策略。应用凭证只授予该 bucket 的必要对象写权限，不提供管理凭证。生产 endpoint 使用 HTTPS；代码允许显式私网 HTTP 以便接入内部对象存储。
3. 设置 `S3_ENDPOINT_URL`、`S3_ACCESS_KEY_ID`、`S3_SECRET_ACCESS_KEY`、`S3_BUCKET`、`S3_REGION` 与 `INGESTION_ENABLED=true`。完整模板见 `.env.example`。不得将真实凭证写进 Git。
4. 执行 `docker compose --env-file <配置文件> --profile ingestion up --build -d`。这会启动 API、微信同步/回复 worker 及 ingestion-worker，迁移先于 worker。API 仍只解密回调并持久入 Redis。

M3 新增 `0003_ingestion` 迁移，不改 M1/M2 历史 revision。正式升级前备份数据库，先停止旧 worker、执行迁移，再启动新版本；存在 M3 数据时降级会主动拒绝，不能用丢弃数据的方式回退。部署使用非 owner 的 `pkb_app` / `pkb_connector`，worker 不接收管理员 DSN。

## 用户可观察行为

- “记住/保存/收录/收藏”文本：先确认整理，后台保存完整原文、Canonical JSON 与 Markdown。
- 附件：官方媒体流式下载，默认最大 20 MiB；默认最多解析 2 MiB UTF-8 文本。超限下载明确失败，无法解析的格式保留原件并标记 `metadata_only`。
- 多链接：每条消息默认最多 10 个不同链接，分别建 source；超过时整条不创建任务并提示分批发送。链接优先于文本保存词；附注进入 metadata。
- 未启用知识索引时回复 `已保存：…（尚未建立检索索引）`；有正文且远端索引成功后才进入 `ready` 并回复“已收录”。未解析时明确保存的是原件/元数据，`stored` 和 `metadata_only` 都不表示正文可检索。
- 开启 Agent 后普通问题进入[知识库问答](agent-setup.md)；关闭时回复功能边界说明。回复遵守官方 48 小时/5 条配额；多链接可能消耗多条通知，任务完成不保证每条通知都能在微信送达。超过额度保留 deferred 状态供运维处理。

## 一致性和资源限制

入站消息、来源、job、dispatch 和首次回复同一租户事务提交。Redis 只携带 dispatch UUID，消费端通过受限企业角色回查权威用户和任务，再绑定租户事务。调度表每 60 秒重新发布未完成记录；投递后进程退出、ACK 丢失或 Redis 恢复均通过数据库状态和幂等键收敛。

任务默认 90 秒总预算、120 秒租约、最多 3 次尝试。配置要求租约至少比预算长 15 秒。外部 S3 SDK 在线程执行；取消时等候有界网络调用释放文件，最终提交再次检查租约。下载有体积、连接/读取和总时间上限，不跟随官方媒体接口重定向，也不执行响应里的任意下载 URL。

SHA-256 仅在同用户内去重，事务锁串行化同用户相同内容的最终写入。重复发送会增加消息来源映射，使用既有来源；不同用户保存独立对象。原件 SHA 是真实存储字节的哈希；正文的换行归一化不改变原件。URL SHA 使用规范化 URL 的 UTF-8 字节，附注保存在 metadata。

对象键形如 `users/{内部用户UUID}/sources/{内部来源UUID}/{original|canonical|markdown}/{sha256}`。外部文件名只作 metadata。数据库与 S3 不能形成一个原子事务：上传成功后数据库回滚，重试覆盖同一确定性键；永久失败可留下无数据库引用的对象。当前没有自动孤立对象删除器，运维应按引用清单与足够保留期审计，不能直接删除正在执行任务的对象。索引和删除补偿见[知识库配置](knowledge-setup.md)，备份维护见[部署说明](deployment.md)。

## 查询和人工重试

`ingestion_jobs` 保留 `error_message`、`attempts`、`max_attempts`、`next_retry_at`、租约和阶段状态。错误只存固定代码，不存原始异常或凭证。瞬时故障指数退避加 jitter；永久错误或次数耗尽停在 failed。

使用部署方受控的数据库管理路径查询目标用户的失败 job 及其 `ingestion_dispatches.id`，随后执行：

```sh
docker compose --env-file <配置文件> --profile ingestion run --rm ingestion-worker python -m app.workers.ingestion --retry <dispatch-uuid>
```

只接受本企业已存在的失败任务，重置尝试次数并保留最后错误直至成功。重复领取和已完成任务不会再次保存。租约有效时不会由其他 worker 抢占；租约到期后恢复。allowlist 在领取及最终保存前再次核对。默认重新分发周期可能造成最多约两轮的重试可见延迟。

微信发送失败的重试仍使用 M2 的 outbox 运维入口。完成通知不会越过仍在 queued/uncertain 的首次确认；uncertain 需先人工裁决，不能假设超时等于未发送。

## 验证边界

`uv run python scripts/verify.py --official-mirror` 在随机一次性项目中运行真实 PostgreSQL、Redis 和 S3 兼容服务。对象存储测试使用固定 digest 的 SeaweedFS 4.47，仅存在于 `compose.test.yml`，无宿主端口；随机临时凭证、私有 bucket、测试结束自动清理。它验证 S3 协议实现，不能据此声称已验证部署方 MinIO 或 AWS IAM 策略。真实企业凭证、客服权限及公网微信端收发仍需部署方联调。

手工集成测试除已有 DB/Redis 变量外，还需要 `TEST_S3_ENDPOINT`、`TEST_S3_ACCESS_KEY`、`TEST_S3_SECRET_KEY`；必须指向独占、可销毁环境。对象存储无凭证不能启动测试服务。

官方契约：[企业微信临时素材](https://developer.work.weixin.qq.com/document/path/90254)、[微信客服消息](https://developer.work.weixin.qq.com/document/path/94670)、[AWS PutObject](https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/put_object.html)。媒体有效期和平台附件上限仍由官方 API 决定，应用提高下载限额不代表平台一定传递更大文件。
