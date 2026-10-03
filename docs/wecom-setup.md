# 微信客服接入与恢复

## 范围

提供 `/wecom/callback` GET/POST、消息同步、文本/媒体元数据保存和渠道确认回复。后续M3—M8已实现媒体下载、文件解析、知识索引、AI问答及视频号后补；本页聚焦渠道设置和恢复，首次微信使用见[接入指南](wechat-launch.md)，完整部署见[部署说明](deployment.md)。全部企业微信 HTTP 请求由 `HttpWeComAPI` / `RedisTokenProvider` 封装；私有setup工具复用同一adapter，固定访问 `https://qyapi.weixin.qq.com`，不跟随重定向、不使用环境代理。

## 配置与启动

使用 `.env.example` 中的数据库/Redis 密码和以下变量，由部署环境安全注入，不提交配置或真实身份：

| 变量 | 含义 |
| --- | --- |
| WECOM_ENABLED | 默认 false；启用 API callback 与 worker 时设 true |
| WECOM_CORP_ID | 企业 ID；必须与加密消息尾部和 XML 身份一致 |
| WECOM_SECRET | 已获微信客服权限的企业应用 Secret；不是假定任意客服 Secret 都有效 |
| WECOM_CALLBACK_TOKEN | 后台 callback Token |
| WECOM_ENCODING_AES_KEY | 后台 43 字符 EncodingAESKey |
| WECOM_OPEN_KFIDS | JSON 数组，显式列出允许同步的客服账号 |
| WECOM_ALLOWED_USER_IDS | JSON 数组，官方 external_userid；空数组拒绝所有用户 |
| CONNECTOR_DB_PASSWORD | 新受限数据库角色 pkb_connector 的密码 |
| CONNECTOR_DATABASE_URL | 本地运行 worker 使用；Compose 从上述密码生成 |
| WECOM_AUTO_REPLY | 默认 true；false 只保存已允许用户的消息，不创建回复 |

已有 M1 数据卷不能靠重启自动运行初始化脚本。先使 postgres 容器取得新增 CONNECTOR_DB_PASSWORD 并保持原 POSTGRES/APP 密码，然后由管理员执行以下幂等角色初始化；脚本仅创建缺失角色，不轮换已有密码、不删除数据：

```sh
docker compose --env-file <配置文件> up -d postgres redis
docker compose --env-file <配置文件> exec -T postgres sh /docker-entrypoint-initdb.d/10-app-role.sh
docker compose --env-file <配置文件> run --build --rm migrate
docker compose --env-file <配置文件> --profile wecom up --build -d api worker
```

新环境可直接执行最后一条，Compose 会先初始化角色和迁移。当前迁移 head 为 `0005_agent`，不要修改已应用的历史迁移。上述命令只启动渠道，完整助手还需ingestion和agent profiles。API 不接触 connector/admin DSN；worker 使用 pkb_app 与 pkb_connector，管理员仅用于 migration/init。现有用户如有人工导入的非稳定 UUID，需先建立审查过的身份迁移，不能靠入口覆盖身份。

在企业后台配置 `https://<你的域名>/wecom/callback`，由 HTTPS 反向代理转到内部 API。代理应关闭或清洗该路径的 query 日志，限制请求体和速率，避免泄露 echostr/签名/通知。公网 TLS、企业可调用权限和可信出口 IP 由部署方配置；本地测试不能代替真实回调域名验证。不要在聊天中粘贴密钥。

## 回复与故障语义

- GET 验证返回解密后的原始 bytes，没有换行。POST 只校验和持久入 Redis，默认最多 3 秒；不会同步调用官方 API、下载文件或模型。
- 未启用收录/问答时，默认回复说明“渠道已就绪、整理和问答尚未启用”，不表示资料已收录。启用后由真实任务状态生成收录或问答回复。媒体只使用实际返回的 media_id，不编造文件名、大小或视频 URL。
- 正常页全部消息提交后更新客服 cursor；任何本页失败保留旧 cursor。消息与回复 outbox 一起提交，重拉不会重复创建回复。
- Redis 使用 AOF everysec 和同连接 WAITAOF；不能禁用 AOF。503 表示没有确认成功，官方不保证对所有 503 重试，worker 每 60 秒无通知补拉。官方只保留最近 3 天消息，停机/积压须在这个窗口内处理。
- `sent` 表示官方接受，不是最终送达；发送失败事件会更新 outbox。`uncertain` 表示超时或重复 ID 等不能确认的结果，不自动换新 ID 重发。
- 48 小时、5 条本地检查只是预防，官方仍裁决接待状态/窗口/配额。`deferred` 不无限自动重试；新入站消息仍可产生新的确认回复，旧 deferred 需运维判断后重试。
- 同步状态 `failed` 及 outbox `failed/deferred/uncertain` 保留安全 error_message；瞬时错误有限退避。日志只有安全事件标识，诊断状态通过受限运维数据库访问，未开放管理 HTTP API。

## 人工恢复

先排除权限/IP/凭证/客服状态或数据错误，再按确定的账号/记录重试：

```sh
docker compose --env-file <配置文件> --profile wecom exec worker python -m app.workers.wecom --retry-account <open_kfid>
docker compose --env-file <配置文件> --profile wecom exec worker python -m app.workers.wecom --retry-outbox <outbox-uuid>
```

重试保留 cursor 和原回复 msgid，不会生成新 ID 来绕过去重。结果不确定时应先核查微信侧状态；95033 本身不是送达证据。超出三天拉取窗口的数据无法保证由官方补回。内部异常 Stream 条目保留 pending 供排查，不直接清空整个队列；私有状态CLI、监控与备份步骤见[部署运维](deployment.md)。当前没有公网死信管理界面。

## 验证

`uv run python scripts/verify.py --official-mirror` 使用独立可销毁 PostgreSQL/Redis 和模拟官方 HTTP，覆盖回调→队列→同步→存储→回复、RLS、去重、分页、错误与恢复。全套过程中没有真实企业微信发送。手工集成测试须用独占 Redis，因为落盘回归测试会暂时改变 fsync 策略并在 finally 恢复。

实际部署验收另需：官方 URL 验证成功、allowlist 内微信用户的文本和附件 metadata 入库、非 allowlist 用户不入库不回复、回复可在客户端观察、进程重启后 cursor/去重保持。尚未提供真实企业配置时，这些项目保持未验证。
