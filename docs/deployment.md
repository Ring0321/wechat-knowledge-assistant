# Docker部署与运维（M9）

## 部署形态与准备

建议首批内测使用一台长期在线的Linux主机：API、微信worker、ingestion-worker、agent-worker、PostgreSQL、Redis和隔离浏览器。生产私有S3单独提供；测试用SeaweedFS不作为生产推荐。没有基准容量数据时不承诺并发用户数量；以媒体worker 2GiB/2CPU、浏览器1GiB/1CPU限制为基础，为数据库、模型等待和其他服务预留资源，再根据真实压测定容。

在部署主机安装Docker/Compose和Python3.12/uv（运行本地预检用）。获取已核验源码及其SHA清单，安装 `uv sync --locked`。完整首次微信路径见[从开通到扫码使用](wechat-launch.md)。密钥通过部署环境或仓库外0600配置文件注入；只维护仓库中的 `.env.example`，不要提交真实配置。配置文件和值不要粘贴到日志或聊天。

所需最低网络：用户到代理443；服务访问微信官方API、OpenAI、私有S3，以及内部数据库/Redis。数据库/Redis/browser均不公开端口。网页fetcher有SSRF固定IP校验，生产还应配置独立出口防火墙，禁止非必要内网和云metadata访问，同时明确放行数据库/Redis/S3所需流量；当前Compose本身不代替这一主机/网络策略。

## 全功能配置

保留 `.env.example` 的全部必填数据库/Redis和企业配置，使用随机长密码。当前迁移head为 `0005_agent`。启用完整助手必须同时设置：

```text
APP_ENV=production
WECOM_ENABLED=true
WECOM_AUTO_REPLY=true
INGESTION_ENABLED=true
PARSING_ENABLED=true
MEDIA_PARSING_ENABLED=true
KNOWLEDGE_ENABLED=true
AGENT_ENABLED=true
INGESTION_JOB_TIMEOUT_SECONDS=300
INGESTION_LEASE_SECONDS=330
PKB_TLS_DIR=/opt/wechat-knowledge-private/tls
PKB_HTTPS_BIND=0.0.0.0
PKB_HTTPS_PORT=443
```

另外填写自己的 `WECOM_CORP_ID`、`WECOM_SECRET`、callback Token/AESKey、JSON数组 `WECOM_OPEN_KFIDS` 和 `WECOM_ALLOWED_USER_IDS`、HTTPS私有S3地址/桶/访问凭证、`OPENAI_API_KEY`、明确的 `OPENAI_AGENT_MODEL` 和 `OPENAI_VISION_MODEL`。模型选择须确认当前项目支持所需工具/结构化输出/图片输入，不使用未经核对的模型名。预检仅验证配置，不能验证远端模型权限或费用。

部署证书目录必须已有 `fullchain.pem`、`privkey.pem`，由受信任CA签发并覆盖域名；容器UID/GID10001需可读，私钥建议640、组10001，目录750。不要使用测试脚本生成的临时证书上线。TLS终止配置见 `infrastructure/nginx/nginx.conf`：TLS1.2/1.3，仅放行精确callback路径，限制64KiB和请求速率；API本身仍监听主机127.0.0.1。

生产代理overlay是可选项；若已有可信HTTPS负载均衡，可按相同路径/日志/大小/超时策略代理到API，但需要单独验收。默认overlay只绑定127.0.0.1:8443，公网部署必须显式设置上面的绑定和端口。关闭了可能含query的代理原始访问/错误日志，以应用安全JSON日志和探针监控请求结果。启动配置错误通过管理员 `nginx -t` 检查。

## 检查与启动

以下示例在Linux部署主机执行；`/opt/wechat-knowledge-private/runtime.conf` 是管理员维护的仓库外配置路径。命令不会创建配置或密钥。

```sh
uv run python scripts/deployment_check.py --env-file /opt/wechat-knowledge-private/runtime.conf
```

预检在内存读取Compose渲染结果，检查完整服务、各角色开关/凭证隔离/allowlist/任务停止宽限期，只输出固定检查名和布尔值。失败时不要运行明文 `docker compose config` 贴出所有密钥。`configuration_valid` 仅表示配置匹配，不表示服务已经可用。

为避免漏启动问答worker，完整命令必须包含三个profile：

```sh
docker compose --env-file /opt/wechat-knowledge-private/runtime.conf -f compose.yml -f compose.production.yml --profile ingestion --profile agent --profile production up --build -d
docker compose --env-file /opt/wechat-knowledge-private/runtime.conf -f compose.yml -f compose.production.yml --profile ingestion --profile agent --profile production ps
docker compose --env-file /opt/wechat-knowledge-private/runtime.conf -f compose.yml -f compose.production.yml exec proxy nginx -t
```

首次启动先初始化受限数据库角色，迁移使用独立管理员，业务请求使用非owner、非BYPASSRLS角色。API和微信worker不持有OpenAI密钥。已有卷不会因环境变量改密码而更新角色密码；使用经过审查的显式密码轮换，不删除卷。旧M1卷的角色补建见[微信接入说明](wecom-setup.md)。

配置更新必须重建对应服务，单纯restart不会加载新的Compose环境。完整助手须统一更新API和三个worker的allowlist。只在私有主机检查 `http://127.0.0.1:8000/health/ready`；公网 `/health`、`/docs` 等路径均拒绝。

每个镜像内还有离线检查：`python -m app.operations.preflight --role api|wecom|ingestion|agent`，在相应容器执行。它要求完整助手配置；首次空名单引导阶段预检失败属于预期，不能因此放开所有用户。

## 监控、日志与故障处理

运行 `python -m app.operations.status --help` 查看私有只读状态命令，在微信worker环境调用。默认只输出当前企业的同步/回复/dispatch计数、积压时间和内部操作编号；指定dispatch可检查任务状态、重试、租约。数据由非owner角色和RLS读取，命令不写业务数据、不暴露用户标识/正文/凭证，也不开放管理HTTP接口。

注意三层含义不同：`/health/live`只证明API存活；`/health/ready`验证数据库角色/迁移和Redis；worker `--health`只证明心跳，不证明S3/OpenAI/微信发送正常。状态CLI读取成功也不等于业务健康，必须结合积压和失败计数。

建议监控系统每分钟采集安全状态，至少对以下信号告警（阈值是内测起点，需真实负载校准）：

- ready失败或任一worker心跳消失，持续2分钟。
- 已配置客服没有成功同步，或最新同步距今超过5分钟；任何failed同步立即排查，不能接近3天补拉窗口才处理。
- 到期未完成的收录/问答dispatch超过10分钟；突增的failed/deferred/uncertain回复。
- 磁盘/对象存储、Redis AOF写入故障、数据库连接耗尽、TLS证书剩余14天、模型额度/费用预算。
- 结构化事件 `agent_worker_failed`、`agent_worker_retry`、`ingestion_worker_retry`、`wecom_sync_failed` 等持续出现。

日志只保留白名单事件和严格类型化字段，原异常文本/URL query/身份/消息不会输出。Compose生产overlay对日志限10MiB×5份；持久监控/告警接收渠道由部署方配置，本项目没有暗中发送通知。不要开启HTTP debug、数据库echo、代理原始请求日志排障。

用CLI内部编号定位后，复用[微信回复重试](wecom-setup.md)、[收录恢复](ingestion-setup.md)、[索引恢复](knowledge-setup.md)、[问答恢复](agent-setup.md)。`uncertain` 先核对远端事实，不盲目换新msgid。删除/索引不确定操作也需既有恢复流程，不能清空队列当修复。

## 备份与恢复

PostgreSQL是业务事实，S3是原件和派生资产；两者都必须备份。Redis保存持久通知、待发送协调和短期状态，也需要恢复方案。OpenAI Vector Store是远端派生数据，但当前映射/不确定操作日志必须随数据库保留；不能直接清空vector字段宣称无损重建。

小规模部署可选择维护窗口的一致性备份：先暂停入口及所有worker，等待当前任务结束，记录镜像digest、迁移head和备份时间，再执行PG逻辑备份和同窗口的S3版本/对象快照，最后停止Redis并备份完整持久卷。备份文件和恢复操作都应在私有目录，独立加密保存到另一故障域并定期验证哈希；密钥备份单独保管。

管理员在数据库容器内执行 `pg_dump -Fc` 生成归档，不在命令参数中写数据库密码。恢复先在新的隔离项目和新数据库中初始化相同角色，再用 `pg_restore --exit-on-error --single-transaction` 恢复，核对迁移、数据量、随机原件SHA和非owner RLS。S3需恢复相同对象键和访问策略，Redis在服务停止时恢复整个匹配卷；不要把旧通知直接灌进仍运行的业务服务。

真实数据恢复后暂不公开入口：先检查不确定发送/索引和正在删除资料，验证租约到期恢复、用户隔离以及资料可检索，再恢复worker和入口。S3对象恢复及远端向量一致性须在部署方自己的服务执行；本地完整脚本仅演练真实PG逻辑归档恢复、数据/SHA/RLS和Redis有序停机恢复，不声称完成异地灾难恢复或零数据丢失。

默认Redis AOF everysec存在持久窗口，callback另做WAITAOF确认；快照间隔决定整体备份RPO。由部署方给出可接受RPO/RTO并用真实备份演练计时，本项目没有未经测量的分钟级恢复承诺。

## 升级与回退

先备份和记录当前镜像digest，停止入口接收新请求并停止worker；Compose给worker650秒宽限期，覆盖支持的最长600秒单任务预算。微信单轮批量同步或阻塞数据库仍可能超时强杀，此时按持久游标、dispatch和租约恢复；外部调用可能已发生，不承诺恰好一次计费。

在隔离环境运行完整门槛，通过后再以固定镜像digest发布。数据库迁移先执行；当前有业务数据时部分迁移拒绝downgrade，因此不能把 `alembic downgrade` 当通用回退。优先回退兼容schema的应用镜像；不兼容时在维护窗口恢复匹配的数据库/S3备份，核对远端副作用后再开放流量。不要使用 `down --volumes` 升级或回退。

证书续期后运行 `nginx -t`，成功再reload代理；确认实际客户端使用新证书。完整工具命令和模型/微信权限应随部署方环境留存私有运维记录。

参考：[NGINX HTTPS](https://nginx.org/en/docs/http/ngx_http_ssl_module.html)、[代理参数](https://nginx.org/en/docs/http/ngx_http_proxy_module.html)、[Compose停止宽限期](https://docs.docker.com/reference/compose-file/services/#stop_grace_period)。
