# 微信个人知识库 AI 助手

以企业微信“微信客服”官方 API 服务普通微信用户，把资料转为用户独立知识库，并以可追溯来源回答问题。

**M1—M9 已完成本地工程验收：首次微信接入工具、部署预检、只读运维状态、HTTPS 入口和完整业务链路测试通过。** 真实企业权限、凭证、服务器和公网 HTTPS 尚未配置验收，微信客户端送达及真实模型效果也未验证；项目尚未上线。

2026-10-02 本地验收：1573 项 Windows 单元、887 项 Linux 单元、233 项真实服务集成通过。Windows 的 4 项权限跳过已在 Linux 补齐；两平台用例重叠，不直接累加。微信和 OpenAI 使用模拟 HTTP；复现命令、覆盖范围和验证限制见[测试说明](docs/testing.md)。

## 在微信中怎样使用

管理员配置一个专用的官方微信客服账号，向用户提供官方客服链接或二维码。普通微信用户打开会话，获准后发送“保存：……”或附件，等待收录结果，再直接提问。系统通过同一个客服会话返回带来源的回答。这个入口使用微信客服，既不登录用户的个人微信，也不要求先开发小程序。

可以在客服会话中发送文字或粘贴公众号文章链接。**微信的公众号或视频号“转发”菜单能否直接选到该客服账号，尚未经过真机验证。** 后端支持官方接口传入的卡片，不表示分享菜单一定提供这个入口。视频号卡片默认只保存官方可见信息并标记 `metadata_only`，不代表已读取视频正文。

上线需要有权限的企业微信/微信客服组织、服务器与域名 HTTPS、私有 S3 和 OpenAI 配置，以及管理员确认的首批用户 allowlist。按 [首次开通与扫码使用](docs/wechat-launch.md) 完成账号发现、客服链接获取、一次性识别码匹配和名单确认，再按 [完整 Docker 部署](docs/deployment.md) 开启全部功能。工具不会自动放行用户；不要在聊天、日志或仓库中提供密钥或私有身份文件。

## 当前可运行能力

- FastAPI 存活/就绪探针、应用资源生命周期、JSON 日志与随机 request_id。
- PostgreSQL 六张核心表、Alembic 迁移、用户行级安全策略和同用户去重约束。
- 官方 callback 校验/解密、Redis 持久入队、独立 Worker 分页拉取与定时补拉。
- Redis access_token 缓存与刷新锁，文本和媒体元数据归一化、持久消息去重、事务回复 outbox。
- allowlist、用户 RLS 和独立企业协调角色；发送窗口/配额、退避、人工重试与结果不确定状态。
- CanonicalDocument 统一数据契约、来源/任务状态枚举、默认拒绝的 allowlist 配置。
- 官方媒体限量下载、私有 S3 adapter、Canonical JSON/Markdown、同用户内容去重与多链接来源关联。
- 事务任务分发、Redis ingestion worker、租约与崩溃恢复、失败保留和有限重试；明确区分已保存和已索引。
- PDF 正文/扫描 OCR、DOCX 段落表格、XLSX 单元格、PPTX 幻灯片备注、图片 OCR；保留原件和来源定位。
- httpx 安全网页抓取、隔离 Playwright 回退、公众号字段提取；公网 IP 固定连接、重定向检查、解析资源上限。
- M5 可选音频分片转录、视频场景/定时关键帧、带时间轴的 Markdown 和租户独立媒体资产；模型调用使用adapter。
- M6 可选用户独立 Vector Store、异步 Markdown 索引、带来源与时间定位的检索、历史回填和可重试删除。
- M7 可选 Responses 问答、来源列表/日期检索、逐字摘录与后端引用、无依据明确回复；删除和加标签须用户确认具体操作。
- M8 视频号独立 metadata parser，以及用户明确选择来源后补充原视频的异步流程；不猜测下载地址。
- M9 官方账号/客服链接/首次用户识别 CLI，沿用官方 adapter；一次性识别码不进入知识库，allowlist 由管理员明确配置。
- M9 离线角色与完整 Compose 配置预检、只读同步/回复/任务状态 CLI、严格字段白名单日志和 worker 停止处理。
- M9 可选 HTTPS callback 代理、受限公网路径、部署/监控/备份恢复说明；新增完整收录到问答链路、并发回调、TLS 和逻辑恢复门禁。
- Python 3.12、锁定依赖、ruff、mypy、单元/真实服务集成测试与 CI。

## 基础服务快速开始（Docker Compose）

需要 Docker Engine / Docker Desktop（Linux containers）和 Compose v2。

1. 以 `.env.example` 为模板，在自己的环境或未纳入版本控制的本地配置中设置密码。仓库不会提供 `.env`。不要使用模板占位密码部署。
2. 启动：`docker compose --env-file <你的配置文件> up --build -d`。
3. 访问 `http://127.0.0.1:8000/health/live` 应返回 `{"status":"ok"}`；`/health/ready` 应返回 `ready`，database/redis 均为 `up`。
4. 查看状态：`docker compose --env-file <你的配置文件> ps`；查看日志：`docker compose --env-file <你的配置文件> logs api`。
5. 停止：`docker compose --env-file <你的配置文件> down`。不加 `--volumes` 时保留数据库和 Redis 数据。

Compose 先等待数据库/Redis 健康，然后由一次性 migrate 服务迁移，再启动 API。仅 API 端口绑定 127.0.0.1；数据库、Redis 不映射宿主机端口。该步骤启动基础服务，微信入口默认关闭。首次接入按 [微信开通与名单引导](docs/wechat-launch.md)，已有账号的通道参数与恢复操作见 [微信客服配置与恢复](docs/wecom-setup.md)。

启用 M3 须先配置私有对象存储，再使用 `--profile ingestion` 启动两个 worker，详见 [收录配置与恢复](docs/ingestion-setup.md)。

启用 M4 另设置 `PARSING_ENABLED=true` 并构建同一 profile；独立浏览器容器和本地 OCR 随镜像提供。默认关闭，保留 M3 行为；能力和限制见 [解析配置与安全边界](docs/parsing-setup.md)。

M5 音视频解析单独通过 `MEDIA_PARSING_ENABLED` 启用，需要部署方OpenAI凭证、明确的视觉模型和更长的任务预算；配置及时间轴/计费/失败边界见[音视频解析配置](docs/media-setup.md)。

M6 知识检索通过 `KNOWLEDGE_ENABLED=true` 启用；迁移新增索引记录并沿用非 owner 业务连接。只在远端索引完成后回复“已收录”；配置、回填、删除及不确定结果恢复见[知识库配置](docs/knowledge-setup.md)。

M7 另设置 `AGENT_ENABLED=true` 与明确的 `OPENAI_AGENT_MODEL`，同时启动 `--profile ingestion --profile agent`。模型通过受控工具检索，回答由已保存原文及后端来源字段组成；配置、确认码、任务重试和当前能力边界见[微信问答配置](docs/agent-setup.md)。

M8 卡片解析随收录启用，补充原视频复用媒体配置。微信发送 `补充视频 <来源编号>`，收到提示后在同一会话发送普通视频。原卡片编号及原件保留，操作与恢复说明见[视频号资料配置](docs/channels-setup.md)。

M9 全功能部署使用 `compose.yml` 与 `compose.production.yml`，同时启用 `ingestion`、`agent`、`production` 三个 profile。部署前运行 `uv run python scripts/deployment_check.py --env-file <仓库外私有配置文件>`；它只输出固定检查结果，不展示渲染后的密钥。代理默认为本机 8443，公网绑定、正式证书、出口策略和告警由部署方按 [部署文档](docs/deployment.md) 明确配置。

镜像内 `python -m app.operations.preflight --role <角色>` 检查对应角色的完整功能配置，角色取 `api`、`wecom`、`ingestion` 或 `agent`；微信 worker 内 `python -m app.operations.status` 查看受限的同步、回复和调度状态。配置通过、探针正常和状态读取成功都不能代替微信客户端实际收发验收。

首次初始化由 PostgreSQL init 脚本创建非超级用户 `pkb_app` 和 `pkb_connector`，迁移管理员与运行角色隔离。旧 M1 数据卷须先补建 connector 角色再迁移，步骤见配置文档。更改密码环境变量不会自动修改已有数据库卷的角色密码；已有环境须由管理员显式轮换，不要删除数据卷解决。

## 本地开发

需要 [uv](https://docs.astral.sh/uv/)、Python 3.12，以及 PATH 中的 ffmpeg/ffprobe（本机媒体测试需要；容器镜像内安装）：

```sh
uv sync --locked
uv run ruff check .
uv run ruff format --check .
uv run mypy app
uv run pytest tests/unit
```

开发边界和提交要求见[贡献指南](CONTRIBUTING.md)。以上命令可直接在本地或 CI 执行。

本地 API 需要设置 DATABASE_URL（非 owner 的 `postgresql+asyncpg`）、REDIS_URL。应用不自动读取 `.env`。准备好数据库后执行 `uv run uvicorn app.main:app --host 127.0.0.1 --port 8000`。

迁移单独使用 `DATABASE_ADMIN_URL`，执行 `uv run alembic upgrade head`；运行 API 不需要、也不应接触此变量。新迁移：`uv run alembic revision --autogenerate -m description`，必须人工审查后测试。ORM 的 onupdate 负责应用层 updated_at，直接 SQL 写入须显式维护该字段。

## 完整 M1—M9 验证

```sh
uv run python scripts/verify.py
```

该命令先运行格式、静态检查和单元测试，并校验全部 Compose profile；再以随机独立项目启动真实 PostgreSQL/Redis/S3 兼容服务/API/隔离 Chromium，构建测试镜像，运行 Linux adapter/接入工具测试、真实 OCR、浏览器、存储、合成音视频集成和探针 smoke。M9 门禁还覆盖收录、索引、微信问答和租户隔离的串联路径、后台处理期间的并发 callback、仅 callback 的 HTTPS 入口及日志保护、PostgreSQL 逻辑备份恢复后的 SHA/RLS，以及 Redis 停机恢复。完整生产配置另用合成值渲染检查，不连接真实云服务。

结束仅清理该测试项目及测试卷。无需真实 API Key，企业微信和 OpenAI 采用 HTTP mock；不创建 `.env`，临时凭证通过进程环境注入。日志保存在被忽略的 `test-results/`。Docker 不可用或清理失败时命令失败；不能将跳过的测试算作通过。范围和结果见[测试说明](docs/testing.md)；本地门禁不替代真实微信送达、外部 S3 灾难恢复或真实模型效果验收。

若本地无法连接 Docker Hub，可运行 `uv run python scripts/verify.py --official-mirror`，使用 AWS ECR Public 上的 [Docker Official Images](https://aws.amazon.com/blogs/containers/docker-official-images-now-available-on-amazon-elastic-container-registry-public/)。日常 Compose 使用相同镜像时设置 `.env.example` 注释中的相应 IMAGE 变量即可；无需更改 Docker 全局设置。

只跑测试：单元测试用上述命令；手工集成测试需设置 TEST_DATABASE_ADMIN_URL、TEST_DATABASE_URL、TEST_CONNECTOR_DATABASE_URL、TEST_REDIS_URL，数据库名称必须以 `_test` 结尾，并有两个受限角色。测试会写入记录、创建随机 `_test` 数据库验证迁移往返，以及临时更改 Redis fsync 配置验证落盘屏障；**数据库及 Redis 都必须是独占的可销毁测试实例，不能指向业务服务**。推荐使用完整脚本自动建立隔离环境，TEST_API_URL 也由脚本设置。

## 文件导航

- [架构设计](architecture.md)：全流程、模块职责、隔离、故障恢复和 M1—M9 实现边界。
- [数据库 ER 模型](docs/database-er.md)：字段、关系、约束与 RLS。
- [贡献指南](CONTRIBUTING.md)：开发边界和验收要求。
- [测试说明](docs/testing.md)、[目录导航](docs/file-inventory.md)：复现验证与查找实现。
- [首次微信接入](docs/wechat-launch.md)、[部署与运维](docs/deployment.md)：从管理员准备到微信实机验收。
- `app/connectors/wecom/`：官方通道与首次接入 CLI；`app/operations/`：角色预检和只读状态。
- `app/ingestion/`、`app/parsers/`、`app/knowledge/`、`app/agent/`：收录、解析、索引与问答。
- `migrations/`：数据库迁移；`infrastructure/`：角色初始化与 NGINX callback 代理配置。
- `compose.production.yml`、`scripts/deployment_check.py`：完整生产配置与离线部署检查。
- `tests/`、`scripts/verify.py`、`scripts/proxy_smoke.py`、`scripts/restore_smoke.py`、`.github/workflows/ci.yml`：自动验证与恢复演练。

## 里程碑路线

| 阶段 | 交付与阶段门槛 |
| --- | --- |
| M1 | 骨架、Docker/PostgreSQL/Redis、数据模型和测试 |
| M2 | 官方 callback、快速持久入 Redis、worker sync_msg、去重、token adapter、媒体元数据、文本回复和 allowlist；媒体下载留给 M3 |
| M3 | 下载、CanonicalDocument 流水线、S3、幂等任务/重试/恢复 |
| M4 | PDF/DOCX/图片/网页/公众号解析；补齐 XLSX/PPTX，未知类型保原件；SSRF 对抗测试 |
| M5 | 音频转录、视频音轨/场景和定时关键帧/时间轴；资源上限测试 |
| M6 | OpenAI Vector Store + File Search、KnowledgeService、用户隔离及索引/删除一致性 |
| M7 | Responses Agent、受控知识工具、微信问答和可验证引文、无依据拒答 |
| M8 | 视频号独立 metadata parser、metadata_only、后补原件 |
| M9 | 首次微信接入、部署/角色预检、只读状态、安全日志、串联/异常/受限并发/恢复测试、HTTPS 和 Docker 运维文档；本地工程验收通过 |

九个阶段的本地工程验收已完成；下一步按部署方提供的环境执行真实公网部署及凭证联调，完成微信手机验收。关闭知识索引时有正文资料回复“已保存”并说明尚未建立检索索引；metadata_only 卡片回复明确“仅卡片信息”。未启用 Agent 时普通问题说明问答未启用。

## 关键使用边界

消息入口、发送次数和时间窗口受微信客服 API 约束；附件不保证任意体积都能被平台转交。视频号卡片仅保存官方可见 metadata，正文保持为空；后补视频由用户主动提供并明确关联，系统不验证其与卡片一定相同。引用信息由数据库和检索证据组成，不允许模型编造来源。具体官方约束及链接见架构文档。

## 许可证

本项目以 [MIT License](LICENSE) 开源。第三方服务、模型、平台接口和依赖仍适用各自的条款；部署者需要自行取得相关服务权限。
