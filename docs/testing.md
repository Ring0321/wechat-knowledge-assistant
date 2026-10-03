# 测试与验证边界

项目包含离线单元测试和真实基础服务集成测试。企业微信与 OpenAI 的业务 HTTP 使用合成响应；运行这些测试不需要真实 API Key，也不证明实际微信权限、消息送达、模型效果或公网部署已经可用。

## 本地检查

需要 Python 3.12 和 uv。先安装锁定依赖，再运行：

```sh
uv sync --locked
uv run ruff check .
uv run ruff format --check .
uv run mypy app
uv run pytest tests/unit
```

本机媒体测试需要可执行的 ffmpeg/ffprobe。Windows 可能因当前用户没有符号链接权限而跳过权限相关用例；应将跳过单独记录，并在 Linux 环境补跑，不能把 skipped 算作 passed。

## 完整隔离验证

需要 Docker Engine / Docker Desktop 的 Linux containers、Compose v2，以及下载锁定依赖和镜像的网络：

```sh
uv run python scripts/verify.py
```

如 Docker Hub 连接受限，可使用官方镜像的 ECR Public 来源：

```sh
uv run python scripts/verify.py --official-mirror
```

脚本运行静态检查及单元测试，校验 Compose 配置，然后创建随机命名的一次性 Docker 项目。该项目使用合成凭证及独占测试数据，包含 PostgreSQL、Redis、S3 兼容服务、隔离 Chromium、ffmpeg 和应用进程。完整检查覆盖：

- 数据迁移、非 owner 业务角色、RLS 隔离、事务、连接复用、去重、租约与失败重试。
- 文件/OCR/网页/音视频解析、安全抓取、真实对象存储读写及来源升级。
- 加密 callback → 收录 → 索引 → 问题检索 → 带来源的回复发送请求，及双用户隔离和重放。
- 同步 API 阻塞期间的有界并发 callback；这是回归测试，不是生产容量测试。
- Linux 接入工具、适配器、权限、运维状态和 worker 停止处理。
- HTTPS 证书校验、路径/方法/请求体限制及回调 query 日志保护。
- 实际 `pg_dump` / `pg_restore` 后的内容哈希、迁移与受限角色 RLS，以及 Redis 停机、重启和持久数据恢复。

日志保存到本机忽略的 `test-results/verification.log`。脚本最终清理自身创建的测试项目和测试卷；Docker 不可用或清理失败会使命令失败。不要手工把测试配置改为业务数据库或 Redis；手工集成测试的数据库名称必须以 `_test` 结尾，相关服务必须独占且可销毁。

完整生产配置可另用部署方私有配置离线检查：

```sh
uv run python scripts/deployment_check.py --env-file <仓库外私有配置文件>
```

该命令不应输出渲染后的秘密配置；通过只说明必要字段、服务及角色隔离满足检查，不能证明远端账号权限。

## 已完成的本地验收

2026-10-02 的 M1—M9 本地验收记录如下，供判断当前覆盖范围。后续变更请以对应提交的实际运行和 CI 结果为准。

| 检查 | 结果 |
| --- | --- |
| Ruff 检查、格式、mypy | 通过 |
| Windows Python 3.12 单元 | 1573 passed，4 skipped（符号链接权限） |
| Linux Python 3.12 单元子集 | 887 passed，补齐上述 4 项 Windows 跳过 |
| PostgreSQL/Redis/S3/浏览器/ffmpeg 集成 | 233 passed，无跳过 |
| HTTPS、PostgreSQL 逻辑恢复、Redis 故障恢复 | 通过 |
| 生产镜像运行文件一致性、非 root 用户与私有文件排除 | 通过 |

两个平台的单元用例重叠，不能累加成独立测试总数。完整验证脚本退出码为 0；一次性测试资源已清理。此处仅保留工程验收摘要，原始本机日志不随公开源码提供。

真实企业权限、微信客户端分享菜单和消息送达、OpenAI 账号模型权限及生成质量仍待部署方实测。PostgreSQL 逻辑恢复和 Redis 重启检查不等于外部 S3 灾难恢复、远端向量一致性、生产容量或 RPO/RTO 保证。实机步骤见[微信接入指南](wechat-launch.md)，环境配置与恢复流程见[部署说明](deployment.md)。
