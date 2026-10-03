# 目录导航

此导航对应已实现的 M1—M9，不包含虚拟环境、缓存、运行日志和生成产物。各业务目录均包含实现与测试；详细能力边界见[架构](../architecture.md)。

| 区域 | 入口及职责 |
| --- | --- |
| 项目文档 | [README](../README.md)、[贡献指南](../CONTRIBUTING.md)、[架构](../architecture.md)、[数据库 ER](database-er.md)、[测试说明](testing.md) |
| 配置与依赖 | `pyproject.toml`、`uv.lock`、`.python-version`、`.env.example`；默认关闭外部业务功能 |
| 容器与 CI | `Dockerfile`、`compose.yml`、`compose.test.yml`、`compose.production.yml`、`.github/workflows/ci.yml` |
| 基础设施 | `infrastructure/postgres/init-app-role.sh` 创建受限运行角色；`infrastructure/nginx/nginx.conf` 提供 callback HTTPS 入口 |
| 应用入口与基础能力 | `app/main.py`、`app/api/`、`app/core/`：生命周期、配置、探针、结构化日志 |
| 数据契约与持久化 | `app/domain/`、`app/db/`：CanonicalDocument、解析/存储接口、SQLAlchemy 模型和租户事务 |
| 微信官方连接器 | `app/connectors/wecom/`：callback、加解密、API/token/media、归一化、去重、同步/回复、首次接入 CLI |
| 收录 | `app/ingestion/`：来源检测、任务调度、租约、下载到规范化存储的流水线 |
| 解析 | `app/parsers/`：PDF/Office/图片/网页/公众号、音视频时间轴、视频号 metadata |
| 外部适配器 | `app/adapters/`：S3、安全 HTTP/浏览器、ffmpeg、OpenAI 媒体/向量/Responses |
| 知识与问答 | `app/knowledge/`、`app/agent/`：隔离索引/搜索/删除、受控工具、引用、确认修改 |
| 进程与运维 | `app/workers/`、`app/operations/`：三个 worker 入口、角色预检和只读状态 |
| 迁移 | `alembic.ini`、`migrations/`；从 `0001_initial` 到当前 `0005_agent` |
| 自动验证 | `tests/unit/`、`tests/integration/`；`scripts/verify.py` 运行隔离服务的完整检查 |
| 部署与恢复检查 | `scripts/deployment_check.py`、`scripts/proxy_smoke.py`、`scripts/restore_smoke.py` |

开始真实微信接入见[开通指南](wechat-launch.md)，完整 Docker 配置见[部署说明](deployment.md)。运行日志写入本机忽略的 `test-results/`，不随源码发布，也不进入生产镜像。
