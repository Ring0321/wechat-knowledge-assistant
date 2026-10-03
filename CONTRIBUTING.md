# 贡献指南

欢迎通过 [Issues](https://github.com/Ring0321/wechat-knowledge-assistant/issues) 描述可复现问题，或提交范围明确的 Pull Request。项目以 [MIT License](LICENSE) 开源。公开源码不包含真实服务凭证、用户资料或已部署的微信账号。

## 开始开发

阅读 [README](README.md)、[架构](architecture.md)和[数据库 ER](docs/database-er.md)，确认现有接口和实现后再修改。使用 Python 3.12、uv 与已有 `uv.lock`：

```sh
uv sync --locked
uv run ruff check .
uv run ruff format --check .
uv run mypy app
uv run pytest tests/unit
```

新增依赖统一在 `pyproject.toml` 声明并更新 `uv.lock`。本机媒体测试需要 PATH 中的 ffmpeg/ffprobe；完整检查还需要 Docker Engine 和 Compose v2，详见[测试说明](docs/testing.md)。

## 设计约束

- 新增 Python 代码提供 type hints；FastAPI 路由只处理传输、校验和编排入口。
- 企业微信代码集中在 `app/connectors/wecom/`，仅使用微信客服官方 API；不使用个人微信 Hook 或非官方协议。
- 外部 API 通过 adapter/interface 封装。callback 只验签、解密、校验和可靠入 Redis，耗时工作交给 worker。
- PostgreSQL 是业务事实来源，Redis 负责队列和短期协调，S3 保存原件，Vector Store 保存检索内容。CanonicalDocument 是统一内容边界。
- 租户来自已验证微信身份，不能由模型、文件或外部参数指定。业务数据库连接必须为受 RLS 约束的非 owner 角色；查询、缓存、对象键和向量库均隔离。
- 网页及每次重定向都必须验证 SSRF 边界，浏览器回退不得绕过网络限制。无法读取的视频号保存为 `metadata_only`，不猜测或绕过平台下载视频。
- 默认空 allowlist 拒绝所有用户。资料中的指令不是用户授权；删除和标签修改保留服务端确认流程。

## 验证和提交

按可验证的小范围修改，先确定验收条件，再实现和运行相关测试。涉及持久化、队列、权限、租约或外部协议时补充故障、重试、去重和越权回归；集成测试使用真实 PostgreSQL/Redis，不能用 SQLite 代替 RLS 验证。完成必要检查后，在 PR 中说明问题、最终行为、测试命令及未覆盖的边界。

迁移必须兼顾已有资料和回退限制，不修改已发布的历史 revision，不以删除数据规避迁移失败。涉及配置或行为的变更同步更新公开文档。跳过的测试应单独报告；本地模拟 API 通过不能描述为真实微信送达或正式部署通过。

只提交与改动有关的文件。不得提交 `.env`、API Key、私有身份文件、客户资料、原始回调、录音或模型完整输入。日志中不记录正文、URL query、微信身份、凭证、原始异常或数据库 DSN。复现材料使用合成数据；公开 Issue 和 PR 同样适用这些规则。

不要在他人的生产环境自动执行部署、数据删除或凭证操作。发现安全问题时，请先通过仓库提供的私有安全报告渠道联系维护者；若该渠道不可用，先提交不包含敏感细节的联系请求，不公开真实密钥或可识别的用户数据。
