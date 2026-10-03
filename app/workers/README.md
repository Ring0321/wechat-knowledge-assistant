# 后台任务

本目录负责配置、受限资源装配、心跳、停止与运维重试 CLI；具体业务保持在各自模块。

| 入口 | 业务职责 | 文档 |
| --- | --- | --- |
| `python -m app.workers.wecom` | Redis 通知分页同步，PostgreSQL 游标与回复 outbox，定时补拉和有限退避 | [微信配置](../../docs/wecom-setup.md) |
| `python -m app.workers.ingestion` | 资料下载、解析、存储、知识索引与删除任务，持久租约和恢复 | [收录配置](../../docs/ingestion-setup.md) |
| `python -m app.workers.agent` | 独立问题任务、受控工具循环、原文引用与确认操作 | [问答配置](../../docs/agent-setup.md) |

每个角色只接收必要凭证；业务数据库连接使用非 owner 角色。完整启动组合、停止宽限期、状态检查与恢复见[部署说明](../../docs/deployment.md)。
