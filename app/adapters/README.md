# 外部适配器边界

业务层依赖领域 Protocol，所有第三方请求及进程调用在适配器中封装。

| 文件 | 职责 |
| --- | --- |
| `s3.py` | 通过 boto3 上传私有原件和派生文件，实现 ObjectStore 契约 |
| `web_http.py`、`browser.py` | 固定经校验的公网 IP、限制重定向及资源、协调隔离 Playwright |
| `ffmpeg.py`、`windows_process.py` | 有界媒体进程、音轨提取、场景与定时关键帧、平台进程约束 |
| `openai_media.py` | Audio Transcription 与 Responses 图像理解 |
| `openai_vector.py` | Files、Vector Store 索引、检索和删除 |
| `openai_responses.py` | 有界 Responses 问答请求和受控工具协议 |

企业微信 API、token、media 和首次接入适配器统一位于 `app/connectors/wecom/`；PostgreSQL/Redis 健康适配器在 `app/core/health.py`。凭证只注入需要该服务的运行角色。配置及安全边界见[架构](../../architecture.md)与[部署说明](../../docs/deployment.md)。
