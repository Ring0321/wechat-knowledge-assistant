# M5 音频与普通视频解析

复用[收录配置](ingestion-setup.md)的微信身份、后台任务、受限数据库角色及私有S3。微信callback继续只校验和可靠入队，模型调用在ingestion-worker完成。视频号卡片仍按metadata保存，不尝试下载隐藏视频。

## 启用

在部署方自己的环境配置中设置：

```text
MEDIA_PARSING_ENABLED=true
INGESTION_ENABLED=true
OPENAI_API_KEY=<部署方自己的项目密钥>
OPENAI_VISION_MODEL=<该项目可用的支持图像输入的Responses模型>
INGESTION_JOB_TIMEOUT_SECONDS=300
INGESTION_LEASE_SECONDS=330
```

上述内容仅是说明，不要将真实配置提交到仓库。转录固定使用whisper-1，以获得segment起止时间。视觉模型必须显式指定；应用不会自动选择、升级或探测其他模型。媒体处理仅由ingestion-worker使用OpenAI凭证；开启问答时agent-worker也需要自己的角色配置。API、微信worker和browser不接收该凭证。

执行 `docker compose --env-file <你的配置文件> --profile ingestion up --build -d`。镜像安装ffmpeg/ffprobe，不需要在宿主机额外安装。M4文档/网页解析开关 `PARSING_ENABLED` 可单独启用；M5不依赖浏览器来处理媒体，但当前ingestion profile仍会启动隔离browser服务。

## 处理和输出

- 语音或音频：检查真实媒体流，解码为16kHz单声道PCM WAV，默认每片300秒；转录逐片调用官方Audio Transcription接口，把片内时间加回片段offset。
- 普通视频：提取音轨；同时通过场景变化和定时位置选关键帧，时间去重后限制总帧数，再逐帧进行图像理解。无音轨视频仍处理画面。
- CanonicalDocument与Markdown保存时间轴，显示如 `[00:35–00:52]（语音机器转录）`；关键帧是一个采样时刻，标记“画面描述·模型推断”，不把它当作连续视频内每一秒的事实。
- 原件、音频分片、关键帧、转录JSON、Canonical JSON和Markdown均保存在同用户私有对象空间。相同帧/音频字节可复用同来源对象，metadata保留各自时间位置。
- 已完成的同用户相同原件直接关联既有来源，避免再次调用模型；不同用户完全独立。旧metadata_only资料重新发送后可复用原source_id补齐正文，保留原件、原收录时间和旧资产。静音结果也保存派生资产和 no_speech_detected 状态，重复发送可复用；通知明确“音频已处理，未识别到语音”。

识别结果可能有错字或模型误判；空语音不编造正文。不提供实时语音对话、说话人身份识别、视频字幕布局复刻或完整逐帧理解。分片边界可能影响上下文。未启用知识索引时回复“已保存，尚未建立检索索引”；开启[知识索引](knowledge-setup.md)并索引成功后可由[问答 Agent](agent-setup.md)检索，引用可追溯的时间段。

## 限额和故障

默认最多20MiB原件、15分钟时长、12帧、30秒定时间隔；场景检测阈值0.3，可通过 `.env.example` 中的MEDIA参数调整。采样位置会在总帧数限制内取舍，长视频不会每秒抽帧。当前单帧宽度上限960像素、输入最多约830万像素、派生输出最多80MiB；协议/封装格式采用白名单，播放列表和网络嵌套引用拒绝。

媒体总预算默认240秒，单个子进程默认60秒；整个任务至少留出30秒下载/保存余量，租约至少比任务预算长15秒。ingestion-worker配置2GiB内存、2CPU、128PID、256MiB临时空间；子进程不使用shell，不继承业务凭证，超时/取消清理进程组。这些限额不是完整操作系统权限沙箱；生产需要独立的受控处理节点和网络出口限制。

损坏、类型不支持或资源超限的媒体保留原件并标记metadata_only及固定错误码。OpenAI鉴权、限流、服务故障、响应畸形和转录时间戳缺失会使job失败，不会把部分结果标成成功；瞬时故障沿用有限退避和[人工重试入口](ingestion-setup.md)。配置错误须修复后重试。凭证、正文、图片、原始异常和完整模型请求不写日志。

重试或并发处理相同尚未完成的资料可能再次调用模型，**不保证外部API只计费一次**。中途失败尚未提交的派生结果不会当作成功来源，第一次失败的原件也可能尚未写入S3；重试重新下载。微信临时media存在有效期，应及时处理故障。S3写入与数据库事务无法原子提交，回滚可能留下孤立对象，沿用M3审计/保留期约定。

## 验证范围与官方契约

完整命令：`uv run python scripts/verify.py --official-mirror`。脚本使用真实ffmpeg合成媒体、PostgreSQL受限角色、Redis、S3和Chromium回归。OpenAI通过HTTP MockTransport验证请求/响应和错误路径，不调用真实付费接口。这不证明部署账号的模型权限、真实中文转录质量或视频语义准确率。整体结果与复现条件见[测试说明](testing.md)。

[OpenAI转录文档](https://developers.openai.com/api/docs/guides/speech-to-text)说明25MB单次输入上限及whisper-1时间戳参数；[图像输入文档](https://developers.openai.com/api/docs/guides/images-vision)说明Responses的input_image/base64输入。实现设置store=false、不提供工具，返回文本仅作内容数据，不接受模型指定用户或来源身份。
