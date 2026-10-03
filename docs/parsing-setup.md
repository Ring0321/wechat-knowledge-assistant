# M4 解析配置与边界

先完成 [微信配置](wecom-setup.md) 和 [M3 收录配置](ingestion-setup.md)。M4 复用原任务队列、租户连接和对象存储；不改 callback，不接入 OpenAI。API、微信 worker 和 ingestion worker 都不需要迁移管理员凭证。

## 启用

在自己的配置中设置 `PARSING_ENABLED=true`，保留 `INGESTION_ENABLED=true`，然后执行：

```sh
docker compose --env-file <配置路径> --profile ingestion up --build -d
```

Compose 为 ingestion worker 提供 `ws://browser:3000/`。browser 服务只在 internal 网络监听，不发布宿主机端口，不挂载文件和业务凭证。不要加入 default/host 网络、添加 expose_network 或将浏览器端口公开。容器镜像安装 Tesseract 中文简体和英文语言包、匹配 Playwright 版本的 Chromium；首次构建需访问依赖下载站。

本地只跑单元测试无需安装 Chromium/Tesseract。要执行实际解析，请使用上述容器；手工本地 worker 需要自行安装 Tesseract/语言包并提供同等隔离的浏览器 endpoint，不能直接用有个人登录信息的浏览器。

## 能力

| 输入 | 当前输出 | 未覆盖部分 |
| --- | --- | --- |
| PDF | 正文、页码；空文本页 OCR | 布局完整复刻、复杂表格结构 |
| DOCX | 标题、段落、表格、位置 | 页眉页脚、内嵌图片、嵌入对象 |
| XLSX | 工作表/单元格、缓存公式值 | 公式计算、格式化日期、图表 |
| PPTX | 幻灯片、表格、讲者备注 | 内嵌图片语义 |
| PNG/JPEG/WebP/TIFF/BMP | 单帧 OCR 文本、尺寸 | 多帧图片、无文字画面语义 |
| 网页/公众号 | 正文、作者、标题、发布时间、图片 URL | 登录、验证码或其他访问绕过 |

文件正文成功时先保存为 `stored`；开启知识索引且远端索引完成后进入 `ready`。不可解析时原件加 `metadata_only` 和 `parse_status/parse_error`。网页获取/渲染失败（含浏览器请求或字节超限）保留 source URL 和 failed job；即使 HTTP 阶段已经获得中间 HTML，也不会将该中间页面保存为成功结果。成功取得但正文解码失败的页面可保留原件和解析原因。源码/渲染 HTML 保存在私有 S3，不能当作可信网页公开执行。

`created_at` 是收录时间，`publish_time` 是页面提供的原始字符串；没有可靠发布时间时留空。OCR 是机器识别，可能出现错字；表格公式使用文件已有缓存，可能过期。后续问答必须依据这些标记保留不确定性。

原件、Canonical JSON、Markdown 仍属同用户。segments 保留 page/sheet/cell/slide/paragraph 等定位。重新发送原先只保存 metadata 的同一文件，若能提取正文则复用原 source_id，保留原资产、原收录日期和历史消息关系；不自动处理历史批次。

## 限额与恢复

可调配置在 `.env.example`：默认解析45秒、100页、50000单元格、500000字符。总任务默认90秒，租约120秒；增加解析预算时一并增加任务和租约预算，配置会校验。HTML 2 MiB、浏览器20秒/40次请求/8 MiB资源及ZIP/像素上限是当前代码安全边界。

解析损坏、加密、旧Office或资源超限为可解释的 metadata_only，不静默截取。网页连接/浏览器临时失败沿用有限退避和人工重试；详情见[收录恢复](ingestion-setup.md)。未启用知识索引时成功通知为“已保存，尚未建立检索索引”；启用后有正文且索引完成才回复“已收录”，供 Agent 检索。音视频有独立开关和[配置说明](media-setup.md)，完整功能组合见[部署说明](deployment.md)。

完整验证使用 `uv run python scripts/verify.py --official-mirror`，涵盖真实OCR、真实隔离Chromium动态网页/私网攻击、PostgreSQL RLS/Redis/S3存储和旧来源升级；无真实客户素材或企业凭证。
