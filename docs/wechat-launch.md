# 把助手用在微信里

## 第一版长什么样

创建一个专用的微信客服账号，例如“个人知识库助手”。用户扫码或点官方客服链接，在普通微信会话里发送资料和提问；后台使用本项目处理和回复。入口由微信提供，本项目提供服务器能力。无需先开发小程序，也不是添加个人微信好友的机器人。

用户体验：发送“保存：项目例会是每周三下午三点”→“收到，正在整理…”→索引完成后“已收录…”→发送“项目例会是什么时候？”→收到带来源标题、类型和保存时间的回答。视频号卡片只保存可见信息，可按[视频号说明](channels-setup.md)明确后补原视频。

项目尚未完成真实微信部署。在客服会话中发送文字、粘贴公众号链接是预期接入方式；**公众号或视频号的分享菜单能否直接选到该客服账号，尚未经过真机验证**。后端支持官方接口传入的卡片，不代表客户端一定提供直接转发入口，部署验收应单独核对。

## 上线需要准备什么

| 条件 | 部署方需要提供的内容 |
| --- | --- |
| 企业微信与微信客服 | 有管理权限的组织、已开通客服、专用测试客服账号、获接口权限的自建应用 |
| 服务器与网络 | 长期运行Docker的Linux主机、稳定公网入口和调用微信API的出口IP；网络应能访问所选官方云API |
| 域名与TLS | 域名解析、受信任HTTPS证书、入站443；入口代理IP与API出口IP分别核对 |
| 数据服务 | PostgreSQL、Redis持久卷；私有S3兼容桶、备份位置和访问权限 |
| AI服务 | 部署方自己的OpenAI项目凭证、可用问答/视觉模型、预算及服务可用性确认 |
| 首批用户 | 管理员核对的external_userid allowlist；从1—2位测试用户开始 |

主体认证、可接待范围、实际权限和费用以管理员后台为准；不能仅凭本地代码承诺个人主体一定可开通或无限免费。本项目不会要求在聊天中粘贴任何密钥、微信身份或配置文件。

## 从开通到扫码使用

1. 按[官方开通入口](https://kf.weixin.qq.com/kf/register/intro?from=kf_index)同时开通企业微信和微信客服，使用企业微信接管路径。新建专用客服账号，先避免接管正在人工使用的业务账号。
2. 在“微信客服→API→可调用接口的应用”配置自建应用，并把账号加入“通过API管理微信客服账号→企业内部开发”。相关接待人员应在应用可见范围内。权限路径见[官方概述](https://developer.work.weixin.qq.com/document/path/94638)。
3. 按[部署说明](deployment.md)配置服务器、私有存储、TLS和各worker。使用自建应用Secret，填写服务实际出口IP为可信IP；不混用独立客服后台和企业自建应用凭证。[开发前必读](https://developer.work.weixin.qq.com/document/path/90664)。
4. 在企业后台配置 `https://你的域名/wecom/callback`，Token和EncodingAESKey与服务器一致，完成官方URL验证。[回调配置](https://developer.work.weixin.qq.com/document/path/90930)。首次引导阶段allowlist保持空，API可接收合法通知，worker不会保存未获准用户资料。
5. 从后台获取客服链接/二维码，或使用下方私有管理工具。完整链接原样用于二维码，不手工拼接 `open_kfid` 成URL。[官方客服链接](https://developer.work.weixin.qq.com/document/path/94665)。
6. 测试用户扫码打开会话并发送一次性识别码，管理员取得匹配的官方标识后明确加入allowlist，统一重建API及所有worker配置。工具不会自动授权。加入名单后用户重新发送“保存…”测试资料；运行中同步会跳过精确的识别码消息，不把它存成资料或交给Agent。其他消息仍按已保存游标同步，首次启动或停机可能读取窗口内更早消息。
7. 完成文末实机验收，再逐步增加用户和文档/音视频能力。只打开会话不会建立普通API发送窗口，用户必须主动发消息。

## 私有接入工具

入口为 `python -m app.connectors.wecom.setup`，仅在管理员可信终端调用。它复用官方HTTP和Redis token adapter，不启动模型、不修改业务游标或allowlist。运行账号需有Redis和企业微信凭证；不需要业务数据库管理权限。

支持下列命令（绝对路径必须指向管理员私有目录）：

```sh
python -m app.connectors.wecom.setup accounts --output /private/accounts.json
python -m app.connectors.wecom.setup entry-link --output /private/link.json
python -m app.connectors.wecom.setup identify-user --output /private/candidate.json
```

多账号添加 `--account-index N`，按已配置ID排序从0开始；单账号可省略。结果必须写入新文件，已有文件不覆盖；控制台只显示状态和随机识别码，不输出用户标识、token或原始消息。失败可能留下空文件，下次换一个文件名。管理员应在私有终端读取文件并手工修改配置，不把文件上传到聊天或工单。

`identify-user` 仅在一个已配置且具有API管理权限的客服账号查找10分钟内的精确随机识别码，匹配多个用户即拒绝。不要复用或转发给多人。输出只是待确认候选身份；它不能替代管理员的名单决定。Linux输出文件为0600；Windows另在目录ACL中限制访问。使用Docker时将私有结果目录挂载给UID10001，避免写入容器只读根目录。

若已有多个配置客服账号，通过工具的账号选择参数确定目标，不能把其他客服的用户误认为本账号测试用户。官方[账号列表](https://developer.work.weixin.qq.com/document/path/94661)中的 `manage_privilege` 用于核对当前应用是否管理该账号。

### 第一次名单为空时的启动顺序

先准备TLS文件和私有结果目录，目录限制为管理员和容器UID10001可访问；不要把结果放仓库内。首次配置先关闭六个业务开关（WECOM/INGESTION/PARSING/MEDIA_PARSING/KNOWLEDGE/AGENT_ENABLED均false），allowlist保持 `[]`。填写企业ID、Secret及数据库/Redis密码，先启动postgres/redis并构建worker镜像；此时无需先知道open_kfid。

以下是Linux可信管理终端示例，私有目录由管理员预建：

```sh
dc() { docker compose --env-file /opt/wechat-knowledge-private/runtime.conf -f compose.yml -f compose.production.yml --profile wecom --profile production "$@"; }
dc up -d postgres redis
dc build api worker
dc run --rm --no-deps -v /opt/wechat-knowledge-private/results:/private worker python -m app.connectors.wecom.setup accounts --output /private/accounts.json
```

管理员从私有文件选定 `manage_privilege=true` 的账号，写入 `WECOM_OPEN_KFIDS`，配置callback Token/AESKey并把 `WECOM_ENABLED` 设true；其他五个业务开关继续false。启动 `dc up -d api worker proxy` 并在企业后台验证callback。渠道worker此时仍拒绝全部用户，但会正常推进同步游标。

```sh
dc run --rm --no-deps -v /opt/wechat-knowledge-private/results:/private worker python -m app.connectors.wecom.setup entry-link --output /private/link.json
dc run --rm --no-deps -v /opt/wechat-knowledge-private/results:/private worker python -m app.connectors.wecom.setup identify-user --output /private/candidate.json
```

第二条命令显示随机识别码；让测试用户在该客服会话发送精确文本，再于终端按回车。管理员核对候选并写入allowlist后，才填写S3/OpenAI/模型配置、打开其余功能开关，按[完整部署步骤](deployment.md)预检和启动全部profiles。整个引导不需要临时放开所有用户。

## 微信本身的限制

官方[发送消息](https://developer.work.weixin.qq.com/document/path/94677)限制普通回复：用户主动发消息后的48小时内最多5条；仅“新接入待处理/由智能助手接待”状态可发送，人工接管等状态可能阻止机器人回复。每段文本最多2048 UTF-8字节。资料接收通知、完成通知和分段答案共享配额。发送接口接受不等于最终送达。

官方[接收消息](https://developer.work.weixin.qq.com/document/path/94670)限制图片/语音2MB、视频10MB、文件20MB；超限可能只收到文字提示。系统本地支持PDF/DOCX/XLSX/PPTX解析不代表微信可以传递任意大附件。视频号不保证原始MP4，不能宣称转发卡片即可读取整段视频。

只可补拉最近3天消息，通知token有效10分钟。长时间停机后不能保证找回全部资料。用户不同的微信身份对应不同知识库，用户自己提供的来源编号或模型输出不能切换租户。

## 实机验收记录（需部署方真实环境）

| 步骤 | 应观察的结果 | 本地工程测试不能替代的证据 |
| --- | --- | --- |
| 后台保存callback | 官方URL验证成功 | 后台成功状态 |
| 扫客服二维码并发识别码 | 私有工具匹配唯一用户，管理员加入名单 | 手机会话与本地私有结果核对 |
| 公众号/视频号打开分享菜单 | 核对是否可选择该客服，实际卡片是否到达接口 | 手机操作路径与安全诊断结果，不记录原始回调 |
| 发“保存：合成验收事实…” | 接收通知、完成通知 | 微信客户端实际到达 |
| 问刚才的事实 | 回答引用正确来源，无编造 | 手机显示内容与保存资料比对 |
| 问从未发送的事实 | 明确没有找到相关资料 | 微信实际回复 |
| 第二位允许用户提问第一位资料 | 无法获取第一位资料 | 两部微信分别检查 |
| 非名单用户发消息 | 不入库、不回复、不调用模型 | 管理员状态与账单/任务核对 |
| 发小PDF、图片、音频、视频号 | 正确收录或明确metadata_only | 实际格式、语音识别、引用定位 |
| 重启和重发相同资料 | 可恢复，无重复来源/重复发送 | 重启后的实际会话 |

这些项目在没有真实企业配置时保持“未执行”。当前本地测试通过仅证明代码、真实数据库/队列/对象存储与模拟官方API能够协同。
