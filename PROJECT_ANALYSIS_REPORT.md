# MetaSo Man Indexer 项目分析报告

## 1. 项目意图与目标

本项目是 MetaSo Man Indexer：面向 MetaID 协议的链上社交数据索引与检索服务。其核心目标是从 UTXO 链（BTC/MVC）中解析并索引 MetaID/MetaSo 相关 Pin 数据，将其结构化存储，并通过 HTTP API 与 Web 浏览器界面对外提供检索、社交、资产与统计服务。项目同时支持 mempool 实时更新、协议扩展（Buzz/Follow/MRC20/MRC721/MetaAccess/MetaName 等）、以及多数据库后端（MongoDB/Pebble/PostgreSQL）。

## 2. 主要功能模块

### 2.1 入口与运行控制
- `app.go`：程序入口，加载配置、初始化数据库与链适配器、启动 API 服务、启动索引循环与后台任务。

### 2.2 配置与公共能力
- `common/`：配置解析（TOML + CLI flag）、全局配置与常量、缓存/字典KV、MetaID 计算、内容类型识别。

### 2.3 链适配与索引器
- `adapter/bitcoin`, `adapter/microvisionchain`：链交互（RPC/区块/交易/地址解析）与 Pin 解析逻辑。
- `man/`：索引调度、mempool 处理、Pin 校验、transfer 检测、MetaID 信息维护、协议数据分流与批量落库。

### 2.4 协议与业务模块
- `basicprotocols/metaso`：Buzz 社交数据、推荐/热度、搜索、关注关系、统计指标、PEV。
- `basicprotocols/metaname`：MetaName 索引与查询。
- `basicprotocols/mrc721`：NFT 协议索引与查询。
- `man/mrc20*`：MRC20 协议解析、余额/历史处理。
- `man/meta_access*`：MetaAccess 访问控制与解密。

### 2.5 数据存储
- `database/mongodb`：主推荐后端，支持复杂查询与聚合。
- `database/pebbledb`：轻量快速 KV 存储，供高频分页/索引加速。
- `database/postgresql`：可选关系型存储实现。

### 2.6 API 与 Web
- `api/`：Gin 路由、JSON API、Swagger。
- `web/`：模板与静态资源，提供浏览器式区块/Pin/MetaID 浏览体验。

### 2.7 CLI 与工具
- `cmd/`：命令行工具（如 MRC20 操作）。

## 3. 关键流程实现

### 3.1 启动流程
1. 读取 `config.toml` + CLI 参数（`common.InitConfig`）。
2. 初始化同步数据库与适配器（`common.InitSyncDB`, `man.InitAdapter`）。
3. 若 `server=1`，启动 HTTP 服务（`api.Start`）。
4. 启动 MetaSo/MetaName/MRC721 等模块同步与后台任务。
5. 启动 ZMQ 监听（mempool 实时数据）。
6. 主循环：区块同步索引 + mempool 清理 + PEV 同步。

### 3.2 区块索引流程（主链数据）
1. `IndexerRun` 获取同步高度区间（本地已同步高度到链上最新高度）。
2. 针对每个区块调用 `PebbleStore.DoIndexerRun`，内部解析区块 Pin、过滤/校验、批量落库。
3. `GetSaveData` 负责：
   - 解析 Pin 列表与协议数据
   - 处理 modify/revoke 逻辑与原始 Pin 关联
   - 维护 MetaID 资料（/info 相关）
   - 识别 follow 数据并写入关系表
   - 触发 MRC20 处理（满足高度与模块条件）
4. 完成后更新同步高度，并清理已上链 mempool 数据。

### 3.3 Mempool 实时流程
1. ZMQ 接收新交易，解析为 Pin 列表。
2. `handleMempoolPin` 将 Pin 写入 mempool 存储，并触发 MetaAccess/MRC20 等逻辑。
3. `CheckNewBlock` 定期根据新区块清除已确认的 mempool 记录。

### 3.4 Web/API 服务
- Gin 统一监听，包含 JSON API、Swagger 与 Web 页面。
- 若配置 `cacheUrl`，部分接口会反向代理到缓存服务（如 `/v1/users`、`/v1/search` 等）。

## 4. 对外接口列表（HTTP）

### 4.1 通用响应格式
- 大多数 JSON API 返回：`{ "code": int, "message": string, "data": any }`
- 少数接口返回文件流或自定义结构（见对应接口说明）。

### 4.2 Web UI 页面路由

`GET /` 主页，Pin 列表。
Inputs: 无。
Outputs: HTML。

`GET /pin/list/:page` Pin 列表分页。
Inputs: `page`(path, int), `lastId`(query, string, 可选)。
Outputs: HTML。

`GET /metaid/:page` MetaID 列表分页。
Inputs: `page`(path, int)。
Outputs: HTML。

`GET /blocks/:page` 区块浏览分页。
Inputs: `page`(path, int)。
Outputs: HTML。

`GET /mempool/:page` mempool 列表。
Inputs: `page`(path, int)。
Outputs: HTML。

`GET /block/:height` 指定区块详情。
Inputs: `height`(path, int)。
Outputs: HTML。

`GET /pin/:number` Pin 详情。
Inputs: `number`(path, string, Pin ID 或编号)。
Outputs: HTML。

`GET /search/:key` 搜索页面。
Inputs: `key`(path, string)。
Outputs: HTML。

`GET /tx/:chain/:txid` 交易详情。
Inputs: `chain`(path, btc|mvc), `txid`(path, string)。
Outputs: HTML。

`GET /node/:rootid` MetaID 节点树。
Inputs: `rootid`(path, string)。
Outputs: HTML。

`GET /content/:number` 直接输出 Pin 内容。
Inputs: `number`(path, string)。
Outputs: 原始内容或视频页面；若配置 `cacheUrl` 且 `cache` 未指定，反向代理 `cacheUrl/v1/media/{id}`。

`GET /stream/:number` 输出二进制内容。
Inputs: `number`(path, string)。
Outputs: `application/octet-stream`。

`GET /mrc20/:page` MRC20 列表页面。
Inputs: `page`(path, int)。
Outputs: HTML。

`GET /mrc20/history/:id/:page` MRC20 历史页面。
Inputs: `id`(path, string), `page`(path, int)。
Outputs: HTML。

`GET /mrc721/:page` MRC721 列表页面。
Inputs: `page`(path, int)。
Outputs: HTML。

`GET /mrc721/item/list/:name/:page` MRC721 集合项列表。
Inputs: `name`(path, string), `page`(path, int)。
Outputs: HTML。

`GET /swagger/*any` Swagger UI。
Inputs: 无。
Outputs: HTML。

### 4.3 JSON API（/api）

`GET /api/metaid/list`
Inputs: `page`(query,int,必填), `size`(query,int,必填), `order`(query,string,可选)。
Outputs: `data.list` MetaIdInfo[]，`data.count` 总数。

`GET /api/metaid/list/limit`
Inputs: `lastupdate`(query,int,必填), `limit`(query,int,可选，<=2000)。
Outputs: `data.list` MetaIdInfo[]。

`GET /api/pin/list`
Inputs: `page`(query,int), `size`(query,int), `lastId`(query,string,可选)。
Outputs: `data.Pins` PinMsg[]，`data.Count` 总数，`data.LastId` 游标。

`POST /api/pin/check`
Inputs: JSON body `{ "pinList": [string] }`。
Outputs: `data` Pin 检查结果列表（字段由 DB 实现决定）。

`GET /api/block/list`
Inputs: `page`(query,int), `size`(query,int)。
Outputs: `data.msgMap`(height->PinMsg[]), `data.msgList`(height[])。

`GET /api/mempool/list`
Inputs: `page`(query,int), `size`(query,int)。
Outputs: `data.Pins` PinMsg[]。

`GET /api/node/list`
Inputs: `rootid`(query,string), `page`(query,int), `size`(query,int)。
Outputs: `data.RootId`, `data.Total`, `data.Pins` PinInscription[]。

`GET /api/pin/:numberOrId`
Inputs: `numberOrId`(path,string)。
Outputs: PinInscription（补充 `preview` 与 `content` URL）。

`GET /api/pin/ByOutput/:output`
Inputs: `output`(path,string)。
Outputs: PinInscription。

`GET /api/address/pin/utxo/count/:address`
Inputs: `address`(path,string)。
Outputs: `data.utxoNum`, `data.utxoSum`。

`GET /api/address/pin/list/:addressType/:address`
Inputs: `addressType`(path,string), `address`(path,string), `cursor`(query,int,可选), `size`(query,int,可选), `cnt`(query,bool,可选), `path`(query,string,可选)。
Outputs: 若 `cnt=true`，返回 `{list,total}`；否则返回 PinInscription[]。

`GET /api/node/child/:pinId`
Inputs: `pinId`(path,string)。
Outputs: PinInscription[]。

`GET /api/node/parent/:pinId`
Inputs: `pinId`(path,string)。
Outputs: PinInscription。

`GET /api/info/address/:address`
Inputs: `address`(path,string)。
Outputs: `data` 为 MetaIdInfo + `unconfirmed` + `blocked`。若配置 `cacheUrl` 且 `cache` 未指定则代理 `cacheUrl/v1/users/{address}`。

`GET /api/info/metaid/:metaId`
Inputs: `metaId`(path,string)。
Outputs: `data` 为 MetaIdInfo + `unconfirmed` + `blocked`。若配置 `cacheUrl` 且 `cache` 未指定则代理 `cacheUrl/v1/users/info/metaid/{metaId}`。

`GET /api/info/search`
Inputs: `keyword`(query,string), `keytype`(query,string)。
Outputs: 反向代理 `cacheUrl/v1/search`。

`GET /api/info/metaidUpdate`
Inputs: 无。
Outputs: 更新结果。

`GET /api/getAllPinByPath`
Inputs: `page`(query,int), `limit`(query,int), `path`(query,string)。
Outputs: `data.list` PinInscription[]，`data.total` 总数。

`POST /api/getAllPinByPathAndMetaId`
Inputs: JSON body `{ "page": int, "size": int, "path": string, "metaIdList": [string] }`。
Outputs: `data.list` PinInscription[]，`data.total` 总数。

`POST /api/metaid/dataValue`
Inputs: JSON body `{ "list": [string] }`（metaId 列表）。
Outputs: `data` MetaIdDataValue[]。

`POST /api/generalQuery`
Inputs: JSON body（通用查询）：
```
{
  "collection": "pins | paylike | ...",
  "action": "get | count | sum",
  "filterRelation": "and | or",
  "field": ["fieldName"],
  "filter": [{"operator":"=|>|>=|<|<=","key":"field","value":"xxx"}],
  "cursor": 0,
  "limit": 20,
  "sort": ["field","asc|desc"]
}
```
Outputs: `data` 查询结果。

`GET /api/follow/record`
Inputs: `metaId`(query,string), `followerMetaId`(query,string)。
Outputs: `data` 关注记录详情。

`GET /api/metaid/followerList/:metaid`
Inputs: `metaid`(path,string), `cursor`(query,int,可选), `size`(query,int,可选), `followDetail`(query,bool,可选)。
Outputs: `data.list` + `data.total`。

`GET /api/metaid/followingList/:metaid`
Inputs: 同上。
Outputs: 同上。

`GET /api/metaid/recommended`
Inputs: `limit`(query,int,可选,<=500), `num`(query,int,可选)。
Outputs: 推荐 MetaID 列表。

`GET /api/notifcation/list`
Inputs: `address`(query,string), `lastId`(query,int)。
Outputs: `{code:200,message:"ok",data:NotifcationData[],total:int}`（非通用封装）。

`GET /api/dict/set`
Inputs: `key`(query,string), `value`(query,string)。
Outputs: 通用成功响应。

`GET /api/dict/get`
Inputs: `key`(query,string)。
Outputs: `data` 为字符串值。

`GET /api/block/file`
Inputs: `height`(query,int), `chain`(query,string), `part`(query,int,可选)。
Outputs: 文件下载流。

`GET /api/block/file/partCount`
Inputs: `height`(query,int), `chain`(query,string)。
Outputs: `data.partCount`, `data.btcMin`, `data.btcMax`, `data.mvcMin`, `data.mvcMax`。

`GET /api/block/file/create`
Inputs: `token`(query,string,必填), `chain`(query,string), `from`(query,int), `to`(query,int)。
Outputs: 文本 `block file create finish`。

`GET /api/block/id/list`
Inputs: `token`(query,string,必填), `chain`(query,string), `height`(query,int)。
Outputs: `data` 为 block pinId 列表。

`GET /api/block/id/create`
Inputs: `token`(query,string,必填), `chain`(query,string), `from`(query,int), `to`(query,int)。
Outputs: 文本 `block file pin id list create finish`。

`GET /api/reindex/:chain/:from/:to`
Inputs: `chain`(path,string), `from`(path,int), `to`(path,int), `token`(query,string,必填)。
Outputs: 文本 `reindex finish`。

### 4.4 访问控制 API（/api/access）

`GET /api/access/getPubKey`
Inputs: 无。
Outputs: `data` 为服务端公钥字符串。

`POST /api/access/decrypt`
Inputs: JSON body `{ address, timestamp, publicKey, sign, pinId, controlPath, controlPinId }`。
Outputs: `data.status`(purchased|unpurchased|mempool), `data.contentResult`(string|null), `data.filesResult`(array|null)。

`GET /api/access/getControlByContentPin`
Inputs: `pinId`(query,string)。
Outputs: 访问控制 Pin 信息。

### 4.5 MRC20 API（/api/mrc20）

`GET /api/mrc20/tick/all`
Inputs: `cursor`(query,int), `size`(query,int), `order`(query,string,可选), `completed`(query,string,可选), `orderType`(query,string,可选)。
Outputs: `data.list` MRC20 Tick 列表，`data.total`。

`GET /api/mrc20/tick/info/:id`
Inputs: `id`(path,string)。
Outputs: Tick 详情。

`GET /api/mrc20/tick/info`
Inputs: `id`(query,string,可选), `tick`(query,string,可选)。
Outputs: Tick 详情。

`GET /api/mrc20/tick/address`
Inputs: `cursor`(query,int), `size`(query,int), `tickId`(query,string), `address`(query,string), `status`(query,string,可选), `verify`(query,string,可选)。
Outputs: `data.list` 记录列表，`data.total`。

`GET /api/mrc20/tick/history`
Inputs: `cursor`(query,int), `size`(query,int), `tickId`(query,string)。
Outputs: `data.list`, `data.total`。

`GET /api/mrc20/address/balance/:address`
Inputs: `address`(path,string), `cursor`(query,int), `size`(query,int)。
Outputs: `data.list`, `data.total`。

`GET /api/mrc20/tx/history`
Inputs: `cursor`(query,int), `size`(query,int), `index`(query,int), `txId`(query,string)。
Outputs: `data.list`, `data.total`。

`GET /api/mrc20/address/shovel/list`
Inputs: `cursor`(query,int), `size`(query,int), `tickId`(query,string), `address`(query,string)。
Outputs: `data.list`, `data.total`。

`GET /api/mrc20/shovel/used`
Inputs: `cursor`(query,int), `size`(query,int), `address`(query,string), `tickId`(query,string)。
Outputs: `data.list`, `data.total`。

`GET /api/mrc20/tick/AddressBalance`
Inputs: `address`(query,string), `tickId`(query,string)。
Outputs: `data` 为余额数值。

### 4.6 MRC721 API（/api/mrc721）

`GET /api/mrc721/collection/pageList`
Inputs: `cousor`(query,int), `size`(query,int)。
Outputs: `data.list`, `data.total`。

`GET /api/mrc721/collection/info`
Inputs: `name`(query,string) 或 `pinId`(query,string) 二选一。
Outputs: 集合详情。

`GET /api/mrc721/collection/items/pageList`
Inputs: `cousor`(query,int), `size`(query,int), `pinid`(query,string,可选)。
Outputs: `data.list`, `data.total`。

`GET /api/mrc721/address/collection`
Inputs: `cousor`(query,int), `size`(query,int), `address`(query,string)。
Outputs: `data.list`, `data.total`。

`GET /api/mrc721/address/item`
Inputs: `cousor`(query,int), `size`(query,int), `address`(query,string), `pinId`(query,string,可选)。
Outputs: `data.list`, `data.total`。

`GET /api/mrc721/item/info`
Inputs: `pinId`(query,string)。
Outputs: Item 详情。

### 4.7 MetaName API（/api/metaname）

`GET /api/metaname/list`
Inputs: `size`(query,int), `lastId`(query,string,可选)。
Outputs: `data.list`, `data.total`, `data.lastId`。

`GET /api/metaname/info`
Inputs: `name`(query,string)。
Outputs: `data.info`, `data.history`。

### 4.8 MetaSo 社交与设置 API（模块开启时可用）

`GET /social/buzz/newest`
Inputs: `lastId`(query,string), `size`(query,int), `metaid`(query,string,可选), `followed`(query,string,true/false)。
Outputs: `data.list` Buzz 列表，`data.total`，`data.lastId`。

`GET /social/buzz/recommended`
Inputs: `lastId`(query,string), `size`(query,int), `userAddress`(query,string,可选)。
Outputs: 同上。

`GET /social/buzz/updater`
Inputs: 无。
Outputs: `data` 含 `lastNo,lastVer,curNo,curVer,serverUrl,mandatory`。

`GET /social/buzz/hot`
Inputs: `lastId`(query,string), `size`(query,int,<=50)。
Outputs: 同 `newest`。

`GET /social/buzz/search`
Inputs: `lastId`(query,string), `size`(query,int), `key`(query,string)。
Outputs: 同 `newest`。

`GET /social/buzz/info`
Inputs: `pinId`(query,string)。
Outputs: `data.tweet`, `data.comments`, `data.like`, `data.donates`, `data.blocked`。

`GET /social/buzz/follow`
Inputs: `metaid`(query,string)。
Outputs: `data.list` 关注对象列表（含 mempool 标记）。

`GET /host/block/sync-newest`
Inputs: 无。
Outputs: 当前同步高度信息。

`GET /host/block/ndv`
Inputs: `height`(query,int), `host`(query,string,可选), `cursor`(query,int), `size`(query,int), `orderby`(query,string,可选)。
Outputs: NDV 数据列表。

`GET /host/block/mdv`
Inputs: `height`(query,int), `address`(query,string,可选), `cursor`(query,int), `size`(query,int), `orderby`(query,string,可选)。
Outputs: MDV 数据列表。

`GET /host/info`
Inputs: `host`(query,string), `cursor`(query,int), `size`(query,int), `orderby`(query,string,可选)。
Outputs: Host block 信息列表。

`POST /host/viewed/add`
Inputs: JSON body `{ "pinIdList": [string], "address": string }`。
Outputs: 通用成功响应。

`GET /ft/mrc20/address/deploy-list`
Inputs: `address`(query,string), `tickType`(query,string,可选)。
Outputs: 部署的 MRC20 列表。

`GET /metaso/settings/blocked/list`
Inputs: `blockType`(query,string), `cursor`(query,int), `size`(query,int)。
Outputs: `data.list`, `data.total`。

`GET /metaso/settings/blocked/add`
Inputs: `blockType`(query,string), `blockContent`(query,string)。
Outputs: 成功响应。

`GET /metaso/settings/blocked/delete`
Inputs: `blockType`(query,string), `blockContent`(query,string)。
Outputs: 成功响应。

`GET /metaso/settings/recommended/list`
Inputs: `cursor`(query,int), `size`(query,int)。
Outputs: `data.list`, `data.total`。

`GET /metaso/settings/recommended/add`
Inputs: `authorAddress`(query,string), `authorName`(query,string)。
Outputs: 成功响应。

`GET /metaso/settings/recommended/delete`
Inputs: `authorAddress`(query,string)。
Outputs: 成功响应。

### 4.9 统计 API（/statistics）

`GET /statistics/host/metablock/sync-newest`
Inputs: 无。
Outputs: 当前 MetaBlock 同步状态与进度。

`GET /statistics/host/metablock/info`
Inputs: `height`(query,int), `cursor`(query,int), `size`(query,int)。
Outputs: `data.info`, `data.total`, `data.list`。

`GET /statistics/metablock/address/info`
Inputs: `height`(query,int), `cursor`(query,int), `size`(query,int)。
Outputs: `data.info`, `data.total`, `data.list`。

`GET /statistics/ndv`
Inputs: `host`(query,string,可选), `cursor`(query,int), `size`(query,int), `orderby`(query,string,可选)。
Outputs: NDV 列表。

`GET /statistics/mdv`
Inputs: `address`(query,string,可选), `cursor`(query,int), `size`(query,int), `orderby`(query,string,可选)。
Outputs: MDV 列表。

`GET /statistics/metablock/host/value`
Inputs: `heightBegin`(query,int,可选), `heightEnd`(query,int,可选), `timeBegin`(query,int,可选), `timeEnd`(query,int,可选), `host`(query,string,可选), `cursor`(query,int), `size`(query,int)。
Outputs: `data.total`, `data.list`。

`GET /statistics/metablock/host/address/list`
Inputs: `heightBegin`(query,int,可选), `heightEnd`(query,int,可选), `timeBegin`(query,int,可选), `timeEnd`(query,int,可选), `host`(query,string,可选), `cursor`(query,int), `size`(query,int)。
Outputs: `data.total`, `data.list`。

`GET /statistics/metablock/host/address/value`
Inputs: `heightBegin`(query,int,可选), `heightEnd`(query,int,可选), `timeBegin`(query,int,可选), `timeEnd`(query,int,可选), `host`(query,string,可选), `address`(query,string,可选), `cursor`(query,int), `size`(query,int)。
Outputs: `data.total`, `data.list`。

## 5. show.now API 关联判断

结论：高度可能是本项目（或其兼容实现）编译后的服务，通过反向代理挂载在 `https://www.show.now/man` 路径下。理由是 `https://www.show.now/man/api/access/getPubKey` 可直接返回与本项目 `GET /api/access/getPubKey` 一致的结构与语义，仅多了 `/man` 前缀（典型反向代理前缀），路径命名和接口功能完全匹配。citeturn0view1

更严格的确认方式是：抓取 show.now 页面实际请求的 API 列表，确认是否调用 `/man/api/*` 下的端点；但从路径一致性和端点可达性来看，判断结果为“是”。
