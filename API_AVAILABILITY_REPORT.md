# MetaSo Man Indexer API 可用性与参考文档

生成时间：2026-03-15 06:37:35 UTC

## 1. 范围与方法

本报告目标：
- 从代码层面列出对外 API，说明用途、输入、输出以及是否实现。
- 在生产环境 `https://www.show.now/man` 上对可读接口进行探测，输出可用性结果。

探测方法：
- 使用 `curl` 对生产环境逐一发起请求（GET），超时阈值 5 秒。
- 对需要 POST 或会产生副作用（写入/重建/管理类）的接口未直接探测，保留为“未验证”。

说明与限制：
- `Timeout` 仅代表 5 秒内未返回，不等于不可用（可能是查询过慢或被限流）。
- `404` 表示生产反向代理或后端未提供该路由（可能模块未启用）。
- 部分接口依赖 `cacheUrl` 反向代理或内部服务（如 updater），在生产上可能返回错误。

## 2. 常见响应结构（代码推断）

通用 JSON 响应（多数接口）：
- `code`: int
- `message`: string
- `data`: any

常见数据结构（摘录关键字段）：
- `PinInscription`: `id`, `number`, `metaid`, `address`, `creator`, `output`, `timestamp`, `genesisHeight`, `genesisTransaction`, `operation`, `path`, `contentType`, `contentBody`, `status`, `originalId`, `preview`, `content`, `pop`, `chainName`。
- `MetaIdInfo`: `metaid`, `address`, `name`, `avatar`, `bio`, `background`, `chatpubkey`, `followCount` 等。
- `PinMsg`: `id`, `number`, `content`, `path`, `metaid`, `height`, `pop`, `chainName`。
- `FollowData`: `metaId`, `followMetaId`, `followTime`, `followPinId`, `status`。

## 3. API 参考（代码层面）

### 3.1 Core API（/api）

`GET /api/metaid/list`
用途：MetaID 分页列表。
输入：`page`(int,必填), `size`(int,必填), `order`(string,可选)。
输出：`data.list` MetaIdInfo[]，`data.count` 总数。
实现：`api/btc_jsonapi.go: metaidList`。

`GET /api/metaid/list/limit`
用途：按更新时间批量拉取 MetaID 信息。
输入：`lastupdate`(int,必填), `limit`(int,可选<=2000)。
输出：`data.list` MetaIdInfo[]。
实现：`api/btc_jsonapi.go: metaidListLimit`。

`GET /api/pin/list`
用途：Pin 分页列表（支持 lastId 游标）。
输入：`page`(int), `size`(int), `lastId`(string,可选)。
输出：`data.Pins` PinMsg[]，`data.Count`，`data.LastId`。
实现：`api/btc_jsonapi.go: pinList`。

`POST /api/pin/check`
用途：批量检查 Pin 是否存在/有效。
输入：JSON `{ "pinList": [string] }`。
输出：`data` 校验结果列表。
实现：`api/btc_jsonapi.go: pinCheck`。

`GET /api/block/list`
用途：区块内 Pin 列表分页。
输入：`page`(int), `size`(int)。
输出：`data.msgMap`、`data.msgList`。
实现：`api/btc_jsonapi.go: blockList`。

`GET /api/mempool/list`
用途：mempool Pin 列表。
输入：`page`(int), `size`(int)。
输出：`data.Pins`。
实现：`api/btc_jsonapi.go: mempoolList`。

`GET /api/node/list`
用途：指定 MetaID 的 Pin 列表。
输入：`rootid`(string), `page`(int), `size`(int)。
输出：`data.RootId`, `data.Total`, `data.Pins`。
实现：`api/btc_jsonapi.go: nodeList`。

`GET /api/pin/:numberOrId`
用途：按 Pin 编号或 PinId 获取详情。
输入：`numberOrId`(path)。
输出：PinInscription（含 `preview`/`content` URL）。
实现：`api/btc_jsonapi.go: getPinById`。

`GET /api/pin/ByOutput/:output`
用途：按输出点查询 Pin。
输入：`output`(path)。
输出：PinInscription。
实现：`api/btc_jsonapi.go: getPinByOutput`。

`GET /api/address/pin/utxo/count/:address`
用途：地址 UTXO 数量与总额。
输入：`address`(path)。
输出：`data.utxoNum`, `data.utxoSum`。
实现：`api/btc_jsonapi.go: getPinUtxoCountByAddress`。

`GET /api/address/pin/list/:addressType/:address`
用途：按地址/创建者查询 Pin。
输入：`addressType`(path, "address"|"creator"), `address`(path), `cursor`(query), `size`(query), `cnt`(query) 等。
输出：PinInscription[] 或 `{list,total}`。
实现：`api/btc_jsonapi.go: getPinListByAddress`。

`GET /api/node/child/:pinId`
用途：获取子节点。
输入：`pinId`。
输出：PinInscription[]。
实现：`api/btc_jsonapi.go: getChildNodeById`。

`GET /api/node/parent/:pinId`
用途：获取父节点。
输入：`pinId`。
输出：PinInscription。
实现：`api/btc_jsonapi.go: getParentNodeById`。

`GET /api/info/address/:address`
用途：根据地址获取 MetaID 信息。
输入：`address`。
输出：`data` 为 MetaIdInfo + `unconfirmed` + `blocked`。
实现：`api/btc_jsonapi.go: getInfoByAddress`。

`GET /api/info/metaid/:metaId`
用途：根据 metaid 获取 MetaID 信息。
输入：`metaId`。
输出：同上。
实现：`api/btc_jsonapi.go: getInfoByMetaId`。

`GET /api/info/search`
用途：搜索（代理 cacheUrl）。
输入：`keyword`, `keytype`。
输出：代理结果。
实现：`api/btc_jsonapi.go: infoSearch`。

`GET /api/info/metaidUpdate`
用途：刷新 MetaID 信息（写操作）。
输入：无。
输出：更新结果。
实现：`api/btc_jsonapi.go: infometaidUpdate`。

`GET /api/getAllPinByPath`
用途：按 path 查询 Pin。
输入：`page`, `limit`, `path`。
输出：`data.list`, `data.total`。
实现：`api/btc_jsonapi.go: getAllPinByPath`。

`POST /api/getAllPinByPathAndMetaId`
用途：按 path + metaId 列表查询 Pin。
输入：JSON `{page,size,path,metaIdList}`。
输出：`data.list`, `data.total`。
实现：`api/btc_jsonapi.go: getAllPinByPathAndMetaId`。

`POST /api/metaid/dataValue`
用途：批量查询 metaid 数据值。
输入：JSON `{list:[metaid...]}`。
输出：`data` MetaIdDataValue[]。
实现：`api/btc_jsonapi.go: getDataValueByMetaIdList`。

`POST /api/generalQuery`
用途：通用查询接口（集合、过滤、排序）。
输入：见 README 的 `Generator` 结构。
输出：`data` 查询结果。
实现：`api/btc_jsonapi.go: generalQuery`。

`GET /api/follow/record`
用途：查询某 metaid 是否关注另一个 metaid。
输入：`metaId`, `followerMetaId`。
输出：关注记录。
实现：`api/btc_jsonapi.go: getFollowRecord`。

`GET /api/metaid/followerList/:metaid`
用途：metaid 的粉丝列表。
输入：`cursor`, `size`, `followDetail`。
输出：`data.list`, `data.total`。
实现：`api/btc_jsonapi.go: getFollowerListByMetaId`。

`GET /api/metaid/followingList/:metaid`
用途：metaid 的关注列表。
输入：`cursor`, `size`, `followDetail`。
输出：同上。
实现：`api/btc_jsonapi.go: getFollowingListByMetaId`。

`GET /api/metaid/recommended`
用途：推荐 MetaID 列表。
输入：`limit`, `num`。
输出：推荐列表。
实现：`api/btc_jsonapi.go: getRecommendedList`。

`GET /api/notifcation/list`
用途：通知列表。
输入：`address`, `lastId`。
输出：`{code:200,message:"ok",data:[...],total}`。
实现：`api/btc_jsonapi.go: notifcationList`。

`GET /api/dict/get`
用途：读取 KV。
输入：`key`。
输出：`data` 字符串。
实现：`api/btc_jsonapi.go: dictGet`。

`GET /api/dict/set`（写操作，未探测）
用途：写入 KV。

`GET /api/block/file`、`/api/block/file/partCount`、`/api/block/file/create`、`/api/block/id/list`、`/api/block/id/create`（管理与文件类，未完整探测）。

`GET /api/reindex/:chain/:from/:to`（管理类，需 token，未探测）。

### 3.2 Access API（/api/access）

`GET /api/access/getPubKey`
用途：获取服务端公钥。
输出：`data` 公钥。

`POST /api/access/decrypt`
用途：内容解密与访问控制校验。
输入：JSON `{address,timestamp,publicKey,sign,pinId,controlPath,controlPinId}`。
输出：`status` + `contentResult` + `filesResult`。

`GET /api/access/getControlByContentPin`
用途：获取内容的访问控制 Pin。
输入：`pinId`。
输出：控制信息。

### 3.3 MRC20 API（/api/mrc20）

`GET /api/mrc20/tick/all`
用途：MRC20 Tick 列表。
输入：`cursor`, `size`, `order`, `completed`, `orderType`。
输出：`data.list`, `data.total`。

`GET /api/mrc20/tick/info/:id` 或 `GET /api/mrc20/tick/info?id=...&tick=...`
用途：Tick 详情。
输出：Tick 结构体。

`GET /api/mrc20/tick/address`
用途：按地址查询 Tick 操作历史。
输入：`tickId`, `address`, `cursor`, `size` 等。
输出：`data.list`, `data.total`。

`GET /api/mrc20/tick/history`
用途：按 tickId 查询历史。
输出：`data.list`, `data.total`。

`GET /api/mrc20/address/balance/:address`
用途：地址资产余额。
输出：`data.list`, `data.total`。

`GET /api/mrc20/tx/history`
用途：按 txId 查询历史。
输入：`txId`, `index`, `cursor`, `size`。

`GET /api/mrc20/address/shovel/list`
用途：shovel 查询。

`GET /api/mrc20/shovel/used`
用途：已使用 shovel 查询。

`GET /api/mrc20/tick/AddressBalance`
用途：指定地址 + tickId 查询余额。

### 3.4 MRC721 API（/api/mrc721）

`GET /api/mrc721/collection/pageList`
用途：集合分页列表。

`GET /api/mrc721/collection/info`
用途：集合详情（`name` 或 `pinId`）。

`GET /api/mrc721/collection/items/pageList`
用途：集合项分页。

`GET /api/mrc721/address/collection`
用途：地址持有集合列表。

`GET /api/mrc721/address/item`
用途：地址持有 items。

`GET /api/mrc721/item/info`
用途：item 详情。

### 3.5 MetaName API（/api/metaname）

`GET /api/metaname/list`
用途：MetaName 列表。

`GET /api/metaname/info`
用途：MetaName 详情。

### 3.6 MetaSo Social / Settings / FT

`GET /social/buzz/newest` `recommended` `hot` `search` `info` `follow`
用途：Buzz 动态、推荐、热度、搜索、详情、关注关系。

`GET /social/buzz/updater`
用途：版本更新信息（依赖内部服务）。

`GET /host/block/sync-newest` `block/ndv` `block/mdv` `info`
用途：Host/NDV/MDV 区块与统计。

`POST /host/viewed/add`
用途：记录用户已读（写操作）。

`GET /ft/mrc20/address/deploy-list`
用途：地址部署的 MRC20 列表。

`GET /metaso/settings/blocked/list` `recommended/list`
用途：屏蔽/推荐列表。

`GET /metaso/settings/blocked/add|delete`, `/metaso/settings/recommended/add|delete`
用途：管理写操作（未探测）。

### 3.7 Statistics API（/statistics）

`GET /statistics/host/metablock/sync-newest`
用途：MetaBlock 同步进度。

`GET /statistics/host/metablock/info`
用途：NDV Block 列表。

`GET /statistics/metablock/address/info`
用途：MDV Block 列表。

`GET /statistics/ndv` `mdv`
用途：NDV/MDV 分页列表。

`GET /statistics/metablock/host/value`
用途：host value 列表。

`GET /statistics/metablock/host/address/list`
用途：host address value 列表。

`GET /statistics/metablock/host/address/value`
用途：host + address value 列表。

## 4. 生产可用性探测结果（show.now/man）

说明：以下为 GET 探测结果。`Timeout` 表示 5 秒内无响应；`Not Found` 表示生产未提供路由或模块未启用；`OK (business error)` 表示路由可用但业务返回错误/无数据。

|Method|Path|HTTP|Code|Message|Probe Result|
|---|---|---|---|---|---|
|GET|/api/metaid/list?page=1&size=10|000|||Timeout|
|GET|/api/metaid/list/limit?lastupdate=0&limit=10|404|||Not Found|
|GET|/api/pin/list?page=1&size=10|000|||Timeout|
|GET|/api/block/list?page=1&size=10|200|1|ok|OK|
|GET|/api/mempool/list?page=1&size=10|000|||Timeout|
|GET|/api/node/list?rootid=0d166d6c6e2ac2f839fb63e22bd93ed571fc06940eadca0986427402eb688a4d&page=1&size=10|200|404|service exception.|OK (business error)|
|GET|/api/pin/14bdb4797d8874826a1e0895c8014aca2084ba8177c74b5b963ddb4945952505i0|000000|||Timeout|
|GET|/api/pin/ByOutput/0000000000000000:0|200|100|no pin found.|OK (business error)|
|GET|/api/address/pin/utxo/count/12ghVWG1yAgNjzXj4mr3qK9DgyornMUikZ|000000|||Timeout|
|GET|/api/address/pin/list/address/12ghVWG1yAgNjzXj4mr3qK9DgyornMUikZ?cursor=0&size=10&cnt=true|000|1|ok|Timeout|
|GET|/api/node/child/14bdb4797d8874826a1e0895c8014aca2084ba8177c74b5b963ddb4945952505i0|200|100|no child found.|OK (business error)|
|GET|/api/node/parent/14bdb4797d8874826a1e0895c8014aca2084ba8177c74b5b963ddb4945952505i0|200|100|no node found.|OK (business error)|
|GET|/api/info/address/12ghVWG1yAgNjzXj4mr3qK9DgyornMUikZ|200|1|ok|OK|
|GET|/api/info/metaid/0d166d6c6e2ac2f839fb63e22bd93ed571fc06940eadca0986427402eb688a4d|200|1|ok|OK|
|GET|/api/info/search?keyword=metaid&keytype=metaid|404|||Not Found|
|GET|/api/getAllPinByPath?page=1&limit=10&path=/info/name|000|||Timeout|
|GET|/api/follow/record?metaId=0d166d6c6e2ac2f839fb63e22bd93ed571fc06940eadca0986427402eb688a4d&followerMetaId=0d166d6c6e2ac2f839fb63e22bd93ed571fc06940eadca0986427402eb688a4d|200|1|ok|OK|
|GET|/api/metaid/followerList/0d166d6c6e2ac2f839fb63e22bd93ed571fc06940eadca0986427402eb688a4d?cursor=0&size=10|200|1|ok|OK|
|GET|/api/metaid/followingList/0d166d6c6e2ac2f839fb63e22bd93ed571fc06940eadca0986427402eb688a4d?cursor=0&size=10|200|1|ok|OK|
|GET|/api/metaid/recommended?limit=10&num=6|200|1|ok|OK|
|GET|/api/notifcation/list?address=12ghVWG1yAgNjzXj4mr3qK9DgyornMUikZ&lastId=0|404|||Not Found|
|GET|/api/dict/get?key=test|404|||Not Found|
|GET|/api/block/file/partCount?height=0&chain=btc|404|||Not Found|
|GET|/api/access/getPubKey|200|1|ok|OK|
|GET|/api/access/getControlByContentPin?pinId=14bdb4797d8874826a1e0895c8014aca2084ba8177c74b5b963ddb4945952505i0|200|404|no data|OK (business error)|
|GET|/api/mrc20/tick/all?cursor=0&size=10|200|1|ok|OK|
|GET|/api/mrc20/tick/info/0|200|1|ok|OK|
|GET|/api/mrc20/tick/info?id=0&tick=IOP|200|1|ok|OK|
|GET|/api/mrc20/tick/address?cursor=0&size=10&tickId=0&address=12ghVWG1yAgNjzXj4mr3qK9DgyornMUikZ|200|100|no data found.|OK (business error)|
|GET|/api/mrc20/tick/history?cursor=0&size=10&tickId=0|200|1|ok|OK|
|GET|/api/mrc20/address/balance/12ghVWG1yAgNjzXj4mr3qK9DgyornMUikZ?cursor=0&size=10|200|1|ok|OK|
|GET|/api/mrc20/tx/history?cursor=0&size=10&index=0&txId=0000000000000000000000000000000000000000000000000000000000000000|200|100|no data found.|OK (business error)|
|GET|/api/mrc20/address/shovel/list?cursor=0&size=10&tickId=0&address=12ghVWG1yAgNjzXj4mr3qK9DgyornMUikZ|000000|||Timeout|
|GET|/api/mrc20/shovel/used?cursor=0&size=10&tickId=0&address=12ghVWG1yAgNjzXj4mr3qK9DgyornMUikZ|000000|||Timeout|
|GET|/api/mrc20/tick/AddressBalance?address=12ghVWG1yAgNjzXj4mr3qK9DgyornMUikZ&tickId=0|404|||Not Found|
|GET|/api/mrc721/collection/pageList?cousor=0&size=10|200|1|ok|OK|
|GET|/api/mrc721/collection/info?name=drops genesis card&pinId=a7288d8ad5b6b024e15cbdb520b7009600245869850abb4515a2b90b0fd55933i0|000|||Timeout|
|GET|/api/mrc721/collection/items/pageList?cousor=0&size=10&pinid=a7288d8ad5b6b024e15cbdb520b7009600245869850abb4515a2b90b0fd55933i0|200|1|ok|OK|
|GET|/api/mrc721/address/collection?cousor=0&size=10&address=12ghVWG1yAgNjzXj4mr3qK9DgyornMUikZ|200|100|no data found.|OK (business error)|
|GET|/api/mrc721/address/item?cousor=0&size=10&address=12ghVWG1yAgNjzXj4mr3qK9DgyornMUikZ|200|100|no data found.|OK (business error)|
|GET|/api/mrc721/item/info?pinId=a7288d8ad5b6b024e15cbdb520b7009600245869850abb4515a2b90b0fd55933i0|200|100|no data found.|OK (business error)|
|GET|/api/metaname/list?size=10|404|||Not Found|
|GET|/api/metaname/info?name=|404|||Not Found|
|GET|/social/buzz/newest?size=10|000|1|ok|Timeout|
|GET|/social/buzz/recommended?size=10&userAddress=12ghVWG1yAgNjzXj4mr3qK9DgyornMUikZ|000|||Timeout|
|GET|/social/buzz/updater|200|-1|Get "http://host.docker.internal:7171/api/lastVersion": dial tcp 172.17.0.1:7171: connect: connection refused|OK (business error)|
|GET|/social/buzz/hot?size=10|200|1|ok|OK|
|GET|/social/buzz/search?size=10&key=btc|200|1|ok|OK|
|GET|/social/buzz/info?pinId=14bdb4797d8874826a1e0895c8014aca2084ba8177c74b5b963ddb4945952505i0|200|1|ok|OK|
|GET|/social/buzz/follow?metaid=0d166d6c6e2ac2f839fb63e22bd93ed571fc06940eadca0986427402eb688a4d|200|1|ok|OK|
|GET|/host/block/sync-newest|200|1|ok|OK|
|GET|/host/block/ndv?height=940738&cursor=0&size=10|200|1|ok|OK|
|GET|/host/block/mdv?height=940738&cursor=0&size=10|200|1|ok|OK|
|GET|/host/info?host=&cursor=0&size=10|200|1|ok|OK|
|GET|/ft/mrc20/address/deploy-list?address=12ghVWG1yAgNjzXj4mr3qK9DgyornMUikZ|200|1|ok|OK|
|GET|/metaso/settings/blocked/list?blockType=host&cursor=0&size=10|200|1|ok|OK|
|GET|/metaso/settings/recommended/list?cursor=0&size=10|200|1|ok|OK|
|GET|/statistics/host/metablock/sync-newest|200|1|ok|OK|
|GET|/statistics/ndv?cursor=0&size=10|200|1|ok|OK|
|GET|/statistics/mdv?cursor=0&size=10|200|1|ok|OK|
|GET|/statistics/host/metablock/info?height=336&cursor=0&size=10|200|1|ok|OK|
|GET|/statistics/metablock/address/info?height=336&cursor=0&size=10|200|1|ok|OK|
|GET|/statistics/metablock/host/value?cursor=0&size=10|200|1|ok|OK|
|GET|/statistics/metablock/host/address/list?cursor=0&size=10|200|1|ok|OK|
|GET|/statistics/metablock/host/address/value?cursor=0&size=10|200|1|ok|OK|
