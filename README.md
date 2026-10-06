# Artifact Store — 公开契约（baseline）

**内容寻址**的制品仓库：blob 以其字节的 SHA-256 寻址，相同字节只存一份；本次基线只实现最小可用子集。

## 运行

```bash
PYTHONPATH=src python3 -m artifacts.app --port 18895
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

Python 3.12，**仅标准库**；`127.0.0.1`，端口由 `--port` 指定；单个 blob 上限 **1 MiB**；状态在进程内存中。

## 接口

### `GET /health`
`200 {"status":"ok"}`

### `PUT /v1/blobs`
- 体 = **原始字节**（不是 JSON）。
- 可选头：`X-Blob-Digest: <64 位小写十六进制>`（声明摘要，用于完整性校验）、`Content-Type`（缺省 `application/octet-stream`）。
- 成功：`201`，响应头带 `X-Blob-Digest`，体 `{"digest":..., "size":..., "media_type":...}`。**相同字节重复 PUT 不新增存储**（引用计数 +1）。
- 声明摘要与实际不符 ⇒ `409 conflict`（**不写入**）；空体、超 1 MiB、摘要格式非法 ⇒ `400 invalid_request`。

### `GET /v1/blobs/{digest}`
返回原始字节（`Content-Type` 为存入时的 media type）。未知摘要 ⇒ `404`；摘要格式非法 ⇒ `400`。

不带 `Range` 时返回 `200`、完整原始字节及准确 `Content-Length`，并带：

- `ETag: "<digest>"`（64 位小写十六进制摘要的强引号实体标签）。
- `Accept-Ranges: bytes`。

`Range` 只接受**单个 bytes 闭区间**，从零开始计数，合法形式为 `bytes=first-last`、`bytes=first-`、`bytes=-suffix`（边界均为十进制非负整数；`suffix` 表示末尾 N 字节且不能为 0）。合法请求返回 `206`：

- 响应体为对应闭区间字节，`Content-Length` 为实际返回字节数；末尾超出 blob 时 `last` 截断到 size−1，`-suffix` 大于等于整长时返回整份。
- `Content-Range: bytes first-last/size`、`X-Blob-Digest: <digest>`，并同样带 `ETag`、`Accept-Ranges` 与存入时的 `Content-Type`。

以下情况**不得**返回部分内容：首尾都省略（`bytes=-`）、`last < first`、`first >= size`（`bytes=size-` 同）、`suffix` 为 0、多个区间（含逗号）、重复 `Range` 头、非 `bytes` 单位（大小写敏感）、含空白或非法十进制边界。其中**格式合法但与 blob 无法相交**（`first >= size` 或 `last < first`）⇒ `416`，带 `Content-Range: bytes */size` 与 `{"error":{"code":"conflict","message":...}}`；其余格式问题 ⇒ `400 invalid_request`。

`If-Range` 只接受与**当前 digest 对应**的引号 ETag（`"<64 位小写十六进制>"`，不接受日期、弱标签或其他形式）：匹配时按 `Range` 截取；不匹配时**忽略 Range** 返回完整 `200`（即使该 Range 本身会 416）。`If-Range` 格式非法 ⇒ `400 invalid_request`。

该入口只读：不改变摘要、media type、refs 或回收时机。响应字节在请求开始时一次取得；读取期间该 blob 被释放并回收，已在途的请求仍返回同一份字节，之后新请求才得到 `404`。

### `HEAD /v1/blobs/{digest}`
`200` + `X-Blob-Digest`/`Content-Length`/`Content-Type`/`ETag`/`Accept-Ranges`（与 GET 报告同一组摘要、大小与 media type），无体；未知 ⇒ `404`。

### `GET /v1/blobs`
`200 {"blobs": [{"digest","size","media_type","refs"}...（按 digest 字典序）], "stats": {"blobs","bytes","puts"}}`

可选查询参数（全部同时生效，任一启用时响应增加 `next_cursor` 字段）：

- `digest_prefix`：1–64 位小写十六进制，按摘要前缀过滤。
- `min_size` / `max_size` / `min_refs`：十进制非负整数；两个大小值上限均为 1048576，且 `min_size` 不得大于 `max_size`；`min_refs` 按 refs 值筛选。
- `limit`：1–100，限制本页条数。
- `after`：64 位小写十六进制游标，仅返回严格排在其后的记录（按 digest 字典序），**必须与 `limit` 同时出现**；不是页码或数组位置。

`next_cursor`：有后续记录时为本页最后一条 digest，否则为 `null`。`stats` 始终描述全部 blob，不受过滤与分页影响。单次请求基于同一时刻的元数据快照生成完整结果。未知或重复参数、缺失或非法值 ⇒ `400 invalid_request`。

### `DELETE /v1/blobs/{digest}/refs`
显式释放该 digest 的**一个**引用：

- 成功：`200 {"digest":..., "refs":<剩余引用数>}`。refs > 1 时只减一；从 1 减到 0 时**内容仍保留**——回收前 GET/HEAD 照常命中，列表中该 blob 显示 `refs: 0`。
- digest 格式非法 ⇒ `400 invalid_request`；digest 不存在 ⇒ `404 not_found`；refs 已为 0 再释放 ⇒ `409 conflict`（计数不变）。

### `GET /v1/blobs/{digest}/graph`
把 `{digest}` 指向的 blob 当作**依赖清单**解析，并按依赖 digest 递归读取，导出从根清单可达的完整依赖图。

清单是普通的 blob（经 `PUT /v1/blobs` 或上传会话 complete 入库，无专用存储、无自动引用），内容为 **UTF-8 JSON 对象**，只允许以下字段：

- `name` / `version`：非空字符串，至多 100 字符。
- `dependencies`：可选对象；键为依赖名称，值为对象且只允许 `digest`（64 位小写十六进制 SHA-256）与 `constraint`（非空字符串，至多 200 字符）。

成功：`200 {"root":<根 digest>, "nodes":[...], "edges":[...]}`，**只含这三个字段**：

- `nodes`：按 digest 字典序；每个节点 `{"digest","name","version"}`。多条路径到达同一清单时节点只出现一次。
- `edges`：按 `from`、`to`、`name` 字典序；每条边 `{"from","to","name","constraint"}`，重复边去重。无依赖时只有根节点且 `edges` 为空。

整次查询基于请求开始时的**一致只读快照**输出完整结果：不增加 refs、不改变回收时机，中途发生的 GC 不影响本次输出。

错误：根 digest 格式非法 ⇒ `400 invalid_request`；根 blob 不存在 ⇒ `404 not_found`；根或任一可达 blob 不是合法清单（非 UTF-8 JSON 对象、字段或值违反上述约束）、依赖 blob 不存在、依赖成环 ⇒ `409 conflict`，且**不返回部分图**。

### `GET /v1/blobs/{digest}/resolve`
在与 graph 相同的请求开始时**一致只读快照**上遍历从根清单可达的全部清单，但不止于展开：还按依赖名合并所有边上的版本约束、求交集，并核对承载该名称的唯一清单版本。不写入 blob，不改变 refs、GC 与读取一致性；graph 入口及对其 `constraint`/`version` 的校验均不收紧。

**版本语法**：一到三段以点分隔的十进制非负整数；不足三段右侧补 0 后比较；除单独的 `0` 外禁止前导零（如 `1`、`1.2`、`1.2.0` 等价表示，但 `01`、`1.02` 非法）。

**约束语法**：值以 ASCII 空白（空格、制表、换行、回车、竖向制表、换页）分隔为若干 token；每个 token 仅接受 `=V`、`>V`、`>=V`、`<V`、`<=V`、`^V`、`~V`（`V` 为上述一到三段版本）。比较一律使用补齐后的三段数值：

- `^V`：`major > 0` ⇒ `[V, major+1.0.0)`；`0.minor` 且 `minor > 0` ⇒ `[V, 0.minor+1.0)`；`0.0.patch` ⇒ `[V, 0.0.patch+1)`。
- `~V`：一段 `V` ⇒ `[V, major+1.0.0)`；两段或三段 `V` ⇒ `[V, major.minor+1.0)`。
- `=V` 仅含补齐后的 `V` 一点；`>V`/`>=V`/`<V`/`<=V` 按数值半开/闭界常规处理。

同名约束（来自同一名称的所有边，即使分散在菱形依赖的不同路径上）取交集；仅当交集非空**且**目标版本落在交集内才成功。

成功：`200 {"root":<根 digest>, "resolved":[...]}`，**只含这两个字段**：

- `resolved` 按 `name` 字典序排列，每个名称一项；每项 `{"name","digest","version","constraints"}`。
- 依赖键即目标 `name`：同一名称在整次解析中只允许指向一个 digest。
- `version` 为目标清单自报的原始版本字符串；`constraints` 为该名称上所有**去重后**的原始 constraint 字符串，按字典序排列（多 token 字符串作为整体参与去重与排序）。
- 根清单无依赖时 `resolved` 为空数组；根自身的 `version` 不参与裁决。

错误（HTTP 状态与 code 同 graph；以下 message 均以给定前缀开头）：

- 根 digest 格式非法 ⇒ `400 invalid_request`；根 blob 不存在 ⇒ `404 not_found`。
- 清单结构非法、依赖 blob 不存在 ⇒ `409 conflict`，沿用 graph 的 `is not a valid manifest` / `dependency blob ... does not exist` 文案。
- `409 conflict` 且 message 以 `cycle`、`version_syntax`、`constraint_syntax`、`name_mismatch`、`ambiguous_name`、`empty_intersection`、`version_mismatch` 开头，分别表示：依赖成环；目标清单 `version` 语法错；约束 token 语法错；依赖键与目标清单 `name` 不一致；同名依赖指向多个 digest；同名约束交集为空；交集非空但目标版本不在其中。
- 任何错误都**不返回部分 resolved**。

### `POST /v1/gc`
按需垃圾回收：请求体不参与回收结果。**原子**删除执行时刻 `refs == 0` 的所有 blob，返回 `200 {"deleted": [...], "stats": {"blobs","bytes","puts"}}`：

- `deleted` 为被删 digest，按字典序排列；无内容可删时为空数组，仍返回 `200`。
- `stats` 口径与 `GET /v1/blobs` 无参数时一致，描述回收后的全部 blob。
- `refs > 0` 的内容绝不被回收；`refs == 0` 的内容回收前 GET/HEAD 仍命中，回收后 `404`；回收后再次 PUT 同一字节按新 blob 重建且 `refs = 1`。

同一 digest 上并发的 PUT、上传会话 complete、释放与回收按请求逐个原子裁决，不丢失引用更新：无论交错顺序，最终 refs 等于尚未释放的写入次数。回收只在调用 `POST /v1/gc` 时发生，不自动定时执行。

## 可恢复上传会话

把不超过 1 MiB 的 blob 以**每片非空且不超过 262144 字节（256 KiB）**的原始分片上传，支持断点续传。会话仅存于当前进程内存，重启不恢复。

### `POST /v1/uploads`
创建会话，JSON 体：

- `size`：**必填**整数，`1..1048576`（不接受浮点、布尔、字符串）。
- `media_type`：**必填**非空字符串，至多 200 字符。
- `digest`：可选，64 位小写十六进制 SHA-256，作为完整性声明。

成功：`201 {"upload_id","size","received":0,"status":"uncommitted"}`；`upload_id` 为 32 位小写十六进制串，进程内唯一且不复用。非法 JSON、缺失/非法字段或未知字段 ⇒ `400 invalid_request`。

### `PUT /v1/uploads/{upload_id}`
追加一个原始分片：

- 必须带头 `X-Upload-Offset: <十进制非负整数>`，值须等于当前 `received`。
- 分片非空且 ≤ 262144 字节；追加后不得越过会话 `size`。
- 成功：`200 {"received":<新偏移>,"status":"uncommitted"}`。
- offset 缺失/非十进制、分片为空或超 256 KiB ⇒ `400 invalid_request`；offset 与 `received` 不一致或越过 `size` ⇒ `409 conflict`（该字节不写入）。

### `GET /v1/uploads/{upload_id}`
`200 {"size","received","media_type","status",["digest":声明值]}`，供客户端决定续传偏移。`digest` 仅在创建时声明（或已提交得到实际值）时出现。

### `DELETE /v1/uploads/{upload_id}`
`204` 放弃会话；此后同一 id 对 GET/PUT/DELETE/complete 一律 `404 not_found`（id 不复用）。对已提交会话删除 ⇒ `409 conflict`。

### `POST /v1/uploads/{upload_id}/complete`
- 仅当 `received == size`：计算整体 SHA-256。声明 `digest` 不匹配 ⇒ `409 conflict`，**不建 blob**，会话保持 `uncommitted` 可续传或删除。
- 匹配（或未声明）：`201 {"digest","size","media_type"}`，blob 进入内容寻址存储（相同字节只存一份、refs +1），会话状态变 `committed`。
- 未收齐、重复 complete、或会话已删除 ⇒ `409 conflict` / `404 not_found`。

已提交会话仍可 `GET`（`status:"committed"`, `digest` 为实际值），但对其写入或删除 ⇒ `409 conflict`。

`upload_id` 格式非法（非 32 位小写十六进制）在上述四个动词上一律 `400 invalid_request`；格式合法但未知/已删除 ⇒ `404 not_found`。同一会话的并发写按请求逐个**原子**裁决：恰有一个请求成功，其余得到 `conflict`/`not_found`，不产生重叠、空洞或撕裂字节。

## 错误语义

```json
{"error": {"code": "invalid_request|not_found|conflict|internal_error", "message": "<可读说明>"}}
```

优先级：`Content-Length` 校验先于读体；路由不匹配先于体校验；`invalid_request` 先于 `conflict`（如分片本身非法先于 offset 冲突，`upload_id` 格式非法先于会话查找）。

## 未实现（后续任务候选，非固定题单）

增量去重、签名与信任链、镜像同步、审计与可观测性、上传会话跨进程持久化与重启恢复。
