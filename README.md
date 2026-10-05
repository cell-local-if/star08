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

### `HEAD /v1/blobs/{digest}`
`200` + `X-Blob-Digest`/`Content-Length`/`Content-Type`，无体；未知 ⇒ `404`。

### `GET /v1/blobs`
`200 {"blobs": [{"digest","size","media_type","refs"}...（按 digest 字典序）], "stats": {"blobs","bytes","puts"}}`

可选查询参数（全部同时生效，任一启用时响应增加 `next_cursor` 字段）：

- `digest_prefix`：1–64 位小写十六进制，按摘要前缀过滤。
- `min_size` / `max_size` / `min_refs`：十进制非负整数；两个大小值上限均为 1048576，且 `min_size` 不得大于 `max_size`；`min_refs` 按 refs 值筛选。
- `limit`：1–100，限制本页条数。
- `after`：64 位小写十六进制游标，仅返回严格排在其后的记录（按 digest 字典序），**必须与 `limit` 同时出现**；不是页码或数组位置。

`next_cursor`：有后续记录时为本页最后一条 digest，否则为 `null`。`stats` 始终描述全部 blob，不受过滤与分页影响。单次请求基于同一时刻的元数据快照生成完整结果。未知或重复参数、缺失或非法值 ⇒ `400 invalid_request`。

### `POST /v1/uploads`
创建可恢复上传会话（会话只存于进程内存，重启即丢失）。

- 体 = JSON 对象：`{"size": <1..1048576 的整数>, "media_type": "<≤200 字符的字符串>", "digest": "<可选，64 位小写十六进制>"}`，无其他字段。
- 成功：`201 {"upload_id": "<32 位小写十六进制>", "size": ..., "received": 0, "status": "uncommitted"}`。
- JSON 非法、字段缺失或未知、值越界 ⇒ `400 invalid_request`。

### `PUT /v1/uploads/{upload_id}`
追加一个分片（体 = 原始字节）。

- 必须带 `X-Upload-Offset: <十进制非负整数>`；分片非空且 ≤ 262144 字节。
- 仅当 `offset == received` 时追加，成功 `200 {"received": <新值>, "status": "uncommitted"}`。
- offset 缺失或非法、分片为空或超限 ⇒ `400`；offset 与 received 不一致、分片越过声明 size、会话已 committed ⇒ `409 conflict`。

### `GET /v1/uploads/{upload_id}`
`200 {"size", "received", "media_type", "status", "digest"?}`（digest 仅在创建时声明过才出现），供断点续传查询进度。

### `DELETE /v1/uploads/{upload_id}`
`204`，此后同 id 的所有操作返回 `404`。会话已 committed ⇒ `409 conflict`。

### `POST /v1/uploads/{upload_id}/complete`
仅在 `received == size` 时计算 SHA-256 并提交：

- 声明了 digest 且不匹配 ⇒ `409 conflict`，**不建 blob**，会话保持 uncommitted。
- 成功：`201 {"digest", "size", "media_type"}`；相同字节只存一份、refs +1；会话状态变为 `committed`。
- 未收齐、重复 complete ⇒ `409`；committed 会话上的写入与删除同样 ⇒ `409`。

`upload_id` 非 32 位小写十六进制 ⇒ `400 invalid_request`；未知或已删除的会话 ⇒ `404 not_found`。

## 错误语义

```json
{"error": {"code": "invalid_request|not_found|conflict|internal_error", "message": "<可读说明>"}}
```

优先级：`Content-Length` 校验先于读体；路由不匹配先于体校验；`invalid_request` 先于 `not_found` 先于 `conflict`。并发写同一会话时，每个请求原子地成功或返回上述 conflict，不会留下重叠、空洞或撕裂数据。

## 未实现（后续任务候选，非固定题单）

分块与增量去重、垃圾回收与引用策略、签名与信任链、镜像同步、依赖图与版本约束求解、
会话持久化与跨进程共享、审计与可观测性。
