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

不带查询参数时返回全部 blob。可选查询参数（可同时使用，过滤条件同时生效）：

- `digest_prefix`：1–64 位小写十六进制，按 digest 前缀过滤。
- `min_size` / `max_size`：十进制非负整数，上限 1048576，且 `min_size ≤ max_size` 不得倒置。
- `min_refs`：十进制非负整数，按 refs 下限过滤。
- `limit`：1–100，限制本页条数。
- `after`：64 位小写十六进制游标，只返回严格排在其后的记录；**必须与 `limit` 同时出现**。

启用任一参数时，响应增加 `next_cursor`：还有后续记录时为本页最后一条 digest，否则为 `null`。
`stats` 始终描述全部 blob，不受过滤与分页影响。未知/重复参数、缺失或非法值 ⇒ `400 invalid_request`。

## 错误语义

```json
{"error": {"code": "invalid_request|not_found|conflict|internal_error", "message": "<可读说明>"}}
```

优先级：`Content-Length` 校验先于读体；路由不匹配先于体校验；`invalid_request` 先于 `conflict`。

## 未实现（后续任务候选，非固定题单）

分块与增量去重、上传会话与断点续传、垃圾回收与引用策略、签名与信任链、镜像同步、依赖图与版本约束求解、
并发上传一致性、审计与可观测性。
