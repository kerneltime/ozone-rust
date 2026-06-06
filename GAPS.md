# Known Gaps and Limitations

Status of this document: living gap register for the Rust S3 gateway + datanode.
Produced from a five-reviewer adversarial audit (2026-06-06) plus direct code
reads. It exists so a reader does not mistake "the test suite is green" for "the
system is complete and correct." Read it before claiming any behavior is done.

## How to read this

- **Severity**: HIGH = silent data corruption / integrity hole reachable in
  normal use; MAJOR = a real S3 client or a real OM/SCM would hit wrong behavior;
  MINOR = cosmetic, rare, or polish.
- **State**: OPEN (not started), DOC (behavior is intentional but must be
  documented/guarded), DONE (fixed; links the commit).
- Line numbers are indicative as of 2026-06-06 and may drift; the symbol names
  and described behavior are the load-bearing part. Every claim below was either
  read directly this session or cited with file:line by a reviewer and then
  spot-verified by grep (see the "Verification" note per item where relevant).

## Scope context (what is intentionally NOT here)

Greenfield Rust reimplementation. ONLY erasure-coded data + OBS buckets. A secure
upstream proxy owns all S3 authentication — the gateway trusts a proxy-attested
principal and does **not** verify SigV4 (this is by design, never a finding).
Out of scope by design: ACLs, bucket policies, versioning, lifecycle, SSE,
server-side request signing. The e2e tests use an in-memory `FakeOm` and a
`FakeScm`; there is no real Ozone Manager or SCM in any test.

## The meta-caveat: FakeOm masks the gaps it shares

The single most important thing a fresh reviewer learns: **`FakeOm` is forgiving
in exactly the dimensions where the gateway is loose.** It re-sorts multipart
parts (`fake_om.rs:610`), ignores `max_keys`/`continuation_token` and never
truncates listings, performs the prefix/delimiter folding itself, and holds no
multipart part state. So the green suite gives *false confidence* on multipart
ordering, list pagination, and per-key error mapping — the very areas with open
defects below. Fixing a Tier-A item therefore usually means **also tightening
FakeOm** so the regression test can actually fail before the fix.

---

## Verified-solid (do NOT re-investigate; proven this session)

These are genuinely proven and should not be re-litigated:

- **EC encode/decode incl. partial/empty stripes** — `crates/ozone-ec/src/stripe.rs`.
  `cell_len`/`padded_cell` handle the trailing partial stripe; a 150-case proptest
  (`stripe.rs:345-399`) proves byte-exact recovery for random sizes 0–1500 across
  RS-3-2/6-3/10-4 under up to `p` random erasures, plus explicit edge lengths
  [0,5,24,25,48,50,100] and max-`p`-drop cases. VERIFIED by direct read.
- **Java GF-matrix byte-equivalence** — `isa-l-safe` matrix test +
  `isa-l-safe/tests/golden_vectors.rs`. CAVEAT: equivalence is asserted for the
  Cauchy GF matrix and full cells; the *trailing partial-stripe parity on-disk
  layout* is intentionally NOT asserted byte-identical to Ozone Java
  (`stripe.rs:20-25`). "Byte-identical to Java" must always carry this caveat.
- **ETag algorithm** — single PUT = `md5_hex(body)`; multipart =
  `hex(md5(concat(binary part md5s)))-N`, computed AWS-identically
  (`backend.rs` + `fake_om.rs:610-623`). Header ETags quoted; GetObjectAttributes
  ETag unquoted. VERIFIED by reviewer reading both sides.
- **aws-chunked de-framing** incl. multi-chunk SigV4-signed bodies, proven against
  the real `aws-sdk-s3` and a 1 MiB chunked PUT.
- **Object lifecycle** PUT/GET/HEAD/DELETE exact-byte + ETag through real
  datanodes + real EC; single-failure degraded read end-to-end.
- **Tagging** (subresource + `x-amz-tagging` header) and **GetObjectAttributes**
  (unquoted ETag, multi-line `x-amz-object-attributes` selector via `get_all`).
- **Not-found codes** incl. the easy-to-miss `404 NoSuchUpload`.
- **Code hygiene**: no `todo!`/`unimplemented!`/`dead_code` in live paths; the two
  `Status::unimplemented` arms (PutECStripe/ReadECStripe) are documented-intentional.

---

## Tier A — in-scope defects to fix now

Each is cheap, within the stated scope, and (critically) needs a test that can
*fail* — which often means making FakeOm stricter first.

### A1 [HIGH] Read path never verifies shard integrity — DONE (528c96e)
- **Where**: `backend.rs:650` — `read_chunk(&block_slot, &chunk, false)` with
  `chunk.checksum_data = None`. Verified: the only read_chunk call passes
  `verify=false`.
- **Now**: a silently corrupted shard on disk is EC-decoded as if valid; GET
  returns corrupt bytes with no error. The datanode *can* verify (proven on the
  write/ingress path) but the gateway never asks.
- **Fix**: on read, request verification (or verify in the gateway against stored
  CRC); on `DataLoss`, treat that shard as **missing** and let EC reconstruct from
  survivors. Corruption then degrades to a reconstruct, not a silent bad read.
- **Test**: flip a byte in one on-disk shard file, GET still returns exact bytes
  (reconstructed), and a corrupted-beyond-recovery case errors instead of lying.

### A2 [MAJOR] Multipart Complete does not validate part order / duplicates — DONE (this branch)
- **Where**: `backend.rs` `complete_multipart` + `lib.rs` `parse_complete_parts`
  (preserves client document order). Masked by `fake_om.rs:610` `sort_by_key`.
- **Now**: out-of-order parts are silently sorted into order by the OM instead of
  rejected; duplicate part numbers pass; supplied ETags are ignored. Against a
  real OM that concatenates in given order, this can mis-assemble the object.
- **Fix**: in `complete_multipart`, require strictly ascending, unique part
  numbers; else `InvalidPartOrder` (400). Optionally validate supplied ETags
  against stored part ETags (`InvalidPart`). **Also remove the `sort_by_key` from
  FakeOm** so the gateway, not the fake, owns ordering — otherwise the test cannot
  fail.
- **Test**: complete with `[2,1]` → 400 InvalidPartOrder; `[1,1]` → 400.

### A3 [MAJOR] Unsatisfiable Range returns 200 + full body instead of 416 — DONE (this branch)
- **Where**: `lib.rs` `parse_range` returns `None` for BOTH "no bytes= prefix" and
  "valid syntax but unsatisfiable"; the GET handler serves the full object on
  `None`.
- **Now**: `bytes=200-300` on a 100-byte object → `200` with the whole object.
- **Fix**: distinguish absent (serve full, 200) from unsatisfiable (416 +
  `Content-Range: bytes */{total}`). Likely a 3-state return from `parse_range`.
- **Test**: `bytes=200-300` on a 100-byte object → 416 with `Content-Range`.

### A4 [MAJOR] `max-keys` unenforced; listing stream over-drained — DONE (this branch)
- **Where**: `backend.rs` `list_objects` drains the entire `list_keys` stream into
  one page (overwriting `is_truncated`/`next_continuation_token` with the last
  message's values); `lib.rs` parses `max-keys` and forwards it without capping.
  FakeOm never truncates, so this path is currently untestable.
- **Now**: the gateway adds no cap of its own; a real multi-page OM stream would
  be concatenated and emit a wrong cursor; `max-keys=0` passes through;
  unparseable `max-keys` silently defaults to 1000 rather than 400.
- **Fix**: cap `contents` at `max_keys` at the gateway, set `is_truncated` and a
  correct `NextContinuationToken`; validate `max-keys` (0..=1000) at the boundary.
  **Tighten FakeOm to actually paginate/truncate** so the test can fail.
- **Test**: put 5 keys, `max-keys=2` → 2 keys + `IsTruncated=true` + token; follow
  the token → remaining keys, no overlap.

### A5 [MAJOR] CopyObject ignores metadata/tagging directives; no self-copy guard — DONE (this branch)
- **Where**: `backend.rs` `copy_object` always does a reference copy that clones
  source metadata+tags; `lib.rs` copy branch reads neither directive.
- **Now**: `x-amz-metadata-directive=REPLACE` / `x-amz-tagging-directive=REPLACE`
  are silently treated as COPY, so the common "copy onto self to rewrite
  Content-Type/metadata" idiom is a silent no-op; a pure-COPY self-copy is not
  rejected.
- **Fix**: read both directives (default COPY). On REPLACE, apply the request's
  `x-amz-meta-*`/Content-Type/tags and drop the source's. Reject self-copy unless
  something is being replaced.
- **Test**: self-copy with REPLACE changes Content-Type; self-copy pure COPY →
  400 InvalidRequest.

### A6 [MAJOR] DeleteObjects: wrong per-key codes, no quiet mode, no 1000 cap — DONE (this branch)
- **Where**: `lib.rs` `delete_result_xml` hardcodes `<Code>InternalError</Code>`;
  `parse_delete_request` ignores `<Quiet>`; no batch-size check.
- **Now**: a real per-key failure (e.g. NoSuchBucket mid-batch) reports as
  `InternalError`; quiet mode still returns every `<Deleted>`; >1000 keys are
  processed instead of rejected.
- **Fix**: map `GatewayError` → S3 code per key (reuse the `error_response`
  match); honor `<Quiet>true</Quiet>` (emit only errors); reject >1000 with
  `MalformedXML`.
- **Test**: quiet batch of existing keys → empty `<DeleteResult>`; 1001 keys → 400.

### A7 [MAJOR] `If-Modified-Since` / `If-Unmodified-Since` ignored — DONE (this branch)
- **Where**: neither header is read anywhere in `src/` (grep-confirmed). Only
  If-Match/If-None-Match are evaluated.
- **Now**: date-conditional GET/HEAD always returns 200 — broken cache
  revalidation for browsers/CDNs/`curl -z`.
- **Fix**: parse both date headers (needs an HTTP-date *parser*, the inverse of
  the existing `http_date`); fold into `precondition_status` (304 / 412) with the
  RFC 7232 precedence (If-Match beats If-Unmodified-Since, etc.).
- **Test**: `If-Modified-Since` in the future → 304; `If-Unmodified-Since` in the
  past → 412.

### A8 [MINOR] Multipart: no min part size, no part-number range — OPEN
- **Where**: `upload_part` takes any `u32` part number and any size; `lib.rs` only
  checks the number parses.
- **Now**: `partNumber=0` or `4_000_000_000` is accepted; a <5 MiB non-final part
  completes instead of `EntityTooSmall`.
- **Fix**: enforce part number 1..=10000 on upload; on complete, reject if any
  non-last part < 5 MiB (`EntityTooSmall`).
- **Test**: `partNumber=0` → 400; tiny non-final part → complete 400.

### A9 [MINOR] Error bodies thin; all input errors collapse to `InvalidRequest` — OPEN
- **Where**: `error_response` emits only `<Code>`/`<Message>` (no `RequestId`/
  `Resource`); every `BadRequest` serializes as `InvalidRequest`/400.
- **Now**: SDK control flow still works (Code is parsed), but specific codes
  (`InvalidTag`, `MalformedXML`, `InvalidPartOrder`, `EntityTooSmall`) are not
  surfaced and there is no server correlation id.
- **Fix**: add a typed error-code path (carry an S3 code on `BadRequest`
  variants), emit `<RequestId>` + `<Resource>`. Lower priority; do alongside
  A2/A6/A8 which need the specific codes anyway.

---

## Tier B — facades / inert plumbing (fix or explicitly document)

These are real code with real gaps that would let a reader believe a behavior is
backed when it is not. Each must be either wired or clearly annotated.

### B1 [MAJOR-misleading] `block_token` is fully inert — OPEN
- **Verified**: grep finds no non-empty write and no read outside tests — it is
  only ever `Vec::new()` (`backend.rs`, `fake_om.rs`).
- **Now**: the proto comment says "Opaque token issued by SCM; gateway forwards
  verbatim to the DN," but the gateway never forwards it, the DN protocol has no
  field to receive it, and the DN validates nothing. Implies a
  capacity/security control that does not exist end-to-end.
- **Action (now)**: correct the proto comment to state it is reserved/unused in
  this slice. **Action (security milestone)**: wire SCM-issued token →
  gateway → DN + DN validation.

### B2 [MAJOR] Multi-block write path wired but never called; no size guard — OPEN
- **Verified**: gateway never calls `allocate_block` (grep). `allocate_and_write`
  writes exactly one block group; `get_object` reads `info.locations` which a
  simple PUT only populates with one entry.
- **Now**: a PUT larger than a block is encoded into one oversized block group
  with no `EntityTooLarge` rejection. Documented as "later extension"
  (`backend.rs:9-11`) but **unguarded**.
- **Action (now)**: add a max-object-size guard that returns a clear error until
  multi-block is implemented. **Action (later)**: implement the `allocate_block`
  loop + multi-block read assembly.

### B3 [MAJOR] SCM integration is partial — OPEN
- **Verified**: datanode never calls `container_report` (grep). SCM command
  handler acts on only 2 of 10 oneof variants; the SCM-driven `DeleteContainer`
  deletes metadata only (leaks chunk bytes) while no GC/reclaimer exists; the
  heartbeat `NodeReport` is a fixed stub (`capacity_bytes: 0`).
- **Now**: a real SCM would never learn which containers a Rust DN holds; SCM
  container deletion orphans chunk files; no replication/reconstruction/decommission
  is driven.
- **Action**: send FULL/INCREMENTAL container reports; make the SCM delete path
  also drop chunks; populate a real `NodeReport`; document the unhandled commands
  and the absent reclaimer.

### B4 [MINOR] Inert plumbing kept "consistent for the future" — OPEN/DOC
- `stripe_checksum`, `eof`, and `blockGroupLen` are written/stored but never read
  on any live path (the read path uses `loc.length` from OM). `CreateKey.metadata`
  is always sent empty (user metadata rides on `CommitKey` instead).
  `expected_size` and `exclude_dn_uuids` are set but ignored.
- **Action**: either consume these or annotate them in the proto as
  reserved-for-future so a reader does not assume they are load-bearing.

---

## Tier C — deferred by design (real-cluster integration; larger efforts)

Not bugs against the current scope, but the "validated" claim must be qualified
to acknowledge them. Accurate one-line status: *"S3 SDK surface + erasure-coded
data path validated against a fake in-memory OM, single gateway instance,
single-failure degraded read."*

- **C1** No real OM and no real SCM in any test; the two control planes are tested
  against *disjoint* fakes and never wired together. No full-topology test.
- **C2** Multipart state is in-process (`Gateway.mpu` map): lost on gateway
  restart, broken across replicas (Complete on a different instance → 404). Needs
  part records persisted in OM.
- **C3** Durability/persistence/crash-consistency unproven (FakeOm is in-memory;
  no fsync/flush story validated end-to-end).
- **C4** Degraded read proven end-to-end for only a single failure; max-`p` and
  parity-only-survivor recovery proven only at the pure-EC layer, not through the
  gateway+DN wire path.
- **C5** No GC/reclaimer: orphaned blocks from multipart abort and SCM-deleted
  containers accumulate forever (referenced in comments as if it exists; it does
  not).
- **C6** Scale/concurrency untested: pagination beyond one page, concurrent PUTs
  to one key, concurrent uploads, multi-block large objects.

---

## Corrections (look like issues, are NOT)

- **`KeyCount` includes CommonPrefixes** — this is *correct* for ListObjectsV2
  (a reviewer flagged it then retracted). Not a bug.
- `pct_decode` trailing `%XX`, `decode_aws_chunked` slice bounds, the EC read
  `len: 0` (the DN reads the whole stored shard by path), and shard-path
  collision (path includes replica index) were all checked and are correct.
