# Flow: EC reconstruction — survivor-enumeration + min-length (Track 2, B4–B5)

Design doc for making the datanode's reconstruction compliant with the real Ozone
`ECReconstructionCoordinator`. `[V-source]` = verified against quoted Ozone source
(see the real-EC-repair research output); `[I]` = inferred / decision taken.

## 1. What real Ozone does (the contract to match)

On a `ReconstructECContainersCommandProto` the coordinator DN: [V-source: ECReconstructionCoordinator.java]
1. **Enumerates block groups from the SURVIVORS, not locally.** `getBlockDataMap` calls `ListBlock` on EVERY source DN and assembles `localID -> BlockData[k+p]` (column = `replicaIndex-1`). A block group present on only some sources ("orphan") is dropped.
2. **Derives each block group's length as `min(blockGroupLen)` across the non-null survivors** (`calcEffectiveBlockGroupLen`); `blockGroupLen` is a PutBlock metadata key, NOT the `size` field. This is the partial-stripe correctness guarantee: it excludes torn/garbage trailing writes (over-recovering by ≤1 stripe is acceptable; under-recovering user bytes is data loss).
3. **Creates each target container RECOVERING** (stamping `replicaIndex` + `state=RECOVERING`) before any write.
4. **Per block group:** read exactly `k` survivor shards (`ReadChunk`), `decode` only the missing index(es), `WriteChunk` each rebuilt buffer to its target, then `PutBlock(len)`.
5. **Group-atomic:** on any error, delete every created RECOVERING container (state-guarded) and abort; on success, close them. Completion is signalled by the next container report (optionally a CommandStatus).

The command pairs `targets[i] <-> missingContainerIndexes[i]` positionally; `sources` carry their slot inline. NO block hints in the command.

## 2. The gap vs the current Rust code

The B2 `handle_reconstruct` (and `repair::reconstruct_and_persist`) read the TARGET's
OWN local metadata for the block list + length, and trust the (now removed) command
`block_group_len`. That only heals bit-rot of an existing shard. It CANNOT rebuild a
wholly-lost replica onto a fresh target (no local metadata → nothing to enumerate),
and it never computes `min(blockGroupLen)` — so a partial-stripe object whose
survivors disagree on length could be rebuilt at the wrong length. Both are fixed here.

## 3. The Rust algorithm (all-Rust fleet; data plane stays `DatanodeGatewayService`)

`reconstruct_from_survivors(meta, chunks, input)` where `input` carries: container, ec,
`missing_slots` this DN rebuilds, and `sources: [(slot, endpoint)]`.

1. **Enumerate from survivors.** For each source `(slot, endpoint)`: `DnClient::connect`,
   `list_blocks(container, ...)` (paginated) → its `BlockData`s (this source holds that
   one slot, so its blocks are that slot's). Build `local_id -> { slot -> BlockData }`.
   `[uses DnClient::list_blocks — confirm/add]`
2. **Per block group `local_id`:**
   a. Length = `min` over the survivors that returned a block for `local_id` of
      `block_group_len()` (skip 0). `[V-source: min(blockGroupLen)]`
   b. Gather views: for each slot `1..=k+p`, the survivor shard bytes if a survivor
      holds that slot (read via `DnClient::read_chunk(..., verify=true)` so a rotten
      survivor is dropped); `None` otherwise.
   c. If `< k` present → skip this block group (unrecoverable; log). Else
      `decode_object(profile, len, views)` then `encode_object` to recover the exact
      stored shards (data + parity byte-identical — proven by `repair_identity`).
   d. For each `slot` in `missing_slots`: persist the rebuilt shard locally (this DN is
      the target) — `write_chunk` + recomputed CRC + `put_block` with `block_group_len = len`,
      into a RECOVERING-then-CLOSED container (B5).
3. **Group-atomic:** accumulate; on a fatal error mid-group, roll back this DN's
   created container/blocks. `[I] for a single-target self-rebuild, per-block idempotent
   writes + a final close is sufficient; full multi-target coordinator push is deferred.]`

**Scope decision [I]:** real Ozone designates ONE coordinator that pushes rebuilt
shards to PEER targets. For the Rust slice we model each TARGET DN rebuilding its OWN
assigned slot from survivors (the command is delivered to the target; it reads
survivors and writes locally). Multi-target push-to-peer is a later refinement; it does
not change the per-shard correctness (survivor-enum + min-length + decode).

## 4. Invariants (data safety is paramount)

- **S1 length correctness.** The rebuilt block-group length is EXACTLY `min(blockGroupLen)`
  over survivors — never the target's stale local value, never a command value. Wrong
  length silently corrupts the trailing partial stripe.
- **S2 never decode below k.** `< k` verified survivors ⇒ skip (no write), never a
  partial/garbage shard.
- **S3 verified survivors only.** Each survivor read uses `verify=true`; a survivor whose
  own shard is corrupt is dropped before it can poison the decode.
- **S4 byte-identical rebuild.** The persisted shard equals the original (decode→re-encode
  identity); a later verified read passes with no reconstruction.
- **S5 no resurrection / no orphan.** Writing into a non-Open container is refused (the
  G1/G2 guard); a wholly-lost replica is provisioned RECOVERING then closed (B5).
- **L1 wholly-lost replica is rebuildable.** A target with NO local metadata rebuilds the
  whole replica from survivors (the case the current code cannot do).

## 5. Tests (B4–B5 worklist)

1. **wholly-lost replica**: target DN has no local container/metadata; given a command +
   survivors, it enumerates from survivors, creates the container, rebuilds every block
   group, and a verified per-slot read succeeds. (The current code literally cannot do
   this.)
2. **partial-stripe min-length**: survivors report DIFFERENT `blockGroupLen` (simulate a
   torn trailing write on one survivor); assert the rebuilt length == the MIN and the
   recovered bytes == the shorter-valid object. Directly targets S1, the headline risk.
3. **orphan prune**: a `local_id` present on only some survivors is dropped (not rebuilt
   at a wrong length).
4. **unrecoverable**: `< k` good survivors ⇒ no write, shard stays absent, no panic.
5. **idempotent re-delivery**: a second identical command is a no-op (already-clean
   shard skipped).
6. RECOVERING lifecycle (B5): create RECOVERING with replica_index → write → close; a
   failure deletes the RECOVERING container (state-guarded), never a CLOSED one.

## 6. B5 prerequisites (data-plane additions, Rust-native, all-Rust fleet)

- `ContainerState::Recovering` (or reuse Open with a recovering flag) so a fresh target
  can be provisioned before writes and the G1/G2 refuse-non-Open guard still applies to
  real CLOSED containers.
- `CreateContainerRequest.replica_index` so the target stamps its EC slot.
- A state-guarded delete for rollback.
- `DnClient::list_blocks` survivor enumeration (confirm it exists / exercise it).
