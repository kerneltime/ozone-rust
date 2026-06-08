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
      the target) — `write_chunk` + recomputed CRC + `put_block` with `block_group_len = len`.
3. **Lifecycle (B5-lite, IMPLEMENTED).** The in-progress state is `Open` (we do not yet
   have a distinct `RECOVERING` enum — see §6); the terminal state is `Closed`:
   - target absent → create `Open`, rebuild, then `set_container_state(Closed)` on
     success — a complete replica and a valid future EC source (real Ozone's
     RECOVERING→CLOSED). `we_created = true`.
   - target pre-existing `Open` (the in-place scrubber heal) → rebuild in place, leave
     `Open`. `we_created = false`. NEVER closed or deleted here.
   - target `Closed` → already reconstructed; a re-delivered command is a no-op.
4. **Group-atomic rollback (IMPLEMENTED).** A mid-rebuild error on a container WE
   created deletes it (metadata + bytes) before surfacing the error, so a half-built
   replica is never left to be reported healthy (which could make SCM trim a real
   replica → data loss). A pre-existing container is NEVER deleted on a rebuild error —
   that is the `we_created` safety boundary, proven load-bearing by mutation
   (unconditional rollback fails `reconstruct_keeps_preexisting_container_on_failure`).
   `[I] single-target self-rebuild; the full multi-target coordinator push-to-peer is
   deferred and does not change per-shard correctness.]`

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
6. Lifecycle (B5-lite, DONE): `reconstruct_closes_fresh_target_and_noops_on_redelivery`
   (create Open → rebuild → CLOSED; re-delivery to CLOSED is a no-op),
   `reconstruct_rolls_back_created_container_on_failure` (a write fault mid-rebuild
   deletes the container WE created), `reconstruct_keeps_preexisting_container_on_failure`
   (a pre-existing container survives a rebuild failure — never deleted). All three were
   confirmed load-bearing by mutating the close / rollback / `we_created` guard and
   watching exactly the matching test fail.
7. Convergence ICR (DONE): `reconstruct_announces_closed_replica_to_scm` — a compliant
   ReconstructEC drives the loop, and a later heartbeat carries an INCREMENTAL report
   marking the rebuilt replica `CLOSED` (slot 1). Load-bearing (disabling `report_state`
   fails it).
8. Empty-rebuild rollback (DONE): `reconstruct_rolls_back_empty_rebuild_no_spurious_replica`
   — a fresh target offered `< k` survivors rebuilds nothing and the provisioned
   container is rolled back (no empty replica is announced). Load-bearing (disabling the
   `rebuilt.is_empty()` guard fails it).

## 6. B5 status (data-plane additions, Rust-native, all-Rust fleet)

- **[DONE, reuse-Open]** In-progress state. B5-lite reuses `Open` as the in-progress
  state and `Closed` as terminal, gated by `we_created`. A distinct
  `ContainerState::Recovering` enum is DEFERRED; it would let a full container report
  exclude an in-progress target as a source even across a DN restart. Today the report
  risk is absent for a different reason: the heartbeat loop is single-threaded and the
  full container report is built only at registration, so no report observes the Open
  target mid-rebuild (a crash mid-rebuild leaves an Open, non-CLOSED container that SCM
  does not treat as an EC source; a `Recovering` enum + restart sweep would make this
  explicit rather than incidental).
- **[DONE]** State-guarded delete for rollback — `MetaStore::delete_container` +
  `ChunkStore::delete_container`, invoked only when `we_created`.
- **[DONE]** `DnClient::list_blocks` survivor enumeration — exercised by every B4/B5
  test (the fresh target enumerates slots from the survivor peers).
- **[DONE]** Reporting the restored replica back to SCM. After a successful rebuild
  (and after a CloseContainer), the compliant loop emits an INCREMENTAL container
  report (`report_state`) carrying the replica's CURRENT state — `CLOSED` for a fresh
  whole replica — exactly mirroring real Ozone's `sendICR`-on-close. This is the
  convergence signal: SCM keys replicas by `(containerID, datanodeID)` IGNORING state
  ([V-source]: `ContainerReplica.equals` + `ContainerStateMap.put`), so a `CLOSED`
  report OVERWRITES the prior `UNHEALTHY` entry. Verified against
  `AbstractContainerReportHandler.processContainerReplica` (full and ICR share the
  per-replica update path). Test: `reconstruct_announces_closed_replica_to_scm`. The
  guard `if !locals.is_empty()` plus the empty-rebuild rollback ensures we never
  announce a replica that holds no rebuilt data.
- **[DEFERRED, low-risk]** Clearing the scrubber's rising-edge latch after an in-place
  heal so a future re-rot of the same slot can be re-reported. Only matters for the
  same-DN in-place model (FakeScm / bespoke loop); with a REAL SCM the reconstruction
  target is a FRESH DN, the rotted replica is deleted by SCM, and its latch entry is
  moot. Revisit when the bespoke loop is retired (B7).
- **[DEFERRED]** Periodic FULL container report (real default 60m,
  `hdds.container.report.interval`). Its unique value is RECONCILIATION — pruning
  replicas SCM thinks exist on this DN but no longer do ([V-source]:
  `ContainerReportHandler.processMissingReplicas`). Convergence-after-heal does NOT
  need it (the ICR above suffices); we still want it for full compliance. We currently
  send a full report only at registration.
- **[N/A for self-rebuild]** `CreateContainerRequest.replica_index` — the target stamps
  its slot via the per-block `ReplicaIndex` on `put_block`, not a container field.
