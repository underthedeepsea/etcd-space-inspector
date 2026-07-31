# Key Churn Analysis Design

## Goal

Make frequently updated etcd Keys visible in both a single Snapshot and a two-Snapshot comparison, without retaining raw Values or overstating what a Snapshot can prove.

## Scope

`0.7.0` adds two views:

1. A single-Snapshot **High-churn Keys** leaderboard, ranked by the number of MVCC revision records currently retained for each Key.
2. A two-Snapshot **Key churn** leaderboard, ranked by the net retained-revision delta. When the operator supplies both Snapshot collection times for that comparison, the view also shows the delta divided by the observed interval.

The existing per-Key `revisionCount` and two-Snapshot `revisionCountDelta` are reused. No raw Values, log files, Audit events, external services, or new runtime dependencies are introduced.

## Evidence and terminology

The product must distinguish evidence from inference:

- **Retained revisions** is the count of decoded MVCC records still present in the imported DB. A high count is evidence that this Key has substantial retained history.
- **Net retained-revision delta** is the target count minus the baseline count for the same Key. It is not a count of all writes: compaction can remove records between Snapshots.
- **Net retained revisions per hour** is available only when both collection times are provided, strictly ordered, and at least one second apart. It is the retained-revision delta divided by that observed interval. A negative value can occur after compaction.
- The tool must not call either measure an exact client write rate or identify a Controller, client, or user.

## Data flow

The single-Snapshot leaderboard reads the existing paginated Key endpoint with `sort=revision_count` and descending order. It shows the first 20 rows, including Key, retained revisions, historical bytes, and tombstone count. The complete Key table remains available for filtering and inspection.

Comparison manifests gain optional `baselineObservedAt` and `targetObservedAt` values. They belong to a comparison rather than a task so an already imported Snapshot can be compared using the collection time known for that specific investigation. Both values must be supplied together, must be RFC 3339 timestamps, and the target must be later than the baseline.

The comparison calculator derives an interval in seconds from those manifest values. It persists the interval in the comparison summary and uses it for the existing global net retained-revision rate. The UI derives each Key's rate from its existing `revisionCountDelta` and that interval, avoiding a redundant database column.

## Interfaces

- `POST /api/v1/diffs` accepts optional `baselineObservedAt` and `targetObservedAt` strings in RFC 3339 format.
- `etcd-analyzer diff` accepts matching `--baseline-observed-at` and `--target-observed-at` flags.
- The Web comparison form provides two optional `datetime-local` controls. The browser converts a supplied local time to RFC 3339 before posting it.
- `GET /api/v1/diffs/{id}/overview` adds `observationWindowSeconds` while preserving all existing fields.
- The existing `GET /api/v1/diffs/{id}/keys?sort=revision_count&order=desc` endpoint provides the two-Snapshot leaderboard; no endpoint is added.

Existing task and comparison manifests without observation times remain valid. Their rate fields are explicitly unavailable rather than inferred from import timestamps.

## User interface

The MVCC analysis panel gains a compact **High-churn Keys** table above the full Key table. The copy says that it is ranked by retained revisions, with a short notice that compaction may have removed older history.

The comparison panel gains a **Key churn between Snapshots** table when MVCC comparison is available. It always shows the revision delta. It shows a rate column and the observation interval only when both timestamps were supplied. When no interval exists, it explains that collection times are required for a rate.

All new visible text, accessible labels, and metric help are localized in Chinese and English.

## Error handling and compatibility

- One observation timestamp without the other is rejected as invalid input.
- Malformed timestamps and non-increasing timestamps are rejected as invalid input.
- MVCC-incompatible comparisons retain their existing explicit unavailable state and do not show churn conclusions.
- Existing comparisons are opened with an `observationWindowSeconds` default of zero, so their persisted databases remain readable.

## Verification

- Unit tests cover comparison time validation, interval derivation, unavailable rates for missing times, and a positive per-Key revision delta.
- API tests cover accepted timestamps and invalid/missing-pair input.
- Type checking and the locale catalog test cover the new UI contracts and translations.
- Full Go tests, Go vet, frontend type checking, locale test, production frontend build, and a browser check of both leaderboards must pass.
