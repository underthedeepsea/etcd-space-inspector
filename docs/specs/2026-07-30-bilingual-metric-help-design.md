# Bilingual Interface and Metric Help Design

## Goal

Make the local analysis interface usable in Chinese and English, and explain every analysis metric card without changing analysis results or adding runtime dependencies.

## Scope

- Add a `中文 / English` control in the page header.
- Select Chinese when the browser language begins with `zh`; otherwise select English.
- Persist an explicit user selection in browser local storage and restore it on the next visit.
- Translate all fixed interface copy: section titles, forms, buttons, notices, table headers, filters, status labels, pagination, and empty states.
- Translate task type, task status, page type, decode status, boolean display, and version-evidence text while keeping API values and CSS status classes unchanged.
- Add a `?` help affordance beside the label of every metric card in physical analysis, MVCC analysis, Kubernetes analysis, and snapshot comparison.
- Show the help text in the active language on hover and keyboard focus.

## Non-goals

- Do not translate user-provided task names, bucket paths, keys, Kubernetes object names, hashes, API group/resource values, or errors returned by the server.
- Do not add tooltip, internationalization, test-framework, or UI-library dependencies.
- Do not add help icons to table columns in this change.
- Do not change API contracts, database schema, or stored analysis data.

## UI Design

The header contains a compact two-button language switch. The selected language uses the existing primary button treatment; the other button uses a neutral visual treatment. The page language is set through the root `lang` attribute.

Metric cards retain their existing layout. A small inline `?` follows the metric label. It exposes its definition through a native browser tooltip and an accessible label, so it works with a mouse and keyboard without a custom popup state machine.

## Translation Design

`web/src/locales.ts` owns:

- `Locale` (`zh` or `en`) and browser-language resolution;
- the persisted preference key;
- fixed UI text, including simple `{placeholder}` interpolation;
- metric-card labels and explanations keyed by a stable metric identifier.

`App.tsx` owns the selected locale and passes it to nested analysis components through one React context. This prevents language props from being threaded through the physical, MVCC, and Kubernetes component chain. The context is limited to locale and text lookup; it does not own task or network state.

## Metric Definitions

Each metric card has a stable key and a localized label/help pair:

- Comparison: physical file delta, current data delta, historical-byte delta, tombstone-byte delta, free-page-byte delta, current-key delta, revision-record delta, historical-version delta, tombstone-count delta, and revision rate.
- Physical bbolt: physical file size, in-use page bytes, free page bytes, fragmentation ratio, and page count.
- MVCC: current key count, current stored bytes, historical-version count, historical bytes, and tombstone count.
- Kubernetes: current object count, current bytes, historical bytes, JSON revision count, Protobuf revision count, and encrypted revision count.

Definitions describe what the displayed value measures and, for comparison cards, that positive values mean the target snapshot exceeds the baseline.

## Large-Snapshot Defaults

The server configuration defaults change as follows:

- `analysis.workerCount`: cap the decode worker count at `4`, while preserving a lower CPU count. The MVCC pipeline has a single reader and writer, so unlimited logical CPUs mainly increase retained work and scheduling overhead for large inputs.
- `analysis.channelSize`: change from `256` to `128` to bound the number of raw and decoded records held between pipeline stages.
- `analysis.sqliteBatchSize`: remain `1000`; increasing it without a benchmark raises transient allocation and write bursts.
- `security.maxInputBytes`: remain `50 GiB`; it is an admission limit rather than a throughput setting.

YAML overrides keep their existing names and behavior.

## Verification

- A Go unit test covers the capped worker-count function and the new configuration defaults.
- A dependency-free TypeScript test compiles and executes the locale catalog, checks Chinese browser resolution, persisted-language precedence, interpolation, and that every metric key has both a label and help text in both languages.
- Full Go tests, web locale test, web typecheck, and a production web build must pass.
- A browser check confirms language switching updates visible copy and a metric help icon exposes localized help text.
