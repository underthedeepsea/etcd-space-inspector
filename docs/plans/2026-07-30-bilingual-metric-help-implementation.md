# Bilingual Interface and Metric Help Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Chinese/English interface switch, localized definitions for every metric card, and safer default configuration for large snapshot imports.

**Architecture:** A dependency-free locale catalog provides all fixed copy and metric-card definitions. `App.tsx` keeps one selected locale and exposes it to analysis components through a small React context. The Go configuration package caps the existing MVCC decode worker default and reduces the existing bounded channel default.

**Tech Stack:** Go, React 19, TypeScript, Vite, native browser local storage and tooltips.

## Global Constraints

- Continue development on `release/0.6.0`; do not create a release tag.
- Do not add runtime or development dependencies.
- Do not modify API contracts, database schemas, or stored result values.
- Do not add `etcd-dbsize-analyzer-codex-development-guide.md` or any `docs/superpowers/` content to Git.
- Translate all fixed interface text; preserve user-provided data and server error text.
- Add help only to metric cards, not table headers.

---

### Task 1: Verify safer server defaults

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`

**Interfaces:**
- Produces: `defaultWorkerCount(cpus int) int`, returning an integer from `1` through `4`.
- Preserves: `Load(path string) (Config, error)` and the existing YAML field names.

- [ ] **Step 1: Write the failing tests**

Add a table-driven test that names the capped-worker behavior:

```go
func TestDefaultWorkerCountCapsLargeCPUHosts(t *testing.T) {
    cases := []struct {
        cpus int
        want int
    }{{0, 1}, {1, 1}, {4, 4}, {8, 4}}
    for _, tc := range cases {
        if got := defaultWorkerCount(tc.cpus); got != tc.want {
            t.Fatalf("cpus=%d got=%d want=%d", tc.cpus, got, tc.want)
        }
    }
}
```

Extend `TestLoadDefaultsAndOverride` so it asserts `ChannelSize == 128`, `SQLiteBatchSize == 1000`, and `WorkerCount` is within `1..4`.

- [ ] **Step 2: Run the focused test to verify it fails**

Run:

```bash
go test ./internal/config -run 'Test(DefaultWorkerCountCapsLargeCPUHosts|LoadDefaultsAndOverride)' -count=1
```

Expected: fail because `defaultWorkerCount` does not exist and the current channel default is `256`.

- [ ] **Step 3: Implement the smallest default change**

Add:

```go
func defaultWorkerCount(cpus int) int {
    if cpus < 1 {
        return 1
    }
    if cpus > 4 {
        return 4
    }
    return cpus
}
```

Set `c.Analysis.WorkerCount = defaultWorkerCount(runtime.NumCPU())` and `c.Analysis.ChannelSize = 128`. Leave the SQLite batch and max-input defaults untouched.

- [ ] **Step 4: Run the focused test to verify it passes**

Run:

```bash
go test ./internal/config -run 'Test(DefaultWorkerCountCapsLargeCPUHosts|LoadDefaultsAndOverride)' -count=1
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add internal/config/config.go internal/config/config_test.go
git commit -m "perf: tune large snapshot defaults"
```

### Task 2: Add a tested locale catalog

**Files:**
- Create: `web/src/locales.ts`
- Create: `web/src/locales.test.ts`
- Create: `web/tsconfig.locales-test.json`
- Modify: `web/package.json`
- Modify: `.gitignore`

**Interfaces:**
- Produces: `Locale`, `metricKeys`, `resolveLocale(saved, browserLanguage)`, `text(locale, key, values?)`, and `metric(locale, key)`.
- Consumes: no browser or React APIs; the catalog remains directly executable by Node after TypeScript compilation.
- Preserves: existing Vite build and typecheck scripts.

- [ ] **Step 1: Write the failing locale-catalog test**

Create `web/src/locales.test.ts` with Node assertions:

```ts
assert.equal(resolveLocale(null, 'zh-CN'), 'zh');
assert.equal(resolveLocale('en', 'zh-CN'), 'en');
assert.equal(text('zh', 'pagination.records', { page: 2, count: 3 }), '第 2 页 · 3 条记录');
for (const key of metricKeys) {
  for (const locale of ['zh', 'en'] as const) {
    const copy = metric(locale, key);
    assert.ok(copy.label.length > 0);
    assert.ok(copy.help.length > 0);
  }
}
```

Add `web/tsconfig.locales-test.json` with `rootDir: "./src"`, `outDir: "./.locales-test"`, `noEmit: false`, `module: "NodeNext"`, and `moduleResolution: "NodeNext"`. Add a `test:locales` package script that compiles this configuration and runs `node .locales-test/locales.test.js`. Ignore `web/.locales-test/`.

- [ ] **Step 2: Run the locale test to verify it fails**

Run:

```bash
npm --prefix web run test:locales
```

Expected: TypeScript reports that `./locales.js` cannot be resolved because the locale module does not exist.

- [ ] **Step 3: Implement the locale catalog**

Create `web/src/locales.ts` with:

```ts
export type Locale = 'zh' | 'en';
export const languagePreferenceKey = 'etcd-space-inspector.language';
export function resolveLocale(saved: string | null, browserLanguage: string): Locale {
  if (saved === 'zh' || saved === 'en') return saved;
  return browserLanguage.toLowerCase().startsWith('zh') ? 'zh' : 'en';
}
```

Add complete Chinese and English fixed-copy catalogs, a placeholder replacement implementation, the 26 metric keys named in the design, and typed localized metric label/help records. Import paths in the test use `.js` so Node can execute the compiled ESM output.

- [ ] **Step 4: Run the locale test to verify it passes**

Run:

```bash
npm --prefix web run test:locales
```

Expected: pass with no test output.

- [ ] **Step 5: Commit**

```bash
git add .gitignore web/package.json web/tsconfig.locales-test.json web/src/locales.ts web/src/locales.test.ts
git commit -m "feat: add bilingual interface catalog"
```

### Task 3: Localize the application and metric cards

**Files:**
- Modify: `web/src/App.tsx`
- Modify: `web/src/style.css`

**Interfaces:**
- Consumes: locale catalog exports from `web/src/locales.ts`.
- Produces: `LanguageContext`, `useTranslation()`, and a `Metric` component that accepts `metricKey`.
- Preserves: task and comparison API payloads, status CSS classes, and every analysis request.

- [ ] **Step 1: Confirm the current application does not expose a language control**

Run:

```bash
npm --prefix web run typecheck
```

Then inspect the page at `http://127.0.0.1:18080/`.

Expected: page copy is English and metric labels do not include help icons.

- [ ] **Step 2: Add minimal locale state and translation context**

Import `createContext`, `useContext`, and locale helpers. Initialize state with:

```ts
const [locale, setLocale] = useState(() =>
  resolveLocale(window.localStorage.getItem(languagePreferenceKey), window.navigator.language),
);
```

Wrap the page in a context provider. The language button updates state and writes the preference key to local storage. Set `<main lang={locale === 'zh' ? 'zh-CN' : 'en'}>`.

- [ ] **Step 3: Replace fixed copy**

Use `t(...)` for header, task and comparison controls, table headers, status/type/page/decode labels, filters, notices, loading states, pagination, version evidence, and boolean values. Use `toLocaleString(locale === 'zh' ? 'zh-CN' : 'en-US')` for timestamps. Leave paths, names, keys, hashes, resource values, and server errors untouched.

- [ ] **Step 4: Convert every metric card to keyed localized help**

Replace `Metric label="..."` calls with `Metric metricKey="..."`. Render:

```tsx
<span className="metric-label">
  {copy.label}
  <span className="metric-help" role="img" aria-label={copy.help} title={copy.help}>?</span>
</span>
```

Use all 26 catalog keys, including the snapshot-comparison cards.

- [ ] **Step 5: Style the language and help controls**

Add compact styles for `.language-switch`, `.language-switch button`, `.language-switch .active`, `.metric-label`, and `.metric-help`. The help affordance must remain visible, keyboard focusable, and compact enough not to wrap metric-card values.

- [ ] **Step 6: Run typecheck and locale test**

Run:

```bash
npm --prefix web run test:locales
npm --prefix web run typecheck
```

Expected: both pass.

- [ ] **Step 7: Browser verification**

Open the local page. Switch to Chinese and verify the task title and physical metric labels change. Hover and focus the `?` beside “物理文件” and confirm Chinese help text appears. Switch to English and confirm the matching English help text appears.

- [ ] **Step 8: Commit**

```bash
git add web/src/App.tsx web/src/style.css
git commit -m "feat: localize analysis interface and metric help"
```

### Task 4: Document and verify the integrated change

**Files:**
- Modify: `README.md`

**Interfaces:**
- Documents: the server YAML knobs, their safer defaults, and the browser-only language preference behavior.

- [ ] **Step 1: Update the user documentation**

Add a concise configuration example:

```yaml
analysis:
  workerCount: 4
  channelSize: 128
  sqliteBatchSize: 1000
security:
  maxInputBytes: 53687091200
```

State that `workerCount` applies to MVCC decode workers, `channelSize` bounds in-flight records, and the browser language selection is stored locally.

- [ ] **Step 2: Run all verification**

Run:

```bash
env GOCACHE=/private/tmp/etcd-analyzer-go-cache-060 GOPATH=/private/tmp/etcd-analyzer-gopath-060 go test ./...
npm --prefix web run test:locales
npm --prefix web run typecheck
npm --prefix web run build
git diff --check
```

Expected: all commands pass.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: describe bilingual UI and analysis tuning"
```
