import assert from 'node:assert/strict';

import { metric, metricKeys, resolveLocale, text, type TextKey } from './locales.js';

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

const churnKeys: TextKey[] = [
  'semantic.highChurnKeys',
  'semantic.retainedRevisions',
  'semantic.retainedCaveat',
  'comparison.configure',
  'comparison.baselineObservedAt',
  'comparison.targetObservedAt',
  'comparison.collectionTimePair',
  'comparison.highChurnKeys',
  'comparison.revisionDelta',
  'comparison.revisionsPerHour',
  'comparison.observationWindow',
  'comparison.rateUnavailable',
];

for (const key of churnKeys) {
  for (const locale of ['zh', 'en'] as const) {
    assert.ok(text(locale, key).length > 0);
  }
}

const logKeys: TextKey[] = [
  'form.log',
  'form.logHint',
  'type.log',
  'log.title',
  'log.inputSummary',
  'log.totalLines',
  'log.recognizedEvents',
  'log.unknownLines',
  'log.parseErrors',
  'log.firstObservedAt',
  'log.lastObservedAt',
  'log.from',
  'log.to',
  'log.eventType',
  'log.severity',
  'log.source',
  'log.allEvents',
  'log.allSeverities',
  'log.allSources',
  'log.event',
  'log.time',
  'log.line',
  'log.duration',
  'log.revision',
  'log.dbSize',
  'log.parseStatus',
  'log.fingerprint',
  'log.unknownTime',
  'log.empty',
  'log.loadFailed',
  'log.safetyBoundary',
  'log.noAttribution',
  'log.previous',
  'log.next',
];

for (const key of logKeys) {
  for (const locale of ['zh', 'en'] as const) {
    assert.ok(text(locale, key).length > 0);
  }
}
