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

const evidenceKeys: TextKey[] = [
  'evidence.title', 'evidence.selectLog', 'evidence.noLogs', 'evidence.noWindow',
  'evidence.recreateComparison', 'evidence.loadFailed', 'evidence.window',
  'evidence.coverage', 'evidence.coverage.full', 'evidence.coverage.partial',
  'evidence.coverage.none', 'evidence.coverage.unknown', 'evidence.sourceUnverified',
  'evidence.evidenceOnly', 'evidence.byEventType', 'evidence.bySeverity',
  'evidence.bySource', 'evidence.taskSha', 'evidence.empty',
  'evidence.previous', 'evidence.next',
];
for (const key of evidenceKeys) {
  for (const locale of ['zh', 'en'] as const) assert.ok(text(locale, key).length > 0);
}

for (const key of ['evidence.matchedEvents', 'evidence.windowSeconds'] as const) {
  for (const locale of ['zh', 'en'] as const) {
    const copy = metric(locale, key);
    assert.ok(copy.label.length > 0);
    assert.ok(copy.help.length > 0);
  }
}

const auditKeys: TextKey[] = [
  'form.audit', 'form.auditHint', 'type.audit', 'audit.title', 'audit.inputSummary',
  'audit.validEvents', 'audit.writeEvents', 'audit.deduplicatedEvents', 'audit.username',
  'audit.userAgent', 'audit.sourceNetwork', 'audit.verb', 'audit.resource', 'audit.namespace',
  'audit.object', 'audit.responseCode', 'audit.payloadCaveat', 'audit.safetyBoundary',
  'audit.empty', 'audit.loadFailed', 'audit.previous', 'audit.next',
  'auditEvidence.title', 'auditEvidence.selectAudit', 'auditEvidence.noAudits',
  'auditEvidence.noWindow', 'auditEvidence.sourceUnverified', 'auditEvidence.causality',
  'auditEvidence.matchLevel', 'auditEvidence.high', 'auditEvidence.medium',
  'auditEvidence.low', 'auditEvidence.unverified', 'auditEvidence.exactObjects',
  'auditEvidence.writes', 'auditEvidence.candidates', 'auditEvidence.objectsUnavailable',
  'auditEvidence.loadFailed', 'comparison.objectGrowth',
];
for (const key of auditKeys) {
  for (const locale of ['zh', 'en'] as const) assert.ok(text(locale, key).length > 0);
}

const metricsKeys: TextKey[] = [
  'form.metrics', 'form.metricsHint', 'type.metrics', 'metrics.title', 'metrics.loadFailed',
  'metrics.allTypes', 'metrics.instance', 'metrics.series', 'metrics.samples', 'metrics.empty',
  'metricsEvidence.title', 'metricsEvidence.select', 'metricsEvidence.none', 'metricsEvidence.loadFailed',
  'metricsEvidence.growthStart', 'metricsEvidence.growthInterval', 'metricsEvidence.coverage',
  'metricsEvidence.sourceUnverified', 'metricsEvidence.causality', 'metricsEvidence.counterGap',
  'metricsEvidence.reclaimableCaveat', 'metricsEvidence.aligned', 'metricsEvidence.notAligned',
];
for (const key of metricsKeys) {
  for (const locale of ['zh', 'en'] as const) assert.ok(text(locale, key).length > 0);
}

for (const key of ['metrics.supportedSeries', 'metrics.validSamples', 'metrics.instances', 'metrics.dbDelta', 'metrics.inUseDelta', 'metrics.reclaimable', 'metrics.quotaRatio', 'metrics.putRate', 'metrics.deleteRate', 'metrics.backendP99', 'metrics.walP99'] as const) {
  for (const locale of ['zh', 'en'] as const) {
    const copy = metric(locale, key);
    assert.ok(copy.label.length > 0);
    assert.ok(copy.help.length > 0);
  }
}

for (const key of ['audit.totalLines', 'audit.validEvents', 'audit.writeEvents', 'audit.unknownLines', 'audit.parseErrors', 'audit.candidates', 'audit.exactMatches', 'audit.payloadBytes'] as const) {
  for (const locale of ['zh', 'en'] as const) {
    const copy = metric(locale, key);
    assert.ok(copy.label.length > 0);
    assert.ok(copy.help.length > 0);
  }
}
