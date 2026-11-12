// Ready-made runs for the most common stress tests. Choosing one fills the Generator card (and the intake toggles on
// the Backend card); any field can still be changed before starting, which marks the choice as modified. Live
// controls that act immediately (load shedding, pausing downstream work, Kafka settings) are left as they are.
//
// Numbers come from what was measured on this machine: the system keeps up to about 9,000-11,500 accepted per second
// (550k-700k per minute), and the generator alone tops out near 15,000 per second.

const BASE = {
    rate: 50, duration: 1, endless: false, warmup: 30, maxInFlight: null, httpClient: 'JETTY',
    retry: false, retryPeriod: false, retryPeriodSeconds: 120, duplicates: 0, circuitBreaker: true,
    intake: 'KAFKA',
};

export const PRESETS = [
    { id: 'smoke', name: 'Smoke test', settings: { rate: 6_000, duration: 1, warmup: 10, retryPeriod: true, retryPeriodSeconds: 30 },
      purpose: 'Does everything work end to end? 100 submissions per second for a minute.',
      lookFor: 'Every submission accepted, and the checker says Consistent once processing settles.' },
    { id: 'baseline', name: 'Baseline load', settings: { rate: 300_000, duration: 2, retryPeriod: true },
      purpose: 'Normal heavy traffic, well within capacity: 5,000 per second for 2 minutes.',
      lookFor: 'Replies in tens of milliseconds, nothing skipped, no backlog building in Kafka.' },
    { id: 'capacity', name: 'Find the ceiling', settings: { rate: 700_000, duration: 1 },
      purpose: 'Just above the measured capacity, with no retry period, so the limit shows plainly.',
      lookFor: 'Accepted per second levels off; the rest are skipped, never Unknown. The Kafka panel\'s producer metrics show where time goes.' },
    { id: 'overload', name: 'Overload and recover', settings: { rate: 1_000_000, duration: 1, retryPeriod: true, retryPeriodSeconds: 180 },
      purpose: 'Far beyond capacity: does the system degrade gracefully and finish the job afterwards?',
      lookFor: 'No Unknown outcomes, Connections opened and closed stay 0, and the retry period recovers what was skipped.' },
    { id: 'spike', name: 'Cold spike', settings: { rate: 500_000, duration: 1, warmup: 0, retryPeriod: true },
      purpose: 'A sudden burst with no warm-up: cold code, no open connections, a cold Kafka producer.',
      lookFor: 'How bad the first seconds are (skips, refused or Unknown) and how fast it settles.' },
    { id: 'soak', name: 'Soak (until stopped)', settings: { rate: 400_000, endless: true },
      purpose: 'Sustained load for as long as you let it run, to see whether anything builds up over time.',
      lookFor: 'Flat accepted rate and latency, Connections opened and closed staying 0. Press Stop, then Retry now if anything was skipped.' },
    { id: 'idempotency', name: 'Duplicates and retries', settings: { rate: 200_000, duration: 2, retry: true, retryPeriod: true, duplicates: 20 },
      purpose: 'Every fifth submission sent twice, plus retries: resends must never create or process anything twice.',
      lookFor: 'The checker\'s Cases created twice and Processed more than once both 0.' },
    { id: 'downtime', name: 'Backend failure drill', settings: { rate: 300_000, duration: 3, retryPeriod: true, retryPeriodSeconds: 180 },
      purpose: 'Run for 3 minutes; about a minute in, press Crash (or Stop gracefully) on the Backend card, then restart the backend from IntelliJ.',
      lookFor: 'Backend reachable goes No, Checking, Back. Afterwards Accepted but missing and Cases created twice are 0; Unknown, saved anyway shows requests caught mid-crash.' },
    { id: 'connection', name: 'Generator ceiling (connection test)', settings: { rate: 1_000_000, duration: 1, intake: 'NOOP' },
      purpose: 'Nothing stored: the backend replies instantly, so this measures the generator and the web server alone.',
      lookFor: 'Accepted per second here is the most the generator can send (about 15,000 per second measured).' },
];

/** A preset's full settings: its own values over the defaults. */
export function presetSettings(preset) {
    return { ...BASE, ...preset.settings };
}

/** Writes a preset's settings into the form's fields and fires their events, so everything that depends on them updates. */
export function applyPreset(preset, el) {
    const s = presetSettings(preset);
    const set = (id, value, event = 'input') => {
        const field = el(id);
        if (!field) return;
        if (field.type === 'checkbox') field.checked = Boolean(value); else field.value = String(value);
        field.dispatchEvent(new Event(event, { bubbles: true }));
    };
    set('rate', s.rate);
    set('duration', s.duration);
    set('endless', s.endless, 'change');
    set('warmup', s.warmup);
    set('unlimited', s.maxInFlight == null, 'change');
    if (s.maxInFlight != null) set('capacity', s.maxInFlight);
    set('http-client', s.httpClient, 'change');
    set('retry', s.retry, 'change');
    set('retry-period', s.retryPeriod, 'change');
    set('retry-period-seconds', s.retryPeriodSeconds);
    set('duplicates', s.duplicates);
    set('circuit-breaker', s.circuitBreaker, 'change');
    set('connection-test', s.intake === 'NOOP', 'change');
    set('use-kafka', s.intake !== 'DIRECT', 'change');
}

/** The fields a preset sets: changing any of them by hand marks the preset as modified. */
export const PRESET_FIELDS = ['rate', 'duration', 'endless', 'warmup', 'unlimited', 'capacity', 'http-client', 'retry',
    'retry-period', 'retry-period-seconds', 'duplicates', 'circuit-breaker', 'connection-test', 'use-kafka'];
