import { TrafficModel, DeltaTracker, kafkaBacklog } from './traffic-model.js';

const colors = { ink: '#192b2a', muted: '#4c615e', green: '#146650', kafka: '#2f6a88' };
const TRAVEL_MS = 1400;

// Generator -> Backend -> Kafka -> PostgreSQL. In direct mode the Kafka stage is bypassed and
// replies flow straight from the Backend to PostgreSQL.
export class TrafficView {
    constructor(root) {
        this.root = root;
        this.canvas = root.querySelector('canvas');
        this.ctx = this.canvas.getContext('2d');
        this.model = new TrafficModel();
        this.acceptedTracker = new DeltaTracker();
        this.savedTracker = new DeltaTracker();
        this.saved = null;
        this.selectedMode = 'KAFKA';
        this.connections = { generator: null, backend: null };
        this.particles = [];
        this.reduced = window.matchMedia('(prefers-reduced-motion: reduce)');
        this.lastText = 0;
        this.frame = 0;
        this.onVisibility = () => {
            this.particles = [];
            this.model.rebase();
            this.acceptedTracker.reset();
            this.savedTracker.reset();
        };
        document.addEventListener('visibilitychange', this.onVisibility);
        this.resizeObserver = new ResizeObserver(() => this.resize());
        this.resizeObserver.observe(this.canvas);
        this.tick = this.tick.bind(this);
        this.resize();
        this.frame = requestAnimationFrame(this.tick);
    }

    // The mode of the run being shown; before any run, the toggle's selection.
    get mode() { return this.model.snapshot?.settings?.submissionMode ?? this.selectedMode; }
    get kafka() { return this.mode !== 'DIRECT'; }

    setMode(mode) { this.selectedMode = mode; }

    update(snapshot) {
        const now = performance.now();
        const changedRun = this.model.runId !== snapshot.runId;
        if (changedRun) {
            this.particles = [];
            this.acceptedTracker.reset();
            this.savedTracker.reset();
            this.saved = null;
        }
        const pulse = this.model.update(snapshot, now);
        this.emit('request', pulse.count, now);
        const replies = this.acceptedTracker.update(snapshot.accepted, now);
        // Kafka mode: replies mean "written to Kafka". Direct mode: replies mean "saved".
        this.emit(this.kafka ? 'toKafka' : 'bypass', replies, now);
    }

    setSaved(runId, saved) {
        if (runId !== this.model.runId) return;
        const now = performance.now();
        this.saved = saved;
        if (saved == null) { this.savedTracker.disconnect(); return; }
        const count = this.savedTracker.update(saved, now);
        if (this.kafka) this.emit('toDatabase', count, now);
    }

    emit(segment, count, now) {
        if (!count || this.reduced.matches || document.hidden) return;
        // Equal-weight particles represent the full observed batch, bounded at 60 per update.
        const particles = Math.min(count, 60);
        for (let i = 0; i < particles; i++) {
            this.particles.push({ segment, born: now + i / particles * 450, lane: Math.random(), weight: count / particles });
        }
        this.particles = this.particles.slice(-480);
    }

    setConnections(health) {
        this.connections = { generator: health.generator, backend: health.backend };
    }

    disconnect() {
        this.model.disconnect();
        this.acceptedTracker.disconnect();
        this.particles = [];
    }

    resize() {
        const bounds = this.canvas.getBoundingClientRect();
        this.width = bounds.width;
        this.height = bounds.height;
        const ratio = Math.min(window.devicePixelRatio || 1, 2);
        this.canvas.width = Math.round(bounds.width * ratio);
        this.canvas.height = Math.round(bounds.height * ratio);
        this.ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
    }

    tick(now) {
        if (!document.hidden) {
            this.draw(now);
            if (now - this.lastText > 300) { this.renderText(now); this.lastText = now; }
        }
        this.frame = requestAnimationFrame(this.tick);
    }

    renderText(now) {
        const snapshot = this.model.snapshot;
        const finished = snapshot?.finishedAt != null;
        const rate = this.model.rate(now);
        const q = selector => this.root.querySelector(selector);
        q('[data-rate]').textContent = !snapshot ? '—' : finished ? '0' : rate === null ? '—' : Math.round(rate).toLocaleString();
        q('[data-target]').textContent = snapshot ? snapshot.settings.requestsPerMinute.toLocaleString() : '—';
        q('[data-density]').textContent = snapshot ? `${snapshot.inFlight} / ${snapshot.settings.maxInFlight ?? 'Unlimited'}` : '0 / —';
        const backlog = snapshot && this.kafka ? kafkaBacklog(snapshot.accepted, this.saved) : null;
        q('[data-backlog]').textContent = !this.kafka ? 'Not used' : backlog === null ? '—' : backlog.toLocaleString();
        const meter = q('[data-capacity]');
        meter.hidden = !!snapshot && snapshot.settings.maxInFlight == null;
        meter.value = snapshot?.settings.maxInFlight ? snapshot.inFlight / snapshot.settings.maxInFlight : 0;
        const path = this.kafka ? 'Through Kafka' : 'Direct to PostgreSQL';
        q('[data-traffic-status]').textContent = `${path} · ` + (!snapshot ? 'Waiting for a run' : finished ? 'Run finished' : !this.model.fresh(now) ? 'Waiting for live telemetry' : this.reduced.matches ? 'Live · reduced motion' : 'Live telemetry');
        q('[data-chart-note]').textContent = this.reduced.matches ? 'Reduced motion is enabled. The pipeline counts still update.' : 'Animation only: dots show observed count increases, not measured travel time. At high rates, dots represent groups.';
    }

    layout() {
        const w = this.width, h = this.height;
        const vertical = w < 600;
        const nodes = [];
        if (vertical) {
            const nodeW = Math.min(230, w - 90), nodeH = 118;
            const x = (w - nodeW) / 2 - 20;
            const gap = (h - 36 - 4 * nodeH) / 3;
            for (let i = 0; i < 4; i++) nodes.push({ x, y: 18 + i * (nodeH + gap), w: nodeW, h: nodeH });
            const bottom = n => ({ x: n.x + n.w / 2, y: n.y + n.h }), top = n => ({ x: n.x + n.w / 2, y: n.y });
            const side = x + nodeW + 34, right = n => ({ x: n.x + n.w, y: n.y + n.h / 2 });
            return {
                vertical, nodes,
                request: [bottom(nodes[0]), top(nodes[1])],
                toKafka: [bottom(nodes[1]), top(nodes[2])],
                toDatabase: [bottom(nodes[2]), top(nodes[3])],
                bypass: [right(nodes[1]), { x: side, y: right(nodes[1]).y }, { x: side, y: right(nodes[3]).y }, right(nodes[3])],
            };
        }
        const nodeW = Math.min(140, (w - 36 - 3 * 50) / 4), nodeH = 148;
        const gap = (w - 36 - 4 * nodeW) / 3;
        const y = (h - nodeH) / 2 + 22;
        for (let i = 0; i < 4; i++) nodes.push({ x: 18 + i * (nodeW + gap), y, w: nodeW, h: nodeH });
        const left = n => ({ x: n.x, y: n.y + n.h / 2 }), right = n => ({ x: n.x + n.w, y: n.y + n.h / 2 });
        const over = y - 52;
        return {
            vertical, nodes,
            request: [right(nodes[0]), left(nodes[1])],
            toKafka: [right(nodes[1]), left(nodes[2])],
            toDatabase: [right(nodes[2]), left(nodes[3])],
            bypass: [right(nodes[1]), { x: nodes[1].x + nodeW + gap / 2, y: over }, { x: nodes[3].x - gap / 2, y: over }, left(nodes[3])],
        };
    }

    draw(now) {
        const ctx = this.ctx, w = this.width, h = this.height;
        if (!w || !h) return;
        ctx.clearRect(0, 0, w, h);
        const layout = this.layout();
        const kafka = this.kafka;

        this.pipe(layout.request, true);
        if (kafka) {
            this.pipe(layout.toKafka, true);
            this.pipe(layout.toDatabase, true);
        } else {
            this.pipe(layout.toKafka, false);
            this.pipe(layout.toDatabase, false);
            this.pipe(layout.bypass, true);
        }

        this.particles = this.particles.filter(p => now - p.born < TRAVEL_MS);
        if (!this.reduced.matches && this.model.fresh(now)) {
            for (const p of this.particles) {
                const progress = (now - p.born) / TRAVEL_MS;
                if (progress < 0) continue;
                const { x, y } = pointAlong(layout[p.segment], progress, (p.lane - .5) * 14);
                ctx.fillStyle = p.segment === 'toDatabase' ? colors.kafka : colors.green;
                ctx.globalAlpha = Math.min(1, progress * 12, (1 - progress) * 12);
                ctx.beginPath(); ctx.arc(x, y, Math.min(5.5, 3 + Math.log2(p.weight + 1) * .4), 0, Math.PI * 2); ctx.fill();
            }
        }
        ctx.globalAlpha = 1;

        const snapshot = this.model.snapshot;
        const accepted = snapshot?.accepted ?? 0;
        const backlog = snapshot ? kafkaBacklog(accepted, this.saved) : null;
        const fmt = n => n == null ? '—' : n.toLocaleString();
        const backend = this.connections.backend;
        const [g, b, k, p] = layout.nodes;
        this.drawNode(g, { kind: 'generator', title: 'Generator', subtitle: 'Creates requests',
            count: fmt(snapshot?.dispatched ?? 0), caption: 'requests sent', tone: healthTone(this.connections.generator) });
        this.drawNode(b, { kind: 'backend', title: 'Backend', subtitle: kafka ? 'Replies 202' : 'Replies 201',
            count: fmt(accepted), caption: kafka ? 'stored in Kafka' : 'saved replies', tone: healthTone(backend) });
        this.drawNode(k, { kind: 'kafka', title: 'Kafka', subtitle: kafka ? 'Holds the backlog' : 'Not in this path',
            count: kafka ? fmt(backlog) : '—', caption: kafka ? 'waiting to save' : 'bypassed',
            tone: kafka ? { accent: colors.kafka, surface: '#eaf2f6', label: 'Not health-checked' }
                        : { accent: '#9aa6a1', surface: '#f3f4f3', label: 'Bypassed' } });
        this.drawNode(p, { kind: 'database', title: 'PostgreSQL', subtitle: kafka ? 'Saved in batches' : 'Saved per request',
            count: fmt(snapshot ? this.saved : 0), caption: 'cases saved', tone: healthTone(backend) });
    }

    pipe(points, active) {
        const ctx = this.ctx;
        ctx.lineCap = 'round'; ctx.lineJoin = 'round';
        const stroke = (color, width, dash = []) => {
            ctx.setLineDash(dash); ctx.strokeStyle = color; ctx.lineWidth = width;
            ctx.beginPath(); ctx.moveTo(points[0].x, points[0].y);
            for (const point of points.slice(1)) ctx.lineTo(point.x, point.y);
            ctx.stroke();
        };
        if (active) {
            stroke('#b8cbc3', 30); stroke('#e8f0eb', 26); stroke('#f7faf8', 16);
            // Direction chevron at the middle of the path stays readable when idle.
            const mid = pointAlong(points, .5, 0), ahead = pointAlong(points, .52, 0);
            const angle = Math.atan2(ahead.y - mid.y, ahead.x - mid.x);
            ctx.save(); ctx.translate(mid.x, mid.y); ctx.rotate(angle);
            ctx.strokeStyle = '#90aa9e'; ctx.lineWidth = 2;
            ctx.beginPath(); ctx.moveTo(-4, -5); ctx.lineTo(2, 0); ctx.lineTo(-4, 5); ctx.stroke();
            ctx.restore();
        } else {
            stroke('#d5dcd8', 2, [5, 6]);
        }
        ctx.setLineDash([]);
    }

    drawNode(node, { kind, title, subtitle, count, caption, tone }) {
        const ctx = this.ctx, { x, y, w, h } = node, center = x + w / 2;
        const compact = h < 140;
        ctx.fillStyle = tone.surface; ctx.strokeStyle = tone.accent; ctx.lineWidth = 1.5;
        ctx.beginPath(); ctx.roundRect(x, y, w, h, 14); ctx.fill(); ctx.stroke();
        ctx.strokeStyle = tone.accent; ctx.fillStyle = tone.accent; ctx.lineWidth = 2;
        const iy = y + (compact ? 12 : 16);
        if (kind === 'backend') {
            for (let i = 0; i < 3; i++) {
                ctx.beginPath(); ctx.roundRect(center - 17, iy + i * 9, 34, 6, 2); ctx.stroke();
                ctx.beginPath(); ctx.arc(center + 11, iy + 3 + i * 9, 1, 0, Math.PI * 2); ctx.fill();
            }
        } else if (kind === 'kafka') {
            // An append-only log: rows of segments.
            for (let row = 0; row < 3; row++) for (let col = 0; col < 4 - (row === 2 ? 1 : 0); col++) {
                ctx.beginPath(); ctx.roundRect(center - 20 + col * 10.5, iy + row * 9, 8, 6, 1.5); ctx.fill();
            }
        } else if (kind === 'database') {
            ctx.beginPath(); ctx.ellipse(center, iy + 3, 15, 4, 0, 0, Math.PI * 2); ctx.stroke();
            ctx.beginPath(); ctx.moveTo(center - 15, iy + 3); ctx.lineTo(center - 15, iy + 22);
            ctx.ellipse(center, iy + 22, 15, 4, 0, Math.PI, 0, true); ctx.lineTo(center + 15, iy + 3); ctx.stroke();
            ctx.beginPath(); ctx.ellipse(center, iy + 12.5, 15, 4, 0, 0, Math.PI); ctx.stroke();
        } else {
            for (let row = 0; row < 3; row++) for (let col = 0; col < 3; col++) {
                ctx.beginPath(); ctx.arc(center - 10 + col * 10, iy + 4 + row * 9, 2.6, 0, Math.PI * 2); ctx.fill();
            }
        }
        const countFont = w < 130 ? 22 : 26;
        ctx.textAlign = 'center'; ctx.fillStyle = colors.ink;
        ctx.font = `600 ${compact ? 17 : 18}px "Segoe UI", sans-serif`; ctx.fillText(title, center, y + (compact ? 54 : 66));
        if (!compact) {
            ctx.fillStyle = colors.muted; ctx.font = '12px "Segoe UI", sans-serif'; ctx.fillText(subtitle, center, y + 84);
        }
        ctx.fillStyle = tone.accent; ctx.font = `600 ${countFont}px "Segoe UI", sans-serif`; ctx.fillText(count, center, y + (compact ? 84 : 116));
        ctx.fillStyle = colors.muted; ctx.font = '12px "Segoe UI", sans-serif'; ctx.fillText(caption, center, y + (compact ? 104 : 135));
        ctx.fillStyle = tone.accent; ctx.font = '600 12px "Segoe UI", sans-serif'; ctx.fillText(tone.label, center, y - 10);
    }

    destroy() {
        cancelAnimationFrame(this.frame);
        this.resizeObserver.disconnect();
        document.removeEventListener('visibilitychange', this.onVisibility);
    }
}

function healthTone(live) {
    return live === null
        ? { accent: '#788882', surface: '#eff1f0', label: 'Checking' }
        : live
            ? { accent: '#16814e', surface: '#eaf4ee', label: 'Live' }
            : { accent: '#bd3029', surface: '#fbefed', label: 'Offline' };
}

// Point at a fraction of a polyline's length, offset sideways to spread particles across the pipe.
function pointAlong(points, fraction, offset) {
    const lengths = [];
    let total = 0;
    for (let i = 1; i < points.length; i++) {
        const length = Math.hypot(points[i].x - points[i - 1].x, points[i].y - points[i - 1].y);
        lengths.push(length); total += length;
    }
    let distance = Math.max(0, Math.min(1, fraction)) * total;
    for (let i = 0; i < lengths.length; i++) {
        if (distance <= lengths[i] || i === lengths.length - 1) {
            const a = points[i], b = points[i + 1], t = lengths[i] ? distance / lengths[i] : 0;
            const nx = -(b.y - a.y) / (lengths[i] || 1), ny = (b.x - a.x) / (lengths[i] || 1);
            return { x: a.x + (b.x - a.x) * t + nx * offset, y: a.y + (b.y - a.y) * t + ny * offset };
        }
        distance -= lengths[i];
    }
    return points[0];
}
