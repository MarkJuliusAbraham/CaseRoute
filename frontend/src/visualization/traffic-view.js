import { TrafficModel, DeltaTracker, kafkaBacklog } from './traffic-model.js';

const colors = {
    ink: '#192b2a', muted: '#4c615e', line: '#9db6ab', idle: '#cfd8d3',
    intake: '#146650', processing: '#2f6a88', kafkaSurface: '#eaf2f6',
};
const TRAVEL_MS = 1300;

// The system as a graph. Nodes are services; edges are the paths data takes between them.
//   Intake:     Generator -> Backend -> Kafka (cases.submitted) -> writer -> PostgreSQL
//               (direct mode: Backend -> PostgreSQL)
//   Processing: PostgreSQL outbox -> relay -> Kafka (cases.saved) -> Processor -> Kafka (cases.processed)
//               -> backend writer -> PostgreSQL. The Processor only talks to Kafka.
// Positions are fractions of the canvas, so the graph is laid out in two dimensions rather than one line.
const LAYOUTS = {
    wide: {
        nodes: {
            generator: [0.085, 0.55], backend: [0.33, 0.55], kafka: [0.60, 0.22],
            database: [0.60, 0.80], processor: [0.895, 0.22],
        },
        size: [140, 92], kafkaSize: [176, 124],
    },
    // Column, then Processor and PostgreSQL side by side under Kafka. The direct-insert edge curves around Kafka.
    narrow: {
        nodes: {
            generator: [0.50, 0.08], backend: [0.50, 0.28], kafka: [0.50, 0.53],
            database: [0.76, 0.86], processor: [0.24, 0.86],
        },
        size: [136, 92], kafkaSize: [160, 122],
        bends: { bypass: -62, toDatabase: -40, relay: 4, toProcessor: 28, results: 4 },
        labelSides: { relay: -1 }, // keep the outbox relay label clear of the writers label
        labelAt: { relay: .3, results: .72 }, // stagger the two middle labels vertically
    },
};

// bend: sideways offset of the curve's midpoint, so opposite-direction edges between the same two nodes
// sit side by side instead of on top of each other.
const EDGES = [
    { id: 'request', from: 'generator', to: 'backend', bend: 0, label: 'HTTP POST', kind: 'intake' },
    { id: 'toKafka', from: 'backend', to: 'kafka', bend: -18, label: 'cases.submitted', kind: 'intake', mode: 'KAFKA' },
    { id: 'bypass', from: 'backend', to: 'database', bend: 18, label: 'direct insert', kind: 'intake', mode: 'DIRECT' },
    { id: 'toDatabase', from: 'kafka', to: 'database', bend: -34, label: 'writers', kind: 'intake' },
    { id: 'relay', from: 'database', to: 'kafka', bend: -34, label: 'outbox relay', kind: 'processing' },
    { id: 'toProcessor', from: 'kafka', to: 'processor', bend: -28, label: 'cases.saved', kind: 'processing' },
    { id: 'results', from: 'processor', to: 'kafka', bend: -28, label: 'cases.processed', kind: 'processing' },
];

export class TrafficView {
    constructor(root) {
        this.root = root;
        this.canvas = root.querySelector('canvas');
        this.ctx = this.canvas.getContext('2d');
        this.model = new TrafficModel();
        this.acceptedTracker = new DeltaTracker();
        this.savedTracker = new DeltaTracker();
        this.processedTracker = new DeltaTracker();
        this.publishedTracker = new DeltaTracker();
        this.stats = null;
        this.statsAt = null; // when pipeline counts were last polled successfully
        this.selectedMode = 'KAFKA';
        this.connections = { generator: null, backend: null, processor: null };
        this.particles = [];
        this.lastActivity = {}; // edge id -> time traffic was last seen on it
        this.reduced = window.matchMedia('(prefers-reduced-motion: reduce)');
        this.lastText = 0;
        this.frame = 0;
        this.onVisibility = () => {
            this.particles = [];
            this.model.rebase();
            this.acceptedTracker.reset();
            this.savedTracker.reset();
            this.processedTracker.reset();
            this.publishedTracker.reset();
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
        if (this.model.runId !== snapshot.runId) {
            this.particles = [];
            this.acceptedTracker.reset();
            this.savedTracker.reset();
            this.processedTracker.reset();
            this.publishedTracker.reset();
            this.stats = null;
        }
        const pulse = this.model.update(snapshot, now);
        this.emit('request', pulse.count, now);
        const replies = this.acceptedTracker.update(snapshot.accepted, now);
        // Kafka mode: an accepted reply means "written to cases.submitted". Direct mode: "saved".
        this.emit(this.kafka ? 'toKafka' : 'bypass', replies, now);
    }

    get saved() { return this.stats?.saved ?? null; }
    get processed() { return this.stats == null ? null : this.stats.routed + this.stats.failed; }
    // Saved cases whose outbox row the relay has already published to cases.saved.
    get published() { return this.stats == null ? null : Math.max(0, this.stats.saved - this.stats.awaitingRelay); }
    // Published to cases.saved but not yet processed (waiting in the topic or being worked on).
    get toProcess() { return this.stats == null ? null : Math.max(0, this.published - this.processed); }

    setStats(runId, stats) {
        if (runId !== this.model.runId) return;
        const now = performance.now();
        this.stats = stats;
        if (stats != null) this.statsAt = now;
        if (stats == null) {
            this.savedTracker.disconnect(); this.processedTracker.disconnect(); this.publishedTracker.disconnect();
            return;
        }
        const saved = this.savedTracker.update(stats.saved, now);
        if (this.kafka) this.emit('toDatabase', saved, now);
        this.emit('relay', this.publishedTracker.update(this.published, now), now);
        // Processed counts come from the database, so these results have made the whole trip:
        // to the processor, back through cases.processed, and into PostgreSQL via the writers.
        const processed = this.processedTracker.update(this.processed, now);
        this.emit('toProcessor', processed, now);
        this.emit('results', processed, now);
        this.emit('toDatabase', processed, now);
    }

    emit(edge, count, now) {
        if (!count) return;
        this.lastActivity[edge] = now;
        if (this.reduced.matches || document.hidden) return;
        // Equal-weight particles represent the full observed batch, bounded at 40 per update.
        const particles = Math.min(count, 40);
        for (let i = 0; i < particles; i++) {
            this.particles.push({ edge, born: now + i / particles * 450, lane: Math.random(), weight: count / particles });
        }
        this.particles = this.particles.slice(-520);
    }

    setConnections(health) {
        this.connections = { generator: health.generator, backend: health.backend, processor: health.processor };
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
        q('[data-processing-backlog]').textContent = snapshot && this.stats ? this.stats.open.toLocaleString() : '—';
        const meter = q('[data-capacity]');
        meter.hidden = !!snapshot && snapshot.settings.maxInFlight == null;
        meter.value = snapshot?.settings.maxInFlight ? snapshot.inFlight / snapshot.settings.maxInFlight : 0;
        const path = this.kafka ? 'Intake through Kafka' : 'Intake direct to PostgreSQL';
        q('[data-traffic-status]').textContent = `${path} · ` + (!snapshot ? 'Waiting for a run' : finished ? (this.stats && (this.stats.open > 0 || this.stats.awaitingRelay > 0) ? 'Run finished · processing continues' : 'Run finished') : !this.model.fresh(now) ? 'Waiting for live telemetry' : this.reduced.matches ? 'Live · reduced motion' : 'Live telemetry');
        q('[data-chart-note]').textContent = this.reduced.matches ? 'Reduced motion is enabled. The graph counts still update.' : 'Animation only: moving dashes show direction (faster while traffic flows); dots show observed count increases, not measured travel time. Green is intake, blue is processing.';
    }

    // Node rectangles and edge curves for the current canvas size.
    layout() {
        const w = this.width, h = this.height;
        const spec = w < 640 ? LAYOUTS.narrow : LAYOUTS.wide;
        const nodes = {};
        for (const [name, [fx, fy]] of Object.entries(spec.nodes)) {
            const [nw, nh] = name === 'kafka' ? spec.kafkaSize : spec.size;
            const cx = Math.min(w - nw / 2 - 8, Math.max(nw / 2 + 8, fx * w));
            const cy = Math.min(h - nh / 2 - 8, Math.max(nh / 2 + 18, fy * h));
            nodes[name] = { cx, cy, x: cx - nw / 2, y: cy - nh / 2, w: nw, h: nh };
        }
        const edges = {};
        for (const base of EDGES) {
            const edge = { ...base, bend: spec.bends?.[base.id] ?? base.bend, labelSide: spec.labelSides?.[base.id],
                labelAt: spec.labelAt?.[base.id] ?? .5 };
            const a = nodes[edge.from], b = nodes[edge.to];
            // Control point: midpoint pushed sideways (perpendicular to a -> b) by the edge's bend.
            const mx = (a.cx + b.cx) / 2, my = (a.cy + b.cy) / 2;
            const dx = b.cx - a.cx, dy = b.cy - a.cy, len = Math.hypot(dx, dy) || 1;
            const control = { x: mx - dy / len * edge.bend * 2, y: my + dx / len * edge.bend * 2 };
            const start = boxExit(a, control), end = boxExit(b, control);
            const points = [];
            for (let i = 0; i <= 24; i++) points.push(quadratic(start, control, end, i / 24));
            edges[edge.id] = { ...edge, start, control, end, points };
        }
        return { nodes, edges };
    }

    draw(now) {
        const ctx = this.ctx, w = this.width, h = this.height;
        if (!w || !h) return;
        ctx.clearRect(0, 0, w, h);
        const { nodes, edges } = this.layout();

        // Edges first, so nodes sit on top of their ends.
        for (const edge of Object.values(edges)) this.drawEdge(edge, !edge.mode || edge.mode === this.mode, now);

        this.particles = this.particles.filter(p => now - p.born < TRAVEL_MS);
        // Each edge's dots follow the data that drives them: request/intake edges follow the generator's live
        // updates (which stop when the run ends); writer and processing edges follow the polled pipeline counts,
        // which keep updating after the run while the backlog drains.
        const generatorFresh = this.model.fresh(now);
        const statsFresh = this.statsAt != null && now - this.statsAt <= 2500;
        if (!this.reduced.matches) {
            for (const p of this.particles) {
                const progress = (now - p.born) / TRAVEL_MS;
                if (progress < 0) continue;
                const edge = edges[p.edge];
                const polled = edge.kind === 'processing' || edge.id === 'toDatabase';
                if (!(polled ? statsFresh : generatorFresh)) continue;
                const { x, y } = pointAlong(edge.points, progress, (p.lane - .5) * 6);
                ctx.fillStyle = edge.kind === 'processing' ? colors.processing : colors.intake;
                ctx.globalAlpha = Math.min(1, progress * 10, (1 - progress) * 10);
                ctx.beginPath(); ctx.arc(x, y, Math.min(4.5, 2.6 + Math.log2(p.weight + 1) * .35), 0, Math.PI * 2); ctx.fill();
            }
        }
        ctx.globalAlpha = 1;
        for (const edge of Object.values(edges)) this.drawEdgeLabel(edge, !edge.mode || edge.mode === this.mode);

        const snapshot = this.model.snapshot;
        const kafka = this.kafka;
        const accepted = snapshot?.accepted ?? 0;
        const fmt = n => n == null ? '—' : n.toLocaleString();
        const backend = this.connections.backend;
        const failed = this.stats?.failed ?? 0;
        const outbox = this.stats?.awaitingRelay ?? 0;

        this.drawNode(nodes.generator, { kind: 'generator', title: 'Generator', count: fmt(snapshot?.dispatched ?? 0),
            caption: 'requests sent', tone: healthTone(this.connections.generator) });
        this.drawNode(nodes.backend, { kind: 'backend', title: 'Backend', count: fmt(accepted),
            caption: kafka ? 'accepted (202)' : 'saved (201)', tone: healthTone(backend) });
        this.drawNode(nodes.database, { kind: 'database', title: 'PostgreSQL', count: fmt(snapshot ? this.saved : 0),
            caption: outbox ? `cases saved · ${outbox.toLocaleString()} in outbox` : 'cases saved', tone: healthTone(backend) });
        this.drawNode(nodes.processor, { kind: 'processor', title: 'Processor', count: fmt(snapshot ? this.processed : 0),
            caption: failed ? `processed · ${failed.toLocaleString()} failed` : 'cases processed',
            tone: healthTone(this.connections.processor) });
        // Kafka is the hub: one row per topic with what is waiting in it.
        const submittedBacklog = snapshot && kafka ? kafkaBacklog(accepted, this.saved) : null;
        this.drawKafka(nodes.kafka, [
            ['cases.submitted', kafka ? fmt(submittedBacklog) : 'unused'],
            ['cases.saved', fmt(snapshot ? this.toProcess : null)],
            ['cases.processed', 'results'],
        ]);
    }

    drawEdge(edge, active, now) {
        const ctx = this.ctx;
        ctx.beginPath(); ctx.moveTo(edge.start.x, edge.start.y);
        ctx.quadraticCurveTo(edge.control.x, edge.control.y, edge.end.x, edge.end.y);
        ctx.lineCap = 'round';
        ctx.setLineDash(active ? [] : [5, 6]);
        ctx.strokeStyle = active ? colors.line : colors.idle; ctx.lineWidth = active ? 2.4 : 1.6;
        ctx.stroke();
        ctx.setLineDash([]);
        // Flow: dashes always drift along active edges in the direction data moves. Slow when idle,
        // fast while traffic was seen on this edge in the last few seconds.
        if (active && !this.reduced.matches) {
            const busy = now - (this.lastActivity[edge.id] ?? -Infinity) < 3000;
            const speed = busy ? 70 : 14; // pixels per second
            ctx.beginPath(); ctx.moveTo(edge.start.x, edge.start.y);
            ctx.quadraticCurveTo(edge.control.x, edge.control.y, edge.end.x, edge.end.y);
            ctx.setLineDash([3, 11]); ctx.lineDashOffset = -(now / 1000) * speed;
            ctx.strokeStyle = edge.kind === 'processing' ? colors.processing : colors.intake;
            ctx.globalAlpha = busy ? .75 : .4; ctx.lineWidth = 2.4;
            ctx.stroke();
            ctx.setLineDash([]); ctx.lineDashOffset = 0; ctx.globalAlpha = 1;
        }
        // Arrowhead along the curve's final direction.
        const angle = Math.atan2(edge.end.y - edge.control.y, edge.end.x - edge.control.x);
        ctx.save(); ctx.translate(edge.end.x, edge.end.y); ctx.rotate(angle);
        ctx.fillStyle = active ? colors.line : colors.idle;
        ctx.beginPath(); ctx.moveTo(0, 0); ctx.lineTo(-9, -4.5); ctx.lineTo(-9, 4.5); ctx.closePath(); ctx.fill();
        ctx.restore();
    }

    // Labels sit beside their edge, not on it, so they never cover the line or its arrowhead.
    drawEdgeLabel(edge, active) {
        const ctx = this.ctx;
        const t = edge.labelAt;
        const mid = quadratic(edge.start, edge.control, edge.end, t);
        ctx.font = '11px "Segoe UI", sans-serif';
        const width = ctx.measureText(edge.label).width + 12, height = 18;
        // The label moves along the curve's normal at t, to the outside of the bend (above for straight edges),
        // far enough for the whole pill to clear the line.
        const tx = 2 * (1 - t) * (edge.control.x - edge.start.x) + 2 * t * (edge.end.x - edge.control.x);
        const ty = 2 * (1 - t) * (edge.control.y - edge.start.y) + 2 * t * (edge.end.y - edge.control.y);
        const tl = Math.hypot(tx, ty) || 1;
        let nx = -ty / tl, ny = tx / tl;
        const side = edge.labelSide ?? (edge.bend === 0 ? (ny > 0 ? -1 : 1) : Math.sign(edge.bend));
        nx *= side; ny *= side;
        const clearance = Math.abs(nx) * width / 2 + Math.abs(ny) * height / 2 + 5;
        mid.x += nx * clearance; mid.y += ny * clearance;
        // Keep the whole pill inside the canvas.
        mid.x = Math.min(this.width - width / 2 - 2, Math.max(width / 2 + 2, mid.x));
        ctx.globalAlpha = active ? 1 : .55;
        ctx.fillStyle = '#ffffff'; ctx.strokeStyle = active ? '#d7e2dd' : '#e4e9e6'; ctx.lineWidth = 1;
        ctx.beginPath(); ctx.roundRect(mid.x - width / 2, mid.y - height / 2, width, height, 9); ctx.fill(); ctx.stroke();
        ctx.fillStyle = edge.kind === 'processing' ? colors.processing : colors.muted;
        ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
        ctx.fillText(edge.label, mid.x, mid.y + .5);
        ctx.textBaseline = 'alphabetic'; ctx.globalAlpha = 1;
    }

    card(node, tone) {
        const ctx = this.ctx;
        ctx.fillStyle = tone.surface; ctx.strokeStyle = tone.accent; ctx.lineWidth = 1.5;
        ctx.beginPath(); ctx.roundRect(node.x, node.y, node.w, node.h, 12); ctx.fill(); ctx.stroke();
        // Status sits above the box's right edge, clear of edges arriving at the box's centre.
        if (tone.label) {
            ctx.fillStyle = tone.accent; ctx.font = '600 11px "Segoe UI", sans-serif'; ctx.textAlign = 'right';
            ctx.fillText(tone.label, node.x + node.w - 4, node.y - 6);
        }
    }

    drawNode(node, { kind, title, count, caption, tone }) {
        const ctx = this.ctx, { x, y, w } = node;
        this.card(node, tone);
        drawIcon(ctx, kind, x + 14, y + 12, tone.accent);
        ctx.textAlign = 'left'; ctx.fillStyle = colors.ink; ctx.font = '600 14px "Segoe UI", sans-serif';
        ctx.fillText(title, x + 44, y + 28);
        ctx.fillStyle = tone.accent; ctx.font = `600 ${w < 150 ? 20 : 22}px "Segoe UI", sans-serif`;
        ctx.fillText(count, x + 14, y + 64);
        ctx.fillStyle = colors.muted; ctx.font = '11px "Segoe UI", sans-serif';
        ctx.fillText(fit(ctx, caption, w - 24), x + 14, y + 82);
    }

    drawKafka(node, topics) {
        const ctx = this.ctx, { x, y, w } = node;
        // No status line: nothing health-checks Kafka directly.
        const tone = { accent: colors.processing, surface: colors.kafkaSurface, label: '' };
        this.card(node, tone);
        drawIcon(ctx, 'kafka', x + 14, y + 12, tone.accent);
        ctx.textAlign = 'left'; ctx.fillStyle = colors.ink; ctx.font = '600 14px "Segoe UI", sans-serif';
        ctx.fillText('Kafka', x + 44, y + 28);
        ctx.fillStyle = colors.muted; ctx.font = '11px "Segoe UI", sans-serif'; ctx.textAlign = 'right';
        ctx.fillText('waiting', x + w - 12, y + 28);
        topics.forEach(([name, value], i) => {
            const rowY = y + 54 + i * 22;
            ctx.strokeStyle = '#d3e2ea'; ctx.lineWidth = 1;
            ctx.beginPath(); ctx.moveTo(x + 12, rowY - 15); ctx.lineTo(x + w - 12, rowY - 15); ctx.stroke();
            ctx.textAlign = 'left'; ctx.fillStyle = colors.ink; ctx.font = '12px Consolas, monospace';
            ctx.fillText(name, x + 12, rowY);
            ctx.textAlign = 'right'; ctx.fillStyle = tone.accent; ctx.font = '600 12px "Segoe UI", sans-serif';
            ctx.fillText(value, x + w - 12, rowY);
        });
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

// Small 22px icons drawn at (x, y), top-left.
function drawIcon(ctx, kind, x, y, color) {
    ctx.save();
    ctx.strokeStyle = color; ctx.fillStyle = color; ctx.lineWidth = 1.8;
    const cx = x + 11, cy = y + 11;
    if (kind === 'backend') {
        for (let i = 0; i < 3; i++) {
            ctx.beginPath(); ctx.roundRect(x + 1, y + 2 + i * 7, 20, 5, 1.5); ctx.stroke();
            ctx.beginPath(); ctx.arc(x + 16, y + 4.5 + i * 7, .9, 0, Math.PI * 2); ctx.fill();
        }
    } else if (kind === 'kafka') {
        for (let row = 0; row < 3; row++) for (let col = 0; col < 4 - (row === 2 ? 1 : 0); col++) {
            ctx.beginPath(); ctx.roundRect(x + col * 5.6, y + 3 + row * 6.5, 4.4, 4.4, 1); ctx.fill();
        }
    } else if (kind === 'database') {
        ctx.beginPath(); ctx.ellipse(cx, y + 4, 9, 3, 0, 0, Math.PI * 2); ctx.stroke();
        ctx.beginPath(); ctx.moveTo(cx - 9, y + 4); ctx.lineTo(cx - 9, y + 18);
        ctx.ellipse(cx, y + 18, 9, 3, 0, Math.PI, 0, true); ctx.lineTo(cx + 9, y + 4); ctx.stroke();
        ctx.beginPath(); ctx.ellipse(cx, y + 11, 9, 3, 0, 0, Math.PI); ctx.stroke();
    } else if (kind === 'processor') {
        ctx.beginPath(); ctx.arc(cx, cy, 5.5, 0, Math.PI * 2); ctx.stroke();
        for (let i = 0; i < 6; i++) {
            const a = i * Math.PI / 3;
            ctx.beginPath(); ctx.moveTo(cx + Math.cos(a) * 6.5, cy + Math.sin(a) * 6.5);
            ctx.lineTo(cx + Math.cos(a) * 9.5, cy + Math.sin(a) * 9.5); ctx.stroke();
        }
    } else {
        for (let row = 0; row < 3; row++) for (let col = 0; col < 3; col++) {
            ctx.beginPath(); ctx.arc(x + 4 + col * 7, y + 4 + row * 7, 1.9, 0, Math.PI * 2); ctx.fill();
        }
    }
    ctx.restore();
}

// Shortens text with an ellipsis so it fits the given width.
function fit(ctx, text, width) {
    if (ctx.measureText(text).width <= width) return text;
    let s = text;
    while (s.length > 1 && ctx.measureText(s + '…').width > width) s = s.slice(0, -1);
    return s + '…';
}

function quadratic(a, c, b, t) {
    const u = 1 - t;
    return { x: u * u * a.x + 2 * u * t * c.x + t * t * b.x, y: u * u * a.y + 2 * u * t * c.y + t * t * b.y };
}

// Where the line from the box centre towards `toward` leaves the box (plus a small gap).
function boxExit(box, toward) {
    const dx = toward.x - box.cx, dy = toward.y - box.cy;
    const sx = dx === 0 ? Infinity : (box.w / 2 + 4) / Math.abs(dx);
    const sy = dy === 0 ? Infinity : (box.h / 2 + 4) / Math.abs(dy);
    const s = Math.min(sx, sy);
    return { x: box.cx + dx * s, y: box.cy + dy * s };
}

// Point at a fraction of a polyline's length, offset sideways to spread particles across the edge.
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
