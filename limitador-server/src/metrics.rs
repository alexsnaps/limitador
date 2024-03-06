use std::collections::HashMap;
use std::ops;
use std::time::Instant;
use tracing::span::{Attributes, Id};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

#[derive(Debug, Clone, Copy)]
pub struct Timings {
    idle: u64,
    busy: u64,
    last: Instant,
}

impl Timings {
    fn new() -> Self {
        Self {
            idle: 0,
            busy: 0,
            last: Instant::now(),
        }
    }
}

impl ops::Add for Timings {
    type Output = Self;

    fn add(self, _rhs: Self) -> Self {
        Self {
            busy: self.busy + _rhs.busy,
            idle: self.idle + _rhs.idle,
            last: if self.last < _rhs.last {
                self.last
            } else {
                _rhs.last
            },
        }
    }
}

impl ops::AddAssign for Timings {
    fn add_assign(&mut self, _rhs: Self) {
        *self = *self + _rhs
    }
}

#[derive(Debug)]
struct SpanState {
    group_times: HashMap<String, Timings>,
}

impl SpanState {
    fn new(group: String) -> Self {
        let mut hm = HashMap::new();
        hm.insert(group, Timings::new());
        Self { group_times: hm }
    }

    fn increment(&mut self, group: String, timings: Timings) {
        self.group_times
            .entry(group)
            .and_modify(|x| *x += timings)
            .or_insert(timings);
    }
}

pub struct MetricsGroup {
    consumer: fn(Timings),
    records: Vec<String>,
}

impl MetricsGroup {
    pub fn new(consumer: fn(Timings), records: Vec<String>) -> Self {
        Self {
            consumer,
            records,
        }
    }
}

pub struct MetricsLayer {
    groups: HashMap<String, MetricsGroup>,
}

impl MetricsLayer {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
        }
    }

    pub fn gather(mut self, aggregate: &str, consumer: fn(Timings), records: Vec<&str>) -> Self {
        // TODO: does not handle case where aggregate already exists
        let rec = records.iter().map(|r| r.to_string()).collect();
        self.groups
            .entry(aggregate.to_string())
            .or_insert(MetricsGroup::new(consumer, rec));
        self
    }
}

impl<S> Layer<S> for MetricsLayer
where
    S: Subscriber,
    S: for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, _attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("Span not found, this is a bug");
        let mut extensions = span.extensions_mut();
        let name = span.name();

        // option 1
        // if is aggregate, append marker
        // if marker present + record -> start timing

        // option 2
        // if is record, iterate through parents
        // if aggregate present -> start timing

        // if this is an aggregate span
        // or parent is an aggregate span
        // append new marker

        // should_rate_limit -> datastore
        // check_and_update -> datastore

        // if there's a parent
        if let Some(parent) = span.parent() {
            // if the parent has SpanState propagate to this span
            if let Some(span_state) = parent.extensions_mut().get_mut::<SpanState>() {
                extensions.insert(SpanState {
                    group_times: span_state.group_times.clone(),
                });
            }
        }

        // if we are an aggregator
        if self.groups.contains_key(name) {
            if let Some(span_state) = extensions.get_mut::<SpanState>() {
                // if the SpanState has come from parent and we are not present, add ourselves
                span_state
                    .group_times
                    .entry(name.to_string())
                    .or_insert(Timings::new());
            } else {
                // otherwise create a new SpanState with ourselves
                extensions.insert(SpanState::new(name.to_string()))
            }
        }

        // if timing is already set (which it shouldn't be)
        // don't create it again
        if !extensions.get_mut::<Timings>().is_none() {
            return;
        }

        if let Some(span_state) = extensions.get_mut::<SpanState>() {
            // either we are an aggregator or nested within one
            for group in span_state.group_times.keys() {
                for record in &self.groups.get(group).unwrap().records {
                    if name == record {
                        extensions.insert(Timings::new());
                        return;
                    }
                }
            }
            // if here we are an intermediate span that should not be recorded
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("Span not found, this is a bug");
        span.name();
        let mut extensions = span.extensions_mut();

        if let Some(timings) = extensions.get_mut::<Timings>() {
            let now = Instant::now();
            timings.idle += (now - timings.last).as_nanos() as u64;
            timings.last = now;
        }
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("Span not found, this is a bug");
        let mut extensions = span.extensions_mut();

        if let Some(timings) = extensions.get_mut::<Timings>() {
            let now = Instant::now();
            timings.busy += (now - timings.last).as_nanos() as u64;
            timings.last = now;
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("Span not found, this is a bug");
        let mut extensions = span.extensions_mut();
        let name = span.name();

        // if timings set + record -> add to total??? marker/field?
        // if aggregate + marker/field? -> consume()

        if let Some(timing) = extensions.get_mut::<Timings>() {
            let mut t = *timing;
            t.idle += (Instant::now() - t.last).as_nanos() as u64;

            if let Some(span_state) = extensions.get_mut::<SpanState>() {
                let group_times = span_state.group_times.clone();
                'aggregate: for group in group_times.keys() {
                    for record in &self.groups.get(group).unwrap().records {
                        if name == record {
                            span_state.increment(group.to_string(), t);
                            continue 'aggregate;
                        }
                    }
                }
                // IF we are aggregator
                // CONSUME
                match self.groups.get(name) {
                    Some(metrics_group) => {
                        (metrics_group.consumer)(*span_state.group_times.get(name).unwrap())
                    }
                    _ => (),
                }
            }
        }
    }
}

// mylayer.gather("aggregate_on", timings| pr.vomit(timings), ["datastore"])

// mylayer.gather("aggregate_on", ["datastore"]).consumer("aggregate_on", |timings| pr.vomit(timings))

// write a consumer function, takes a function that does something with the timings
// Fn
// fn consumer(timings: Timings) -> () {}
