use anyhow::{Result, ensure};
use std::{collections::BTreeMap, fmt, sync::mpsc};
use tracing::{
    Event, Subscriber,
    field::Visit,
    span::{Attributes, Id, Record},
};
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt},
    registry::{LookupSpan, SpanRef},
};

#[derive(Clone, Debug, Default)]
struct Fields(BTreeMap<&'static str, String>);

impl Visit for Fields {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.0.insert(field.name(), value.to_owned());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        self.0.insert(field.name(), format!("{value:?}"));
    }
}

#[derive(Debug)]
pub(crate) struct SpanSnapshot {
    pub(crate) id: Id,
    pub(crate) name: &'static str,
    pub(crate) fields: BTreeMap<&'static str, String>,
}

#[derive(Debug)]
pub(crate) struct Observation {
    pub(crate) name: &'static str,
    pub(crate) fields: BTreeMap<&'static str, String>,
    pub(crate) scope: Vec<SpanSnapshot>,
}

fn snapshot<S>(span: &SpanRef<'_, S>) -> SpanSnapshot
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    SpanSnapshot {
        id: span.id(),
        name: span.name(),
        fields: span
            .extensions()
            .get::<Fields>()
            .cloned()
            .unwrap_or_default()
            .0,
    }
}

struct Capture(mpsc::Sender<Observation>);

impl<S> Layer<S> for Capture
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        if let Some(fields) = span.extensions_mut().get_mut::<Fields>() {
            values.record(fields);
        }
    }

    fn on_follows_from(&self, id: &Id, follows: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let _ = self.0.send(Observation {
            name: "span.follows_from",
            fields: BTreeMap::from([("follows_from", follows.into_u64().to_string())]),
            scope: span
                .scope()
                .from_root()
                .map(|span| snapshot(&span))
                .collect(),
        });
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let _ = self.0.send(Observation {
            name: "span.close",
            fields: BTreeMap::new(),
            scope: span
                .scope()
                .from_root()
                .map(|span| snapshot(&span))
                .collect(),
        });
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        span.extensions_mut().insert(fields.clone());
        let _ = self.0.send(Observation {
            name: span.name(),
            fields: fields.0,
            scope: span
                .scope()
                .from_root()
                .map(|span| snapshot(&span))
                .collect(),
        });
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        let _ = self.0.send(Observation {
            name: event.metadata().name(),
            fields: fields.0,
            scope: ctx
                .event_scope(event)
                .map(|scope| scope.from_root().map(|span| snapshot(&span)).collect())
                .unwrap_or_default(),
        });
    }
}

pub(crate) fn subscriber() -> (impl Subscriber + Send + Sync, mpsc::Receiver<Observation>) {
    let (sender, receiver) = mpsc::channel();
    (
        tracing_subscriber::registry().with(Capture(sender)),
        receiver,
    )
}

pub(crate) fn assert_request(record: &Observation, id: &Id, request_id: &str) -> Result<()> {
    ensure!(
        record.scope.first().is_some_and(|root| {
            root.id == *id
                && root.name == "http.server.request"
                && root.fields.get("request_id").map(String::as_str) == Some(request_id)
                && root.fields.get("http.route").map(String::as_str) == Some("/metrics")
        }),
        "{} lost request {request_id}: {record:?}",
        record.name
    );
    Ok(())
}
