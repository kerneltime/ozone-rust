//! Centralized tracing and metrics initialization shared by both Ozone Rust
//! binaries (the S3 gateway and the datanode).
//!
//! This crate is deliberately thin: it owns the two pieces of telemetry setup
//! that *must* be identical across binaries — the global `tracing` subscriber
//! and a [`prometheus`] registry — and nothing else. It does not start HTTP
//! servers, scrape endpoints, or background tasks; callers wire [`Metrics`]
//! into whatever `/metrics` handler they already have and call
//! [`Metrics::gather`] per request.
//!
//! # Two different lifetimes
//!
//! The two halves of this crate have very different ownership models, and the
//! difference is the main thing to keep straight:
//!
//! * Tracing is **process-global and write-once**. [`init_tracing`] installs a
//!   subscriber into a global slot that the `tracing` runtime owns for the
//!   life of the process. There is exactly one such slot, it cannot be
//!   replaced, and the second attempt to set it fails. Treat [`init_tracing`]
//!   like setting a panic hook: call it once, early, from `main`, before any
//!   thread that might emit spans or events.
//!
//! * Metrics are **owned, not global**. [`Metrics`] wraps a plain
//!   [`prometheus::Registry`] value. You construct it, hand out clones of the
//!   metric handles it registers, and keep the `Metrics` alive yourself
//!   (typically in shared application state). Nothing here touches the
//!   `prometheus` default/global registry, so two `Metrics` instances in the
//!   same process are fully independent — which is exactly what tests below
//!   rely on, and what lets a binary keep separate registries per subsystem if
//!   it ever wants to.
//!
//! # Out of scope
//!
//! OTLP / OpenTelemetry export is intentionally absent. It is planned to land
//! later behind a cargo feature; until then this crate has no dependency on
//! the opentelemetry stack so that the common case stays cheap to build.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use prometheus::{
    Histogram, HistogramOpts, IntCounter, IntCounterVec, Opts, Registry, TextEncoder,
};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Errors produced by this crate.
///
/// The two variants correspond to the two subsystems and have very different
/// recoverability, which is why they are kept distinct rather than collapsed
/// into one string:
///
/// * [`ObsError::Prometheus`] wraps a [`prometheus::Error`] verbatim. The one
///   callers will actually hit is `AlreadyReg`: registering two collectors
///   with the same metric name into the same registry is rejected, and the
///   helper methods on [`Metrics`] surface that as this variant. This is a
///   programming error (a name collision), not a runtime condition to retry.
///
/// * [`ObsError::TracingInit`] carries a human-readable message for the two
///   ways [`init_tracing`] can fail: the global subscriber was already set, or
///   the supplied log-filter directive did not parse. Both are reported as
///   `String` because the underlying error types (`TryInitError`,
///   `ParseError`) are not worth threading through the public API for what is,
///   in practice, terminal startup misconfiguration.
#[derive(Debug, thiserror::Error)]
pub enum ObsError {
    /// A `prometheus` operation failed. In normal use this is a duplicate
    /// metric registration (`prometheus::Error::AlreadyReg`).
    #[error("prometheus error: {0}")]
    Prometheus(#[from] prometheus::Error),

    /// The global tracing subscriber could not be installed: either it was
    /// already set by an earlier call, or the log-filter directive failed to
    /// parse. The contained message says which.
    #[error("tracing init failed: {0}")]
    TracingInit(String),
}

/// Configuration for [`init_tracing`].
///
/// Both fields default to "off / unset", which yields the conventional
/// behavior: human-readable output, verbosity taken from `RUST_LOG` (or `info`
/// when that is also unset). A binary typically derives these from its CLI
/// flags so operators can flip JSON on for log aggregation and override the
/// filter without touching the environment.
#[derive(Debug, Clone, Default)]
pub struct TracingOptions {
    /// Emit machine-parseable JSON lines instead of the default
    /// human-oriented format. Leave `false` for interactive/dev use.
    pub json: bool,

    /// Explicit log-filter directive (same grammar as `RUST_LOG`, e.g.
    /// `"info,ozone_s3_gw=debug"`).
    ///
    /// When `Some`, this value is authoritative and the `RUST_LOG` environment
    /// variable is ignored entirely. When `None`, the filter is read from
    /// `RUST_LOG`, falling back to `info` if that is unset. This precedence
    /// (explicit option beats environment) lets a `--log-filter` CLI flag do
    /// what an operator expects even in a shell that exports `RUST_LOG`.
    pub filter: Option<String>,
}

/// Install the process-global `tracing` subscriber. Call **exactly once**, as
/// early as possible in `main`.
///
/// # What it installs
///
/// A `tracing-subscriber` registry with a single formatting layer driven by an
/// [`EnvFilter`]. The filter comes from `opts.filter` when set, otherwise from
/// the `RUST_LOG` environment variable, otherwise the literal default `info`
/// (see [`TracingOptions::filter`] for the precedence rationale). The layer
/// formats as JSON when `opts.json` is true and in the default human format
/// otherwise.
///
/// # Invariant: once per process
///
/// `tracing` keeps a single global default subscriber for the whole process;
/// it can be set but never replaced. This function uses `try_init`, so a
/// second call — or any prior installation of a global subscriber by some
/// other code — returns [`ObsError::TracingInit`] rather than panicking. That
/// makes the failure recoverable in principle, but the only correct response
/// is usually to fix the startup sequence: spans and events emitted before the
/// subscriber is set are dropped, and there is no way to retroactively capture
/// them.
///
/// Because the effect is global, tests must not call this repeatedly; doing so
/// would couple otherwise-independent tests through the shared global slot and
/// make their outcomes depend on execution order.
///
/// # Errors
///
/// Returns [`ObsError::TracingInit`] if the filter directive in `opts.filter`
/// fails to parse, or if a global subscriber is already installed.
pub fn init_tracing(opts: &TracingOptions) -> Result<(), ObsError> {
    // Build the filter first so a bad directive fails before we touch the
    // global slot. An explicit `opts.filter` is parsed strictly (a typo is an
    // error the operator should see); the env/default path uses the lenient
    // `RUST_LOG`-style resolution and only falls back to `info`.
    let filter = match &opts.filter {
        Some(directive) => EnvFilter::try_new(directive)
            .map_err(|e| ObsError::TracingInit(format!("invalid log filter {directive:?}: {e}")))?,
        None => EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    };

    // One fmt layer, two shapes. The branches differ only in the formatter, so
    // they are installed separately rather than boxed: keeping the layer type
    // concrete avoids dynamic dispatch on the hot logging path.
    let registry = tracing_subscriber::registry().with(filter);
    let result = if opts.json {
        registry
            .with(tracing_subscriber::fmt::layer().json())
            .try_init()
    } else {
        registry.with(tracing_subscriber::fmt::layer()).try_init()
    };

    result.map_err(|e| ObsError::TracingInit(format!("global subscriber already set: {e}")))
}

/// An owned Prometheus registry plus convenience constructors for the metric
/// types this project uses.
///
/// `Metrics` is a thin wrapper around [`prometheus::Registry`]. Its only job is
/// to make the "create the metric, register it, hand back the handle" sequence
/// a single fallible call, and to expose [`gather`](Metrics::gather) for the
/// `/metrics` endpoint.
///
/// # Handles are cheap clones; registration is the commitment
///
/// Every Prometheus metric type is internally reference-counted, so the handle
/// returned by the constructors is a cheap clone that shares state with the
/// copy held by the registry. Store the returned handle wherever the metric is
/// updated; observations made through it are reflected in
/// [`gather`](Metrics::gather) output. The registry holds its own clone, so the
/// metric keeps reporting even if the caller drops theirs — though in practice
/// you keep the handle precisely because you intend to update it.
///
/// # Names are unique per registry
///
/// Prometheus rejects two collectors registered under the same metric name in
/// one registry. The constructors here surface that as
/// [`ObsError::Prometheus`]; there is no silent de-duplication. Choose names
/// once and register each exactly once.
pub struct Metrics {
    registry: Registry,
}

impl Metrics {
    /// Create an empty registry with no metrics and no global side effects.
    ///
    /// This uses a fresh [`prometheus::Registry`], **not** the process-wide
    /// default registry, so multiple instances coexist without interfering.
    pub fn new() -> Self {
        Self {
            registry: Registry::new(),
        }
    }

    /// Borrow the underlying registry, e.g. to register metric types this
    /// crate does not provide a helper for, or to pass to other `prometheus`
    /// machinery. Metrics registered directly on the returned registry appear
    /// in [`gather`](Metrics::gather) just like those created via the helpers.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Construct, register, and return a monotonic [`IntCounter`].
    ///
    /// Use for values that only ever increase within a process lifetime
    /// (requests served, bytes written). `help` becomes the `# HELP` line in
    /// the exposition output and should describe the unit/meaning.
    ///
    /// # Errors
    ///
    /// [`ObsError::Prometheus`] if `name` is already registered in this
    /// registry, or is not a valid metric name.
    pub fn int_counter(&self, name: &str, help: &str) -> Result<IntCounter, ObsError> {
        let counter = IntCounter::with_opts(Opts::new(name, help))?;
        self.registry.register(Box::new(counter.clone()))?;
        Ok(counter)
    }

    /// Construct, register, and return an [`IntCounterVec`]: a family of
    /// monotonic counters keyed by the given label names.
    ///
    /// `labels` are the label *keys* (e.g. `&["method", "status"]`); concrete
    /// label values are supplied later via `with_label_values`. The set of
    /// keys is fixed at construction and must match at every call site.
    ///
    /// # Errors
    ///
    /// [`ObsError::Prometheus`] if `name` is already registered, or if `name`
    /// or any label is not a valid Prometheus identifier.
    pub fn int_counter_vec(
        &self,
        name: &str,
        help: &str,
        labels: &[&str],
    ) -> Result<IntCounterVec, ObsError> {
        let counter = IntCounterVec::new(Opts::new(name, help), labels)?;
        self.registry.register(Box::new(counter.clone()))?;
        Ok(counter)
    }

    /// Construct, register, and return a [`Histogram`] with explicit bucket
    /// upper bounds.
    ///
    /// `buckets` are the cumulative `le` (less-than-or-equal) upper bounds, in
    /// ascending order, in the metric's own unit (e.g. seconds for a latency
    /// histogram). Prometheus adds the implicit `+Inf` bucket; callers should
    /// not include it. Passing an empty `buckets` makes `prometheus` fall back
    /// to its default bucket set.
    ///
    /// # Errors
    ///
    /// [`ObsError::Prometheus`] if `name` is already registered, or the
    /// options are otherwise invalid.
    pub fn histogram(
        &self,
        name: &str,
        help: &str,
        buckets: Vec<f64>,
    ) -> Result<Histogram, ObsError> {
        let histogram = Histogram::with_opts(HistogramOpts::new(name, help).buckets(buckets))?;
        self.registry.register(Box::new(histogram.clone()))?;
        Ok(histogram)
    }

    /// Encode every registered metric into the Prometheus text exposition
    /// format, ready to return from a `/metrics` HTTP handler.
    ///
    /// This is a point-in-time snapshot: it walks the registry, reads each
    /// metric's current value, and renders the `# HELP` / `# TYPE` / sample
    /// lines. Call it per scrape request; the cost scales with the number of
    /// series, so it is cheap for the modest metric counts here.
    ///
    /// # Errors
    ///
    /// [`ObsError::Prometheus`] if encoding fails (e.g. a metric in an
    /// inconsistent state). In normal operation this does not happen.
    pub fn gather(&self) -> Result<String, ObsError> {
        let metric_families = self.registry.gather();
        let encoded = TextEncoder::new().encode_to_string(&metric_families)?;
        Ok(encoded)
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_counter_registers_increments_and_appears_in_output() {
        let metrics = Metrics::new();
        let counter = metrics
            .int_counter("requests_total", "Total requests served")
            .expect("counter registers");
        counter.inc();
        counter.inc();

        let output = metrics.gather().expect("gather succeeds");
        // Both the series name and its current value must be present.
        assert!(output.contains("requests_total"), "missing name: {output}");
        assert!(output.contains("requests_total 2"), "missing value: {output}");
        // HELP text round-trips into the exposition format too.
        assert!(
            output.contains("Total requests served"),
            "missing help: {output}"
        );
    }

    #[test]
    fn int_counter_vec_emits_labeled_series() {
        let metrics = Metrics::new();
        let counter = metrics
            .int_counter_vec("http_requests_total", "Requests by method", &["method"])
            .expect("counter vec registers");
        counter.with_label_values(&["GET"]).inc();

        let output = metrics.gather().expect("gather succeeds");
        // The concrete label value must show up on the sample line.
        assert!(
            output.contains("http_requests_total{method=\"GET\"} 1"),
            "missing labeled series: {output}"
        );
    }

    #[test]
    fn histogram_emits_bucket_count_and_sum_lines() {
        let metrics = Metrics::new();
        let histogram = metrics
            .histogram(
                "request_latency_seconds",
                "Request latency",
                vec![0.1, 0.5, 1.0],
            )
            .expect("histogram registers");
        histogram.observe(0.3);

        let output = metrics.gather().expect("gather succeeds");
        // A histogram exposes the cumulative buckets plus aggregate _count/_sum.
        assert!(
            output.contains("request_latency_seconds_bucket"),
            "missing _bucket: {output}"
        );
        assert!(
            output.contains("request_latency_seconds_count 1"),
            "missing _count: {output}"
        );
        assert!(
            output.contains("request_latency_seconds_sum"),
            "missing _sum: {output}"
        );
    }

    #[test]
    fn duplicate_registration_is_a_prometheus_error() {
        let metrics = Metrics::new();
        metrics
            .int_counter("dupe_total", "first")
            .expect("first registration succeeds");

        let err = metrics
            .int_counter("dupe_total", "second")
            .expect_err("second registration with same name must fail");

        // The duplicate-name rejection must surface as the Prometheus variant,
        // not as a generic/tracing error.
        assert!(
            matches!(err, ObsError::Prometheus(_)),
            "expected ObsError::Prometheus, got {err:?}"
        );
    }

    #[test]
    fn empty_registry_gathers_to_empty_string() {
        let metrics = Metrics::new();
        let output = metrics.gather().expect("gather on empty registry succeeds");
        assert!(output.is_empty(), "expected no output, got: {output}");
    }
}
