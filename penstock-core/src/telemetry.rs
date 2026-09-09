//! Internal, feature-gated metrics helpers.

#[cfg(feature = "metrics")]
mod implementation {
    use std::time::Instant;

    use crate::PipelineId;

    pub(crate) struct RunGuard {
        topology: &'static str,
        pipeline_id: PipelineId,
    }

    impl RunGuard {
        pub(crate) fn new(topology: &'static str, pipeline_id: &PipelineId) -> Self {
            metrics::gauge!("penstock_pipeline_active", "topology" => topology, "pipeline_id" => pipeline_id.as_str().to_owned()).increment(1.0);
            Self {
                topology,
                pipeline_id: pipeline_id.clone(),
            }
        }
    }

    impl Drop for RunGuard {
        fn drop(&mut self) {
            metrics::gauge!("penstock_pipeline_active", "topology" => self.topology, "pipeline_id" => self.pipeline_id.as_str().to_owned()).decrement(1.0);
        }
    }

    pub(crate) struct StageTimer {
        started: Instant,
        topology: &'static str,
        stage: &'static str,
        pipeline_id: PipelineId,
    }

    impl StageTimer {
        pub(crate) fn new(
            topology: &'static str,
            stage: &'static str,
            pipeline_id: &PipelineId,
        ) -> Self {
            Self {
                started: Instant::now(),
                topology,
                stage,
                pipeline_id: pipeline_id.clone(),
            }
        }
    }

    impl Drop for StageTimer {
        fn drop(&mut self) {
            metrics::histogram!(
                "penstock_stage_duration_seconds",
                "topology" => self.topology,
                "stage" => self.stage,
                "pipeline_id" => self.pipeline_id.as_str().to_owned()
            )
            .record(self.started.elapsed().as_secs_f64());
        }
    }

    pub(crate) fn record(topology: &'static str, pipeline_id: &PipelineId) {
        metrics::counter!("penstock_records_total", "topology" => topology, "pipeline_id" => pipeline_id.as_str().to_owned()).increment(1);
    }

    pub(crate) fn batch(topology: &'static str, pipeline_id: &PipelineId, records: usize) {
        metrics::counter!("penstock_batches_total", "topology" => topology, "pipeline_id" => pipeline_id.as_str().to_owned()).increment(1);
        metrics::histogram!("penstock_batch_records", "topology" => topology, "pipeline_id" => pipeline_id.as_str().to_owned())
            .record(records as f64);
    }

    pub(crate) fn sink_delivery(topology: &'static str, pipeline_id: &PipelineId, success: bool) {
        let status = if success { "success" } else { "error" };
        metrics::counter!(
            "penstock_sink_deliveries_total",
            "topology" => topology,
            "pipeline_id" => pipeline_id.as_str().to_owned(),
            "status" => status
        )
        .increment(1);
    }

    pub(crate) fn commit(topology: &'static str, pipeline_id: &PipelineId, success: bool) {
        let status = if success { "success" } else { "error" };
        metrics::counter!(
            "penstock_commits_total",
            "topology" => topology,
            "pipeline_id" => pipeline_id.as_str().to_owned(),
            "status" => status
        )
        .increment(1);
    }

    pub(crate) fn error(topology: &'static str, pipeline_id: &PipelineId, stage: &'static str) {
        metrics::counter!(
            "penstock_errors_total",
            "topology" => topology,
            "pipeline_id" => pipeline_id.as_str().to_owned(),
            "stage" => stage
        )
        .increment(1);
    }
}

#[cfg(not(feature = "metrics"))]
mod implementation {
    use crate::PipelineId;

    pub(crate) struct RunGuard;

    impl RunGuard {
        pub(crate) fn new(_topology: &'static str, _pipeline_id: &PipelineId) -> Self {
            Self
        }
    }

    pub(crate) struct StageTimer;

    impl StageTimer {
        pub(crate) fn new(
            _topology: &'static str,
            _stage: &'static str,
            _pipeline_id: &PipelineId,
        ) -> Self {
            Self
        }
    }

    pub(crate) fn record(_topology: &'static str, _pipeline_id: &PipelineId) {}
    pub(crate) fn batch(_topology: &'static str, _pipeline_id: &PipelineId, _records: usize) {}
    pub(crate) fn sink_delivery(
        _topology: &'static str,
        _pipeline_id: &PipelineId,
        _success: bool,
    ) {
    }
    pub(crate) fn commit(_topology: &'static str, _pipeline_id: &PipelineId, _success: bool) {}
    pub(crate) fn error(_topology: &'static str, _pipeline_id: &PipelineId, _stage: &'static str) {}
}

pub(crate) use implementation::*;
