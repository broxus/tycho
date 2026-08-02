use std::fs::{self, File};
use std::future::Future;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::{Arc, Weak};
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tycho_util::sync::CancellationFlag;

use crate::config::TransactionsGcConfig;

use super::codec::{GcIntent, GcIntentPhase};
use super::filter::FilterWorker;
use super::maintenance::{MaintenanceCoordinator, MaintenancePermit, MaintenancePriority};
use super::partition::{PartitionDeletionGuard, PartitionId, PartitionManager};
use super::storage::{GcEvacuationChunkResult, RpcStorage, SnapshotPublisher};
use super::tail::{AuthoritativeErrorKind, TailStore, TailSweepNext, classify_authoritative_error, conflicting_authoritative_error, missing_authoritative_error};
use tycho_types::models::BlockId;

const REFERENCE_WAIT_INITIAL_DELAY: Duration = Duration::from_millis(10);
const REFERENCE_WAIT_MAX_DELAY: Duration = Duration::from_millis(250);
const DELETE_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(10);
const DELETE_RETRY_MAX_DELAY: Duration = Duration::from_secs(1);
const GC_INTENT_PHASES: [(GcIntentPhase, &str); 4] = [
    (GcIntentPhase::Evacuating, "evacuating"),
    (GcIntentPhase::Prepared, "prepared"),
    (GcIntentPhase::CutoverCommitted, "cutover_committed"),
    (GcIntentPhase::Deleting, "deleting"),
];

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GcDeletionFailureStage {
    BeforeDeletingControl,
    AfterDeletingControl,
    BeforeDirectoryRemoval,
    AfterDirectoryRemoval,
    AfterControlFinalization,
}

pub(super) struct GcStateMachine {
    partitions: Arc<Mutex<PartitionManager>>,
    filter_worker: Arc<FilterWorker>,
    tail: Arc<TailStore>,
    maintenance: Arc<MaintenanceCoordinator>,
    notify: Arc<Notify>,
    tail_sweep_records_per_batch: usize,
    snapshots: Arc<SnapshotPublisher>,
    cancelled: CancellationFlag,
    task: Mutex<Option<JoinHandle<()>>>,
    scheduler: Mutex<GcPassScheduler>,
    startup_configured: AtomicBool,
    startup_resume_checked: AtomicBool,
    observed_phase: Arc<Mutex<Option<ObservedGcPhase>>>,
    #[cfg(test)]
    failure_stage: Mutex<Option<GcDeletionFailureStage>>,
    #[cfg(test)]
    remove_failures: AtomicU64,
    #[cfg(test)]
    remove_attempts: AtomicU64,
    #[cfg(test)]
    guard_acquisition_barrier: Mutex<Option<Arc<tokio::sync::Barrier>>>,
    #[cfg(test)]
    pause_after_guard_precheck: AtomicBool,
    #[cfg(test)]
    guard_precheck_entered: Notify,
    #[cfg(test)]
    guard_precheck_release: Notify,
    #[cfg(test)]
    startup_passes_completed: AtomicU64,
    #[cfg(test)]
    startup_pass_completed: Notify,
}

#[derive(Default)]
struct GcPassScheduler {
    context: Option<GcSchedulerContext>,
    active: Option<GcEligibilityPass>,
    last_started_at: Option<Instant>,
    closed: bool,
}

#[derive(Clone)]
struct GcSchedulerContext {
    storage: Weak<RpcStorage>,
    config: Option<TransactionsGcConfig>,
}

#[derive(Clone, Copy)]
struct GcEligibilityPass {
    frontier: BlockId,
    pending_sealing: Option<PartitionId>,
    started_at: Instant,
    trigger: GcPassTrigger,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GcPassTrigger {
    Startup,
    Frontier,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GcFrontierAdmission {
    Accepted,
    Disabled,
    Busy,
    RateLimited,
    Cancelled,
    ResyncRequired,
}

#[derive(Clone, Copy)]
struct ObservedGcPhase {
    operation_id: u128,
    phase: GcIntentPhase,
    started_at: Instant,
}

#[derive(Clone, Copy)]
enum GcPhaseResult {
    Transitioned,
    Completed,
}

enum GcPassDrive {
    Inactive,
    Worked,
    AwaitSealing,
    CandidateExhausted,
}

enum GcPassStep {
    Worked,
    CandidateExhausted,
    Prepared(GcIntent),
    Delete(GcIntent),
}

fn gc_pass_step_after_evacuation(
    intent: GcIntent,
    result: GcEvacuationChunkResult,
) -> GcPassStep {
    match result {
        GcEvacuationChunkResult::Appended => GcPassStep::Worked,
        GcEvacuationChunkResult::Prepared(prepared) => {
            debug_assert_eq!(prepared.operation_id, intent.operation_id);
            GcPassStep::Prepared(prepared)
        }
    }
}

impl GcPassScheduler {
    fn configure_startup(
        &mut self,
        storage: Weak<RpcStorage>,
        config: Option<TransactionsGcConfig>,
        frontier: BlockId,
        pending_sealing: Option<PartitionId>,
        now: Instant,
    ) -> Result<()> {
        ensure!(self.context.is_none(), "RPC transaction GC scheduler was configured more than once");
        self.context = Some(GcSchedulerContext { storage, config });
        self.active = Some(GcEligibilityPass {
            frontier,
            pending_sealing,
            started_at: now,
            trigger: GcPassTrigger::Startup,
        });
        self.last_started_at = Some(now);
        Ok(())
    }

    fn admit_frontier(
        &mut self,
        frontier: BlockId,
        pending_sealing: Option<PartitionId>,
        now: Instant,
        cancelled: bool,
        resync_required: bool,
    ) -> GcFrontierAdmission {
        let Some(context) = self.context.as_ref() else {
            return GcFrontierAdmission::Disabled;
        };
        let Some(config) = context.config.as_ref() else {
            return GcFrontierAdmission::Disabled;
        };
        if self.closed || cancelled {
            return GcFrontierAdmission::Cancelled;
        }
        if resync_required {
            return GcFrontierAdmission::ResyncRequired;
        }
        if self.active.is_some() {
            return GcFrontierAdmission::Busy;
        }
        if self
            .last_started_at
            .is_some_and(|started_at| now.saturating_duration_since(started_at) < config.min_interval)
        {
            return GcFrontierAdmission::RateLimited;
        }
        self.active = Some(GcEligibilityPass {
            frontier,
            pending_sealing,
            started_at: now,
            trigger: GcPassTrigger::Frontier,
        });
        self.last_started_at = Some(now);
        GcFrontierAdmission::Accepted
    }

    fn active(&self) -> Option<(GcSchedulerContext, GcEligibilityPass)> {
        Some((self.context.as_ref()?.clone(), self.active?))
    }

    fn complete_active(&mut self) -> Option<GcEligibilityPass> {
        self.active.take()
    }
}

impl GcStateMachine {
    pub(super) fn new(
        partitions: Arc<Mutex<PartitionManager>>,
        filter_worker: Arc<FilterWorker>,
        tail: Arc<TailStore>,
        maintenance: Arc<MaintenanceCoordinator>,
        notify: Arc<Notify>,
        tail_sweep_records_per_batch: usize,
        snapshots: Arc<SnapshotPublisher>,
    ) -> Self {
        assert!(tail_sweep_records_per_batch > 0);
        Self {
            partitions,
            filter_worker,
            tail,
            maintenance,
            notify,
            tail_sweep_records_per_batch,
            snapshots,
            cancelled: CancellationFlag::new(),
            task: Default::default(),
            scheduler: Default::default(),
            startup_configured: AtomicBool::new(false),
            startup_resume_checked: AtomicBool::new(false),
            observed_phase: Default::default(),
            #[cfg(test)]
            failure_stage: Default::default(),
            #[cfg(test)]
            remove_failures: AtomicU64::new(0),
            #[cfg(test)]
            remove_attempts: AtomicU64::new(0),
            #[cfg(test)]
            guard_acquisition_barrier: Default::default(),
            #[cfg(test)]
            pause_after_guard_precheck: AtomicBool::new(false),
            #[cfg(test)]
            guard_precheck_entered: Notify::new(),
            #[cfg(test)]
            guard_precheck_release: Notify::new(),
            #[cfg(test)]
            startup_passes_completed: AtomicU64::new(0),
            #[cfg(test)]
            startup_pass_completed: Notify::new(),
        }
    }

    pub(super) fn configure_startup(
        &self,
        storage: Weak<RpcStorage>,
        config: Option<TransactionsGcConfig>,
        frontier: BlockId,
        pending_sealing: Option<PartitionId>,
    ) -> Result<()> {
        ensure!(self.task.lock().is_none(), "RPC transaction GC startup pass was configured after worker start");
        ensure!(self
            .startup_configured
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok(), "RPC transaction GC startup pass was configured more than once");
        self.scheduler.lock().configure_startup(
            storage,
            config,
            frontier,
            pending_sealing,
            Instant::now(),
        )?;
        record_gc_startup_pass_started();
        set_current_gc_phase_metrics(None);
        Ok(())
    }

    pub(super) fn notify_frontier(
        &self,
        frontier: BlockId,
        pending_sealing: Option<PartitionId>,
    ) {
        self.notify_frontier_at(frontier, pending_sealing, Instant::now());
    }

    fn notify_frontier_at(
        &self,
        frontier: BlockId,
        pending_sealing: Option<PartitionId>,
        now: Instant,
    ) -> GcFrontierAdmission {
        let admission = self.scheduler.lock().admit_frontier(
            frontier,
            pending_sealing,
            now,
            self.cancelled.check(),
            self.snapshots.is_resync_required(),
        );
        if admission == GcFrontierAdmission::Accepted {
            self.notify.notify_one();
        }
        admission
    }

    pub(super) fn start(self: &Arc<Self>) {
        let mut task = self.task.lock();
        if task.is_some() {
            return;
        }
        let this = self.clone();
        *task = Some(tokio::spawn(async move { this.run().await }));
        self.notify.notify_one();
    }

    pub(super) fn shutdown(&self) {
        self.cancelled.cancel();
        self.scheduler.lock().closed = true;
        self.clear_gc_intent_observation();
        self.notify.notify_waiters();
        if let Some(task) = self.task.lock().take() {
            task.abort();
        }
    }

    async fn run(&self) {
        let mut retry_delay = DELETE_RETRY_INITIAL_DELAY;
        self.notify.notified().await;
        while !self.cancelled.check() && !self.snapshots.is_resync_required() {
            let control_notified = self.notify.notified();
            let generation_released = self.tail.generation_release_notify().notified();
            tokio::pin!(control_notified);
            tokio::pin!(generation_released);
            match self.run_once().await {
                Ok(TailSweepNext::Ready) => {
                    retry_delay = DELETE_RETRY_INITIAL_DELAY;
                    tokio::task::yield_now().await;
                }
                Ok(TailSweepNext::Empty | TailSweepNext::AwaitPublishedGeneration { .. }) => {
                    retry_delay = DELETE_RETRY_INITIAL_DELAY;
                    control_notified.await;
                }
                Ok(TailSweepNext::AwaitRequestGeneration { .. }) => {
                    retry_delay = DELETE_RETRY_INITIAL_DELAY;
                    tokio::select! {
                        _ = &mut control_notified => {}
                        _ = &mut generation_released => {}
                    }
                }
                Err(error) => {
                    if self.cancelled.check() {
                        break;
                    }
                    if let Some(kind) = classify_authoritative_error(&error) {
                        self.mark_resync_required(kind, &error);
                        break;
                    }
                    tracing::error!(
                        retry_delay_ms = retry_delay.as_millis(),
                        "failed to run RPC transaction GC maintenance: {error:#}",
                    );
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = retry_delay.saturating_mul(2).min(DELETE_RETRY_MAX_DELAY);
                }
            }
        }
        self.clear_gc_intent_observation();
    }

    async fn run_once(&self) -> Result<TailSweepNext> {
        self.observe_startup_resume_once()?;
        self.cleanup_certified_removed_directories().await?;
        if let Some(intent) = self.current_deletion_intent()? {
            self.delete_source(intent).await?;
        }
        let (published_visible_generation, protected_progress_generation) = {
            let partitions = self.partitions.lock();
            let intent = partitions.gc_intent()?;
            (
                partitions.tail_visible_generation(),
                intent.map(|intent| intent.target_generation),
            )
        };
        if published_visible_generation > 0
            && protected_progress_generation != Some(published_visible_generation)
        {
            self.delete_obsolete_progress(
                published_visible_generation,
                published_visible_generation,
            )
            .await?;
        }
        let pass_drive = self.drive_gc_pass_once().await?;
        if matches!(pass_drive, GcPassDrive::Worked) {
            return Ok(TailSweepNext::Ready);
        }
        if matches!(pass_drive, GcPassDrive::AwaitSealing) {
            return Ok(TailSweepNext::Empty);
        }
        let next = self.sweep_tail_once(published_visible_generation).await?;
        if matches!(pass_drive, GcPassDrive::CandidateExhausted) {
            match next {
                TailSweepNext::Ready => {}
                TailSweepNext::Empty | TailSweepNext::AwaitRequestGeneration { .. } => {
                    self.complete_gc_pass();
                }
                TailSweepNext::AwaitPublishedGeneration { .. } => {
                    return Err(conflicting_authoritative_error(
                        "RPC tail retirement queue is ahead of published control without an active GC intent",
                    ));
                }
            }
        }
        Ok(next)
    }

    async fn drive_gc_pass_once(&self) -> Result<GcPassDrive> {
        let Some((context, pass)) = self.scheduler.lock().active() else {
            return Ok(GcPassDrive::Inactive);
        };
        let Some(permit) = acquire_background_permit_after_transient_check(
            || gc_pass_sealing_pending(&context, &pass),
            self.acquire_background_permit(),
        ).await? else {
            return Ok(GcPassDrive::AwaitSealing);
        };
        let storage = context
            .storage
            .upgrade()
            .context("RPC transaction GC pass storage was dropped")?;
        let config = context.config.clone();
        let frontier = pass.frontier;
        let observed_phase = self.observed_phase.clone();
        let cancelled = self.cancelled.clone();
        let step = tokio::task::spawn_blocking(move || -> Result<GcPassStep> {
            let _permit = permit;
            let Some(intent) = storage.begin_or_resume_gc_at_frontier(
                config.as_ref(),
                &frontier,
            )? else {
                return Ok(GcPassStep::CandidateExhausted);
            };
            observe_gc_intent_state(&observed_phase, &cancelled, intent);
            match intent.phase {
                GcIntentPhase::Evacuating => Ok(gc_pass_step_after_evacuation(
                    intent,
                    storage.evacuate_gc_chunk(intent)?,
                )),
                GcIntentPhase::Prepared => Ok(GcPassStep::Prepared(intent)),
                GcIntentPhase::CutoverCommitted | GcIntentPhase::Deleting => {
                    Ok(GcPassStep::Delete(intent))
                }
            }
        })
        .await
        .context("RPC transaction GC pass step failed")??;
        match step {
            GcPassStep::Worked => Ok(GcPassDrive::Worked),
            GcPassStep::CandidateExhausted => Ok(GcPassDrive::CandidateExhausted),
            GcPassStep::Prepared(intent) => {
                self.observe_gc_intent(intent);
                let permit = self.acquire_background_permit().await?;
                let storage = context
                    .storage
                    .upgrade()
                    .context("RPC transaction GC pass storage was dropped")?;
                let committed = storage.cutover_prepared_gc(intent).await?;
                self.observe_gc_intent(committed);
                drop(storage);
                drop(permit);
                Ok(GcPassDrive::Worked)
            }
            GcPassStep::Delete(intent) => {
                self.observe_gc_intent(intent);
                self.delete_source(intent).await?;
                Ok(GcPassDrive::Worked)
            }
        }
    }

    fn complete_gc_pass(&self) {
        let Some(pass) = self.scheduler.lock().complete_active() else {
            return;
        };
        let elapsed = pass.started_at.elapsed();
        record_gc_pass_completed(pass.frontier.seqno, elapsed);
        if pass.trigger == GcPassTrigger::Startup {
            record_gc_startup_pass_completed(elapsed);
        }
        #[cfg(test)]
        {
            self.startup_passes_completed.fetch_add(1, Ordering::AcqRel);
            self.startup_pass_completed.notify_one();
        }
    }

    async fn delete_obsolete_progress(
        &self,
        published_visible_generation: u64,
        target_generation: u64,
    ) -> Result<()> {
        let permit = self.acquire_background_permit().await?;
        let tail = self.tail.clone();
        let deleted = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            tail.delete_obsolete_progress(published_visible_generation, target_generation)
        })
        .await
        .context("RPC tail progress cleanup task failed")??;
        if deleted {
            record_tail_progress_cleanup();
        }
        Ok(())
    }

    async fn sweep_tail_once(&self, published_visible_generation: u64) -> Result<TailSweepNext> {
        let permit = self.acquire_background_permit().await?;
        let tail = self.tail.clone();
        let max_records = self.tail_sweep_records_per_batch;
        let result = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            tail.sweep_retired(published_visible_generation, max_records)
        })
        .await
        .context("RPC tail sweep task failed")??;
        if result.swept_records > 0 {
            tracing::debug!(
                swept_records = result.swept_records,
                oldest_swept_generation = result.oldest_swept_generation,
                newest_swept_generation = result.newest_swept_generation,
                "swept retired RPC tail transactions",
            );
        }
        Ok(result.next)
    }

    fn mark_resync_required(
        &self,
        kind: AuthoritativeErrorKind,
        error: &anyhow::Error,
    ) {
        self.snapshots.transition_to_resync_required(kind, error);
    }

    fn observe_gc_intent(&self, intent: GcIntent) {
        observe_gc_intent_state(&self.observed_phase, &self.cancelled, intent);
    }

    fn observe_startup_resume_once(&self) -> Result<()> {
        if !self.startup_configured.load(Ordering::Acquire)
            || self.startup_resume_checked.load(Ordering::Acquire)
        {
            return Ok(());
        }
        let resumed_intent = self.partitions.lock().gc_intent()?;
        if self
            .startup_resume_checked
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        if let Some(intent) = resumed_intent {
            self.observe_gc_intent(intent);
            record_gc_restart_resume(intent.phase);
        }
        Ok(())
    }

    fn complete_gc_intent_observation(&self, operation_id: u128) {
        let mut observed = self.observed_phase.lock();
        let completed = match *observed {
            Some(current) if current.operation_id == operation_id => observed.take(),
            _ => None,
        };
        if let Some(completed) = completed {
            record_gc_phase_duration(completed, GcPhaseResult::Completed);
            set_current_gc_phase_metrics(None);
        }
    }

    fn clear_gc_intent_observation(&self) {
        let mut observed = self.observed_phase.lock();
        observed.take();
        set_current_gc_phase_metrics(None);
    }

    #[cfg(test)]
    pub(super) async fn run_once_for_test(&self) -> Result<TailSweepNext> {
        self.run_once().await
    }

    #[cfg(test)]
    pub(super) async fn wait_for_resync_required(&self) {
        self.snapshots.wait_for_resync_required().await;
    }

    #[cfg(test)]
    pub(super) fn resync_transitions(&self) -> u64 {
        self.snapshots.resync_transitions()
    }

    #[cfg(test)]
    pub(super) async fn wait_for_startup_passes_completed(&self, expected: u64) {
        self.wait_for_gc_passes_completed(expected).await;
    }

    #[cfg(test)]
    pub(super) async fn wait_for_gc_passes_completed(&self, expected: u64) {
        loop {
            let completed = self.startup_pass_completed.notified();
            if self.startup_passes_completed.load(Ordering::Acquire) >= expected {
                return;
            }
            completed.await;
        }
    }

    #[cfg(test)]
    pub(super) fn startup_passes_completed(&self) -> u64 {
        self.gc_passes_completed()
    }

    #[cfg(test)]
    pub(super) fn gc_passes_completed(&self) -> u64 {
        self.startup_passes_completed.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn active_pass_frontier(&self) -> Option<BlockId> {
        self.scheduler.lock().active.as_ref().map(|pass| pass.frontier)
    }

    #[cfg(test)]
    pub(super) fn complete_active_pass(&self) {
        self.complete_gc_pass();
    }

    fn current_deletion_intent(&self) -> Result<Option<GcIntent>> {
        let intent = self
            .partitions
            .lock()
            .gc_intent()?
            .filter(|intent| matches!(intent.phase, GcIntentPhase::CutoverCommitted | GcIntentPhase::Deleting));
        if let Some(intent) = intent {
            self.observe_gc_intent(intent);
        }
        Ok(intent)
    }

    pub(super) async fn delete_source(&self, expected: GcIntent) -> Result<PartitionId> {
        let source_id = self.delete_source_inner(expected).await?;
        self.complete_gc_intent_observation(expected.operation_id);
        #[cfg(test)]
        self.fail_at(GcDeletionFailureStage::AfterControlFinalization)?;
        let published_visible_generation = {
            let partitions = self.partitions.lock();
            if partitions.removed_through_partition_id() < source_id.0 {
                return Err(conflicting_authoritative_error(
                    "RPC transaction GC progress cleanup requires finalized source deletion",
                ));
            }
            if partitions.tail_visible_generation() < expected.target_generation {
                return Err(conflicting_authoritative_error(
                    "RPC transaction GC progress cleanup requires the target generation to be published",
                ));
            }
            partitions.tail_visible_generation()
        };
        self.delete_obsolete_progress(
            published_visible_generation,
            expected.target_generation,
        )
        .await?;
        Ok(source_id)
    }

    async fn delete_source_inner(&self, expected: GcIntent) -> Result<PartitionId> {
        ensure!(matches!(expected.phase, GcIntentPhase::CutoverCommitted | GcIntentPhase::Deleting),
            "RPC transaction GC source deletion requires a committed or deleting intent");
        let committed = GcIntent {
            phase: GcIntentPhase::CutoverCommitted,
            ..expected
        };
        let deleting = GcIntent {
            phase: GcIntentPhase::Deleting,
            ..expected
        };
        let source_id = PartitionId(expected.source_partition_id);
        if self.deletion_is_finalized(source_id)? {
            return Ok(source_id);
        }
        self.ensure_current_intent(committed, deleting)?;

        // filter retirement acquires its own background permit, so no deletion permit is held here
        self.filter_worker.retire_partition(source_id).await?;
        self.wait_for_references(source_id).await?;
        #[cfg(test)]
        self.fail_at(GcDeletionFailureStage::BeforeDeletingControl)?;

        let deleting = {
            let mut partitions = self.partitions.lock();
            if partitions.removed_through_partition_id() >= source_id.0 {
                ensure!(!partitions.descriptors().iter().any(|descriptor| descriptor.id == source_id),
                    "removed RPC transaction partition still has a manifest");
                return Ok(source_id);
            }
            partitions.transition_gc_intent_to_deleting(&committed)?
        };
        self.observe_gc_intent(deleting);
        #[cfg(test)]
        self.fail_at(GcDeletionFailureStage::AfterDeletingControl)?;

        #[cfg(test)]
        let guard_acquisition_barrier = self.guard_acquisition_barrier.lock().clone();
        #[cfg(test)]
        if let Some(barrier) = guard_acquisition_barrier {
            if barrier.wait().await.is_leader() {
                self.guard_acquisition_barrier.lock().take();
            }
        }
        let Some(guard) = self.acquire_deletion_guard(source_id).await? else {
            return Ok(source_id);
        };
        validate_deletion_path(&guard)?;
        #[cfg(test)]
        self.fail_at(GcDeletionFailureStage::BeforeDirectoryRemoval)?;
        let guard = self.remove_source_directory_with_retry(guard).await?;
        #[cfg(test)]
        self.fail_at(GcDeletionFailureStage::AfterDirectoryRemoval)?;

        self.partitions
            .lock()
            .finalize_gc_source_deletion(&deleting, guard)
    }

    fn ensure_current_intent(&self, committed: GcIntent, deleting: GcIntent) -> Result<()> {
        let current = self
            .partitions
            .lock()
            .gc_intent()?
            .ok_or_else(|| missing_authoritative_error(
                "RPC transaction GC source deletion intent is missing",
            ))?;
        if current != committed && current != deleting {
            return Err(conflicting_authoritative_error(
                "RPC transaction GC source deletion intent identity changed",
            ));
        }
        Ok(())
    }

    fn deletion_is_finalized(&self, source_id: PartitionId) -> Result<bool> {
        let partitions = self.partitions.lock();
        if partitions.removed_through_partition_id() < source_id.0 {
            return Ok(false);
        }
        if partitions.descriptors().iter().any(|descriptor| descriptor.id == source_id) {
            return Err(conflicting_authoritative_error(
                "removed RPC transaction partition still has a manifest",
            ));
        }
        Ok(true)
    }

    async fn wait_for_references(&self, source_id: PartitionId) -> Result<()> {
        let started_at = Instant::now();
        let mut wait_delay = REFERENCE_WAIT_INITIAL_DELAY;
        loop {
            ensure!(!self.cancelled.check(), "RPC transaction GC source deletion cancelled");
            if self.deletion_is_finalized(source_id)? {
                return Ok(());
            }
            if self.partitions.lock().deletion_references_drained(source_id)? {
                record_gc_source_reference_wait(started_at.elapsed());
                return Ok(());
            }
            tokio::time::sleep(wait_delay).await;
            wait_delay = wait_delay.saturating_mul(2).min(REFERENCE_WAIT_MAX_DELAY);
        }
    }

    async fn acquire_deletion_guard(&self, source_id: PartitionId) -> Result<Option<PartitionDeletionGuard>> {
        let mut wait_delay = REFERENCE_WAIT_INITIAL_DELAY;
        loop {
            ensure!(!self.cancelled.check(), "RPC transaction GC source deletion cancelled");
            if self.deletion_is_finalized(source_id)? {
                return Ok(None);
            }
            #[cfg(test)]
            if self.pause_after_guard_precheck.swap(false, Ordering::AcqRel) {
                self.guard_precheck_entered.notify_one();
                self.guard_precheck_release.notified().await;
            }
            let guard_result = {
                self.partitions
                    .lock()
                    .try_acquire_deletion_guard(source_id)
            };
            match guard_result {
                Ok(Some(guard)) => return Ok(Some(guard)),
                Ok(None) => {}
                Err(error) => {
                    if self.deletion_is_finalized(source_id)? {
                        return Ok(None);
                    }
                    return Err(error);
                }
            }
            tokio::time::sleep(wait_delay).await;
            wait_delay = wait_delay.saturating_mul(2).min(REFERENCE_WAIT_MAX_DELAY);
        }
    }

    async fn remove_source_directory_with_retry(
        &self,
        mut guard: PartitionDeletionGuard,
    ) -> Result<PartitionDeletionGuard> {
        let started_at = Instant::now();
        let mut retry_delay = DELETE_RETRY_INITIAL_DELAY;
        loop {
            ensure!(!self.cancelled.check(), "RPC transaction GC source deletion cancelled");
            let permit = self.acquire_background_permit().await?;
            #[cfg(test)]
            let injected_failure = self.take_remove_failure();
            #[cfg(not(test))]
            let injected_failure = false;
            #[cfg(test)]
            {
                self.remove_attempts.fetch_add(1, Ordering::AcqRel);
            }
            let path = guard.path().to_path_buf();
            let (returned_guard, result) = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let result = if injected_failure {
                    Err(anyhow::anyhow!("injected RPC transaction partition directory removal failure"))
                } else {
                    remove_source_directory_once(&path)
                };
                (guard, result)
            })
            .await
            .context("RPC transaction partition directory removal task failed")?;
            guard = returned_guard;
            match result {
                Ok(()) => {
                    record_gc_source_deletion(started_at.elapsed());
                    return Ok(guard);
                }
                Err(error) => {
                    record_gc_source_delete_retry();
                    tracing::error!(
                        partition_id = guard.id().0,
                        path = %guard.path().display(),
                        retry_delay_ms = retry_delay.as_millis(),
                        "failed to remove exact RPC transaction partition directory: {error:#}",
                    );
                    tokio::time::sleep(retry_delay).await;
                    retry_delay = retry_delay.saturating_mul(2).min(DELETE_RETRY_MAX_DELAY);
                }
            }
        }
    }

    async fn cleanup_certified_removed_directories(&self) -> Result<()> {
        let directories = self
            .partitions
            .lock()
            .certified_removed_partition_directories()?;
        for (id, path) in directories {
            validate_partition_path(id, &path)?;
            let mut retry_delay = DELETE_RETRY_INITIAL_DELAY;
            loop {
                ensure!(!self.cancelled.check(), "RPC transaction GC orphan cleanup cancelled");
                let permit = self.acquire_background_permit().await?;
                let remove_path = path.clone();
                let result = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    remove_source_directory_once(&remove_path)
                })
                .await
                .context("RPC transaction orphan directory removal task failed")?;
                match result {
                    Ok(()) => {
                        record_gc_certified_orphan_removed();
                        break;
                    }
                    Err(error) => {
                        record_gc_source_delete_retry();
                        tracing::error!(
                            partition_id = id.0,
                            path = %path.display(),
                            retry_delay_ms = retry_delay.as_millis(),
                            "failed to remove certified RPC transaction orphan directory: {error:#}",
                        );
                        tokio::time::sleep(retry_delay).await;
                        retry_delay = retry_delay.saturating_mul(2).min(DELETE_RETRY_MAX_DELAY);
                    }
                }
            }
        }
        Ok(())
    }

    async fn acquire_background_permit(&self) -> Result<MaintenancePermit> {
        let permit = self.maintenance.acquire(MaintenancePriority::Background);
        tokio::pin!(permit);
        loop {
            tokio::select! {
                result = &mut permit => {
                    return result.context("RPC maintenance coordinator closed during source deletion");
                }
                _ = tokio::time::sleep(REFERENCE_WAIT_INITIAL_DELAY) => {
                    ensure!(!self.cancelled.check(), "RPC transaction GC source deletion cancelled");
                }
            }
        }
    }

    #[cfg(test)]
    fn fail_at(&self, stage: GcDeletionFailureStage) -> Result<()> {
        let mut failure = self.failure_stage.lock();
        if *failure == Some(stage) {
            failure.take();
            anyhow::bail!("injected RPC transaction GC source deletion failure at {stage:?}");
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn fail_deletion_at(&self, stage: GcDeletionFailureStage) {
        *self.failure_stage.lock() = Some(stage);
    }

    #[cfg(test)]
    pub(super) fn fail_next_directory_removals(&self, count: u64) {
        self.remove_failures.store(count, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn remove_attempts(&self) -> u64 {
        self.remove_attempts.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(super) fn synchronize_deletion_guard_acquisition(
        &self,
        barrier: Arc<tokio::sync::Barrier>,
    ) {
        *self.guard_acquisition_barrier.lock() = Some(barrier);
    }

    #[cfg(test)]
    pub(super) fn pause_next_deletion_guard_after_precheck(&self) {
        self.pause_after_guard_precheck.store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) async fn wait_for_deletion_guard_precheck(&self) {
        self.guard_precheck_entered.notified().await;
    }

    #[cfg(test)]
    pub(super) fn release_deletion_guard_after_precheck(&self) {
        self.guard_precheck_release.notify_one();
    }

    #[cfg(test)]
    fn take_remove_failure(&self) -> bool {
        self.remove_failures
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| remaining.checked_sub(1))
            .is_ok()
    }
}

fn observe_gc_intent_state(
    observed_phase: &Mutex<Option<ObservedGcPhase>>,
    cancelled: &CancellationFlag,
    intent: GcIntent,
) {
    let now = Instant::now();
    let mut observed = observed_phase.lock();
    if cancelled.check() {
        return;
    }
    let (previous, current, changed) = match *observed {
        Some(current)
            if current.operation_id == intent.operation_id
                && gc_phase_rank(intent.phase) <= gc_phase_rank(current.phase) =>
        {
            (None, current, false)
        }
        previous => {
            let current = ObservedGcPhase {
                operation_id: intent.operation_id,
                phase: intent.phase,
                started_at: now,
            };
            *observed = Some(current);
            (previous, current, true)
        }
    };
    if let Some(previous) = previous {
        record_gc_phase_duration(previous, GcPhaseResult::Transitioned);
    }
    if changed {
        metrics::counter!(
            "tycho_storage_rpc_gc_phase_observations_total",
            "phase" => gc_phase_label(current.phase),
        )
        .increment(1);
    }
    set_current_gc_phase_metrics(Some(current.phase));
}

fn gc_phase_label(phase: GcIntentPhase) -> &'static str {
    match phase {
        GcIntentPhase::Evacuating => "evacuating",
        GcIntentPhase::Prepared => "prepared",
        GcIntentPhase::CutoverCommitted => "cutover_committed",
        GcIntentPhase::Deleting => "deleting",
    }
}

fn record_gc_restart_resume(phase: GcIntentPhase) {
    metrics::counter!(
        "tycho_storage_rpc_gc_restart_resumes_total",
        "phase" => gc_phase_label(phase),
    )
    .increment(1);
}

fn record_gc_startup_pass_started() {
    metrics::counter!("tycho_storage_rpc_gc_startup_pass_started_total").increment(1);
}

fn record_gc_startup_pass_completed(elapsed: Duration) {
    metrics::histogram!("tycho_storage_rpc_gc_startup_pass_duration_seconds").record(elapsed);
    metrics::counter!("tycho_storage_rpc_gc_startup_pass_completed_total").increment(1);
}

fn record_gc_pass_completed(frontier_seqno: u32, elapsed: Duration) {
    metrics::histogram!("tycho_storage_rpc_gc_pass_duration_seconds").record(elapsed);
    metrics::gauge!("tycho_storage_rpc_gc_last_completed_mc_block_seqno").set(frontier_seqno);
}

fn record_gc_source_reference_wait(elapsed: Duration) {
    metrics::histogram!("tycho_storage_rpc_gc_source_reference_wait_time").record(elapsed);
}

fn record_gc_source_deletion(elapsed: Duration) {
    metrics::histogram!("tycho_storage_rpc_gc_source_deletion_time").record(elapsed);
}

fn record_gc_source_delete_retry() {
    metrics::counter!("tycho_storage_rpc_gc_source_delete_retries_total").increment(1);
}

fn record_gc_certified_orphan_removed() {
    metrics::counter!("tycho_storage_rpc_gc_certified_orphans_removed_total").increment(1);
}

fn record_tail_progress_cleanup() {
    metrics::counter!("tycho_storage_rpc_tail_progress_cleanups_total").increment(1);
}

fn gc_phase_rank(phase: GcIntentPhase) -> u8 {
    match phase {
        GcIntentPhase::Evacuating => 0,
        GcIntentPhase::Prepared => 1,
        GcIntentPhase::CutoverCommitted => 2,
        GcIntentPhase::Deleting => 3,
    }
}

fn gc_phase_result_label(result: GcPhaseResult) -> &'static str {
    match result {
        GcPhaseResult::Transitioned => "transitioned",
        GcPhaseResult::Completed => "completed",
    }
}

fn record_gc_phase_duration(observed: ObservedGcPhase, result: GcPhaseResult) {
    metrics::histogram!(
        "tycho_storage_rpc_gc_phase_duration_seconds",
        "phase" => gc_phase_label(observed.phase),
        "result" => gc_phase_result_label(result),
    )
    .record(observed.started_at.elapsed());
}

fn set_current_gc_phase_metrics(current: Option<GcIntentPhase>) {
    for (phase, label) in GC_INTENT_PHASES {
        let active = current == Some(phase);
        metrics::gauge!("tycho_storage_rpc_gc_current_phase", "phase" => label)
            .set(if active { 1.0 } else { 0.0 });
    }
}

fn gc_pass_sealing_pending(
    context: &GcSchedulerContext,
    pass: &GcEligibilityPass,
) -> Result<bool> {
    let storage = context
        .storage
        .upgrade()
        .context("RPC transaction GC pass storage was dropped")?;
    let pending = storage.gc_pass_sealing_pending(pass.pending_sealing);
    drop(storage);
    Ok(pending)
}

async fn acquire_background_permit_after_transient_check<F, P>(
    mut check: F,
    permit: P,
) -> Result<Option<MaintenancePermit>>
where
    F: FnMut() -> Result<bool>,
    P: Future<Output = Result<MaintenancePermit>>,
{
    if check()? {
        return Ok(None);
    }
    let permit = permit.await?;
    if check()? {
        drop(permit);
        return Ok(None);
    }
    Ok(Some(permit))
}

fn validate_deletion_path(guard: &PartitionDeletionGuard) -> Result<()> {
    validate_partition_path(guard.id(), guard.path())
}

fn validate_partition_path(id: PartitionId, path: &Path) -> Result<()> {
    let expected_name = id.directory_name();
    if path.file_name().and_then(|name| name.to_str()) != Some(expected_name.as_str()) {
        return Err(conflicting_authoritative_error(
            "RPC transaction partition deletion guard has an invalid directory name",
        ));
    }
    if path.parent().and_then(Path::file_name).and_then(|name| name.to_str())
        != Some("transactions")
    {
        return Err(conflicting_authoritative_error(
            "RPC transaction partition deletion guard is outside the transactions directory",
        ));
    }
    Ok(())
}

fn remove_source_directory_once(path: &Path) -> Result<()> {
    match fs::remove_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(error).with_context(|| {
            format!("failed to remove RPC transaction partition at {}", path.display())
        }),
    }
    let parent = path.parent().context("RPC transaction partition path has no parent")?;
    File::open(parent)
        .with_context(|| format!("failed to open RPC transaction partition parent at {}", parent.display()))?
        .sync_all()
        .with_context(|| format!("failed to sync RPC transaction partition parent at {}", parent.display()))?;
    ensure!(!path.try_exists().with_context(|| {
        format!("failed to confirm RPC transaction partition absence at {}", path.display())
    })?, "RPC transaction partition directory reappeared at {}", path.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::{Future, poll_fn};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::Poll;

    use super::*;
    use super::super::partition::tests::TestMetricsRecorder;

    #[test]
    fn gc_restart_resume_metrics_use_only_bounded_phase_labels() {
        let recorder = TestMetricsRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            for (phase, _) in GC_INTENT_PHASES {
                record_gc_restart_resume(phase);
            }
        });

        let mut keys = recorder
            .keys()
            .into_iter()
            .filter(|key| key.starts_with("tycho_storage_rpc_gc_restart_resumes_total"))
            .collect::<Vec<_>>();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "tycho_storage_rpc_gc_restart_resumes_total|phase=cutover_committed",
                "tycho_storage_rpc_gc_restart_resumes_total|phase=deleting",
                "tycho_storage_rpc_gc_restart_resumes_total|phase=evacuating",
                "tycho_storage_rpc_gc_restart_resumes_total|phase=prepared",
            ]
            .map(str::to_owned),
        );
        assert!(keys.iter().all(|key| recorder.counter(key) == 1));
    }

    #[test]
    fn gc_lifecycle_metrics_record_exact_unlabeled_observations() {
        let recorder = TestMetricsRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            record_gc_startup_pass_started();
            record_gc_startup_pass_completed(Duration::from_secs(1));
            record_gc_pass_completed(17, Duration::from_secs(4));
            record_gc_source_reference_wait(Duration::from_secs(2));
            record_gc_source_deletion(Duration::from_secs(3));
            record_gc_source_delete_retry();
            record_gc_certified_orphan_removed();
            record_tail_progress_cleanup();
        });

        let mut keys = recorder.keys();
        keys.sort_unstable();
        let mut expected = [
            "tycho_storage_rpc_gc_startup_pass_started_total",
            "tycho_storage_rpc_gc_startup_pass_duration_seconds",
            "tycho_storage_rpc_gc_startup_pass_completed_total",
            "tycho_storage_rpc_gc_pass_duration_seconds",
            "tycho_storage_rpc_gc_last_completed_mc_block_seqno",
            "tycho_storage_rpc_gc_source_reference_wait_time",
            "tycho_storage_rpc_gc_source_deletion_time",
            "tycho_storage_rpc_gc_source_delete_retries_total",
            "tycho_storage_rpc_gc_certified_orphans_removed_total",
            "tycho_storage_rpc_tail_progress_cleanups_total",
        ]
        .map(str::to_owned);
        expected.sort_unstable();
        assert_eq!(keys, expected);
        assert_eq!(recorder.counter("tycho_storage_rpc_gc_startup_pass_started_total"), 1);
        assert_eq!(recorder.counter("tycho_storage_rpc_gc_startup_pass_completed_total"), 1);
        assert_eq!(recorder.counter("tycho_storage_rpc_gc_source_delete_retries_total"), 1);
        assert_eq!(recorder.counter("tycho_storage_rpc_gc_certified_orphans_removed_total"), 1);
        assert_eq!(recorder.counter("tycho_storage_rpc_tail_progress_cleanups_total"), 1);
        assert_eq!(recorder.gauge("tycho_storage_rpc_gc_last_completed_mc_block_seqno"), 17.0);
        assert_eq!(
            recorder.histogram_values("tycho_storage_rpc_gc_startup_pass_duration_seconds"),
            [1.0],
        );
        assert_eq!(
            recorder.histogram_values("tycho_storage_rpc_gc_pass_duration_seconds"),
            [4.0],
        );
        assert_eq!(
            recorder.histogram_values("tycho_storage_rpc_gc_source_reference_wait_time"),
            [2.0],
        );
        assert_eq!(
            recorder.histogram_values("tycho_storage_rpc_gc_source_deletion_time"),
            [3.0],
        );
    }

    #[test]
    fn gc_scheduler_enforces_zero_pending_start_interval() {
        let started_at = Instant::now();
        let startup_frontier = BlockId {
            seqno: 1,
            ..Default::default()
        };
        let mut scheduler = GcPassScheduler::default();
        scheduler
            .configure_startup(
                Weak::new(),
                Some(TransactionsGcConfig::default()),
                startup_frontier,
                None,
                started_at,
            )
            .unwrap();
        assert_eq!(scheduler.last_started_at, Some(started_at));
        assert_eq!(scheduler.active.unwrap().frontier, startup_frontier);

        let dropped_frontier = BlockId {
            seqno: 2,
            ..Default::default()
        };
        assert_eq!(
            scheduler.admit_frontier(
                dropped_frontier,
                None,
                started_at + Duration::from_secs(60 * 20),
                false,
                false,
            ),
            GcFrontierAdmission::Busy,
        );
        assert_eq!(scheduler.complete_active().unwrap().frontier, startup_frontier);
        assert!(scheduler.active.is_none());

        assert_eq!(
            scheduler.admit_frontier(
                dropped_frontier,
                None,
                started_at + Duration::from_secs(60 * 10 - 1),
                false,
                false,
            ),
            GcFrontierAdmission::RateLimited,
        );
        assert!(scheduler.active.is_none());

        let accepted_frontier = BlockId {
            seqno: 3,
            ..Default::default()
        };
        let accepted_at = started_at + Duration::from_secs(60 * 10);
        assert_eq!(
            scheduler.admit_frontier(accepted_frontier, None, accepted_at, false, false),
            GcFrontierAdmission::Accepted,
        );
        assert_eq!(scheduler.last_started_at, Some(accepted_at));
        assert_eq!(scheduler.active.unwrap().frontier, accepted_frontier);
        assert_eq!(scheduler.complete_active().unwrap().frontier, accepted_frontier);
        assert!(scheduler.active.is_none());
    }

    #[test]
    fn gc_scheduler_rejects_disabled_cancelled_and_resync_frontiers() {
        let now = Instant::now();
        let frontier = BlockId {
            seqno: 1,
            ..Default::default()
        };
        let mut disabled = GcPassScheduler::default();
        disabled
            .configure_startup(Weak::new(), None, frontier, None, now)
            .unwrap();
        disabled.complete_active();
        assert_eq!(
            disabled.admit_frontier(frontier, None, now, false, false),
            GcFrontierAdmission::Disabled,
        );

        let mut scheduler = GcPassScheduler::default();
        scheduler
            .configure_startup(
                Weak::new(),
                Some(TransactionsGcConfig::default()),
                frontier,
                None,
                now,
            )
            .unwrap();
        scheduler.complete_active();
        assert_eq!(
            scheduler.admit_frontier(
                frontier,
                None,
                now + Duration::from_secs(60 * 10),
                true,
                false,
            ),
            GcFrontierAdmission::Cancelled,
        );
        assert_eq!(
            scheduler.admit_frontier(
                frontier,
                None,
                now + Duration::from_secs(60 * 10),
                false,
                true,
            ),
            GcFrontierAdmission::ResyncRequired,
        );
        scheduler.closed = true;
        assert_eq!(
            scheduler.admit_frontier(
                frontier,
                None,
                now + Duration::from_secs(60 * 10),
                false,
                false,
            ),
            GcFrontierAdmission::Cancelled,
        );
        assert!(scheduler.active.is_none());
    }

    #[test]
    fn gc_phase_metrics_use_only_bounded_phase_and_result_labels() {
        let recorder = TestMetricsRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            for (phase, _) in GC_INTENT_PHASES {
                let observed = ObservedGcPhase {
                    operation_id: 1,
                    phase,
                    started_at: Instant::now(),
                };
                record_gc_phase_duration(observed, GcPhaseResult::Transitioned);
                record_gc_phase_duration(observed, GcPhaseResult::Completed);
            }
            set_current_gc_phase_metrics(Some(GcIntentPhase::Prepared));
        });

        for (phase, label) in GC_INTENT_PHASES {
            assert_eq!(gc_phase_label(phase), label);
            assert_eq!(
                recorder.histogram_len(&format!(
                    "tycho_storage_rpc_gc_phase_duration_seconds|phase={label}|result=transitioned"
                )),
                1,
            );
            assert_eq!(
                recorder.histogram_len(&format!(
                    "tycho_storage_rpc_gc_phase_duration_seconds|phase={label}|result=completed"
                )),
                1,
            );
            assert_eq!(
                recorder.gauge(&format!("tycho_storage_rpc_gc_current_phase|phase={label}")),
                if phase == GcIntentPhase::Prepared { 1.0 } else { 0.0 },
            );
        }
        metrics::with_local_recorder(&recorder, || set_current_gc_phase_metrics(None));
        for (_, label) in GC_INTENT_PHASES {
            assert_eq!(
                recorder.gauge(&format!("tycho_storage_rpc_gc_current_phase|phase={label}")),
                0.0,
            );
        }
        assert!(recorder.keys().iter().all(|key| {
            !key.contains("operation_id")
                && !key.contains("partition_id")
                && !key.contains("generation")
                && !key.contains("cutoff")
                && !key.contains("policy")
                && !key.contains("current_phase_age")
        }));
    }

    #[test]
    fn evacuation_observation_precedes_work_and_preserves_prepared_result() {
        let intent = GcIntent {
            phase: GcIntentPhase::Evacuating,
            operation_id: 1,
            source_partition_id: 1,
            source_manifest_digest: tycho_types::cell::HashBytes([1; 32]),
            target_generation: 1,
            previous_visible_generation: 0,
            cutoff_utime: 1,
            keep_tx_per_account: 1,
            retention_policy_digest: tycho_types::cell::HashBytes([2; 32]),
        };
        let prepared = GcIntent {
            phase: GcIntentPhase::Prepared,
            ..intent
        };
        let observed = Mutex::new(None);
        let cancelled = CancellationFlag::new();
        let recorder = TestMetricsRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            observe_gc_intent_state(&observed, &cancelled, intent);
            match gc_pass_step_after_evacuation(
                intent,
                GcEvacuationChunkResult::Prepared(prepared),
            ) {
                GcPassStep::Prepared(actual) => {
                    assert_eq!(actual, prepared);
                    observe_gc_intent_state(&observed, &cancelled, actual);
                }
                _ => panic!("terminal evacuation result must preserve Prepared"),
            }
        });
        assert_eq!(
            recorder.histogram_len(
                "tycho_storage_rpc_gc_phase_duration_seconds|phase=evacuating|result=transitioned"
            ),
            1,
        );
        assert_eq!(
            recorder.gauge("tycho_storage_rpc_gc_current_phase|phase=evacuating"),
            0.0,
        );
        assert_eq!(
            recorder.gauge("tycho_storage_rpc_gc_current_phase|phase=prepared"),
            1.0,
        );
    }

    #[tokio::test]
    async fn startup_precheck_does_not_wait_for_background_permit() {
        let maintenance = MaintenanceCoordinator::new(1);
        let blocker = maintenance.acquire(MaintenancePriority::Background).await.unwrap();
        let checks = AtomicUsize::new(0);

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            acquire_background_permit_after_transient_check(
                || {
                    checks.fetch_add(1, Ordering::AcqRel);
                    Ok(true)
                },
                async {
                    Ok(maintenance
                        .acquire(MaintenancePriority::Background)
                        .await?)
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();

        assert!(result.is_none());
        assert_eq!(checks.load(Ordering::Acquire), 1);
        assert_eq!(maintenance.background_waiters(), 0);
        drop(blocker);
    }

    #[tokio::test]
    async fn startup_rechecks_after_sealing_overtakes_background_waiter() {
        let maintenance = MaintenanceCoordinator::new(1);
        let blocker = maintenance.acquire(MaintenancePriority::Background).await.unwrap();
        let pending = Arc::new(AtomicBool::new(false));
        let checks = Arc::new(AtomicUsize::new(0));
        let worker_maintenance = maintenance.clone();
        let worker_pending = pending.clone();
        let worker_checks = checks.clone();
        let mut worker = tokio::spawn(async move {
            acquire_background_permit_after_transient_check(
                || {
                    worker_checks.fetch_add(1, Ordering::AcqRel);
                    Ok(worker_pending.load(Ordering::Acquire))
                },
                async {
                    Ok(worker_maintenance
                        .acquire(MaintenancePriority::Background)
                        .await?)
                },
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while maintenance.background_waiters() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(checks.load(Ordering::Acquire), 1);
        pending.store(true, Ordering::Release);

        let mut sealing = Box::pin(maintenance.acquire(MaintenancePriority::Sealing));
        poll_fn(|cx| match sealing.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(_) => panic!("sealing acquired the blocked maintenance permit"),
        })
        .await;
        drop(blocker);
        let sealing_permit = tokio::time::timeout(Duration::from_secs(5), &mut sealing)
            .await
            .unwrap()
            .unwrap();
        assert!(!worker.is_finished());
        drop(sealing_permit);

        let result = tokio::time::timeout(Duration::from_secs(5), &mut worker)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(result.is_none());
        assert_eq!(checks.load(Ordering::Acquire), 2);
        assert_eq!(maintenance.background_waiters(), 0);
    }
}
