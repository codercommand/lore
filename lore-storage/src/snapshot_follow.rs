// SPDX-FileCopyrightText: 2026 codercommand
// SPDX-License-Identifier: MIT
//! Signals from the local stores to the bucket snapshot follower.
//!
//! A flush holds [`FlushGuard`] for the whole write, so a snapshot walk does not
//! run in the middle of a rename. A branch flush also holds [`PendingBranch`]
//! from the moment it is scheduled until it starts writing, so the follower waits
//! and publishes that commit's index files and branch tips as one batch.
//!
//! Nothing here uploads. With no follower running, the counters are unused.
//!
//! This module is not upstream. Lore does not notify a durable plugin when a
//! local flush finishes. Remove it, and every `snapshot_follow` call in the
//! local stores, when `lore-bucket` snapshots on its interval instead.

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use tokio::sync::Notify;

/// How long a quiet index flush wave is held so groups that finish together
/// share one batch. A branch flush that is already scheduled holds the batch
/// past this, until that flush writes its tips.
const QUIET: Duration = Duration::from_millis(100);
/// Longest an index-only wave is held. Stops a steady trickle of flushes from
/// keeping the manifest still. A scheduled branch flush is not cut short.
const MAX_HOLD: Duration = Duration::from_millis(500);

static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
static GENERATION: AtomicU64 = AtomicU64::new(0);
static BRANCH_PENDING: AtomicUsize = AtomicUsize::new(0);
static REQUESTED: AtomicBool = AtomicBool::new(false);
static NOTIFY: Notify = Notify::const_new();

type IndexFlusher = Box<dyn Fn(bool) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

static INDEX_FLUSH: Mutex<Option<IndexFlusher>> = Mutex::new(None);

/// Held while index or branch files are being renamed. Nested guards stay busy
/// the whole time: an inner guard does not look like a quiet point.
pub struct FlushGuard;

impl FlushGuard {
    pub fn enter() -> Self {
        let previous = IN_FLIGHT.fetch_add(1, Ordering::AcqRel);
        if previous == 0 {
            GENERATION.fetch_add(1, Ordering::AcqRel);
        }
        NOTIFY.notify_waiters();
        Self
    }
}

impl Drop for FlushGuard {
    fn drop(&mut self) {
        let previous = IN_FLIGHT.fetch_sub(1, Ordering::AcqRel);
        if previous == 1 {
            GENERATION.fetch_add(1, Ordering::AcqRel);
        }
        NOTIFY.notify_waiters();
    }
}

/// Held from the schedule of a delayed branch flush until that flush starts
/// writing. The follower will not publish while one of these is alive.
pub struct PendingBranch;

impl PendingBranch {
    pub fn enter() -> Self {
        BRANCH_PENDING.fetch_add(1, Ordering::AcqRel);
        NOTIFY.notify_waiters();
        Self
    }
}

impl Drop for PendingBranch {
    fn drop(&mut self) {
        BRANCH_PENDING.fetch_sub(1, Ordering::AcqRel);
        NOTIFY.notify_waiters();
    }
}

/// Requests one snapshot batch when dropped, after the flush that armed it.
#[must_use = "hold until the flush finishes"]
pub struct RequestBatch;

impl RequestBatch {
    pub fn arm() -> Self {
        Self
    }
}

impl Drop for RequestBatch {
    fn drop(&mut self) {
        request_batch();
    }
}

/// Ask the follower to publish the files that are on disk once flushes are quiet.
pub fn request_batch() {
    REQUESTED.store(true, Ordering::Release);
    NOTIFY.notify_waiters();
}

pub fn in_flight() -> usize {
    IN_FLIGHT.load(Ordering::Acquire)
}

pub fn generation() -> u64 {
    GENERATION.load(Ordering::Acquire)
}

pub fn branch_pending() -> usize {
    BRANCH_PENDING.load(Ordering::Acquire)
}

/// Install the callback a branch flush runs before it renames branch tips.
/// The server points this at the local index flush. Absent, branch flushes
/// do not wait.
pub fn set_index_flusher<F, Fut>(flush: F)
where
    F: Fn(bool) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let flush: IndexFlusher = Box::new(move |sync_data| Box::pin(flush(sync_data)));
    *INDEX_FLUSH.lock().unwrap_or_else(|err| err.into_inner()) = Some(flush);
}

/// Flush the local index, when a callback is installed, before branch tips are written.
pub async fn flush_index_before_branch(sync_data: bool) {
    let flush = INDEX_FLUSH
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .as_ref()
        .map(|flush| flush(sync_data));
    if let Some(flush) = flush {
        flush.await;
    }
}

/// Wait until a flush wave has asked for a batch and writing has stopped.
///
/// Index groups that finish within [`QUIET`] share a batch. A scheduled branch
/// flush holds the batch until it has written, so the tips and the index files
/// they depend on publish together. The wait ends by [`MAX_HOLD`] for an
/// index-only wave so a steady trickle cannot leave the manifest unmoved.
pub async fn wait_for_follow() {
    loop {
        if !REQUESTED.load(Ordering::Acquire) {
            NOTIFY.notified().await;
            continue;
        }
        let started = Instant::now();
        loop {
            if branch_pending() > 0 || in_flight() > 0 {
                NOTIFY.notified().await;
                continue;
            }
            if started.elapsed() >= MAX_HOLD {
                REQUESTED.store(false, Ordering::Release);
                return;
            }
            let generation_at_quiet = generation();
            let wait_for = QUIET.min(MAX_HOLD.saturating_sub(started.elapsed()));
            tokio::select! {
                _ = tokio::time::sleep(wait_for) => {
                    if branch_pending() == 0
                        && in_flight() == 0
                        && (generation() == generation_at_quiet || started.elapsed() >= MAX_HOLD)
                    {
                        REQUESTED.store(false, Ordering::Release);
                        return;
                    }
                }
                _ = NOTIFY.notified() => {}
            }
        }
    }
}
