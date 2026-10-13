//! Device-side mouse jiggler.
//!
//! Keeps the target host awake while nobody is using the console. The loop runs
//! entirely on the device: it only needs the configured interval and the shared
//! [`InputActivity`] clock, so it keeps working when the Web UI is closed.
//!
//! A jiggle displaces the cursor and moves it back. If the user touches the
//! keyboard or mouse while the cursor is displaced, the return move is dropped
//! instead of fighting the user's input (see [`jiggle`]).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{watch, Mutex, Notify};
use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::config::MouseJigglerConfig;
use crate::error::Result;

use super::{HidController, MouseEvent};

/// PiKVM displaces by ±10 device units in relative mode and ±100 pixels in
/// absolute mode. Absolute moves are scaled by the capture resolution.
const RELATIVE_OFFSET: i32 = 10;
const ABSOLUTE_OFFSET_PX: i32 = 100;
/// Absolute HID mouse coordinates span 0..=32767.
const ABS_MAX: i32 = 32767;
/// How long the cursor stays displaced before the return move.
const MOVE_HOLD: Duration = Duration::from_millis(150);
/// Retry delay while the HID backend is unavailable.
const UNAVAILABLE_RETRY: Duration = Duration::from_secs(5);
/// Matches PiKVM's randomization of the interval by ±25%.
const JITTER_PERCENT: u64 = 25;

/// Monotonic clock of real user input, shared by [`HidController`] and the
/// jiggler.
///
/// [`InputActivity::touch`] sits on the HID hot path, so it only performs a
/// relaxed atomic store plus a best-effort wakeup. The generation counter lets
/// the jiggler detect input that arrives while it is mid-move, and the interval
/// clock lets it tell whether the host has been idle long enough.
#[derive(Debug)]
pub struct InputActivity {
    base: Instant,
    last_ms: AtomicU64,
    generation: AtomicU64,
    changed: Notify,
}

impl Default for InputActivity {
    fn default() -> Self {
        Self::new()
    }
}

impl InputActivity {
    pub fn new() -> Self {
        Self {
            base: Instant::now(),
            last_ms: AtomicU64::new(0),
            generation: AtomicU64::new(0),
            changed: Notify::new(),
        }
    }

    /// Record real user input. Jiggler-generated events deliberately never call
    /// this, so the jiggler cannot keep itself awake forever.
    pub fn touch(&self) {
        self.last_ms.store(self.now_ms(), Ordering::Relaxed);
        self.generation.fetch_add(1, Ordering::Relaxed);
        self.changed.notify_waiters();
    }

    /// Time elapsed since the last real user input.
    pub fn idle_for(&self) -> Duration {
        Duration::from_millis(self.now_ms().saturating_sub(self.last_ms.load(Ordering::Relaxed)))
    }

    /// Monotonic count of real user input events.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    /// Resolves on the next real user input event. Cancel-safe, so it can be
    /// raced against a timer to abandon a jiggle early.
    pub async fn wait_for_input(&self) {
        self.changed.notified().await;
    }

    fn now_ms(&self) -> u64 {
        self.base.elapsed().as_millis() as u64
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct JigglerSettings {
    enabled: bool,
    interval: Duration,
    absolute: bool,
}

impl Default for JigglerSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            interval: Duration::from_secs(60),
            absolute: false,
        }
    }
}

/// Owns the background jiggle task and applies persisted configuration to it.
pub struct MouseJiggler {
    hid: Arc<HidController>,
    activity: Arc<InputActivity>,
    settings: watch::Sender<JigglerSettings>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl MouseJiggler {
    pub fn new(hid: Arc<HidController>, activity: Arc<InputActivity>) -> Self {
        let (settings, _receiver) = watch::channel(JigglerSettings::default());
        Self {
            hid,
            activity,
            settings,
            task: Mutex::new(None),
        }
    }

    /// Apply persisted configuration. Cheap and idempotent: callers invoke this
    /// on every USB config apply, including ones that only touch other HID
    /// fields, so a running task picks up interval changes through the watch
    /// channel without being restarted.
    pub async fn apply_config(&self, config: &MouseJigglerConfig, absolute: bool) {
        let next = JigglerSettings {
            enabled: config.enabled,
            // Clamp as defense in depth: validation rejects these values, but a
            // hand-edited config file could still carry them and a zero interval
            // would turn the loop into a busy spin.
            interval: Duration::from_secs(u64::from(config.interval_secs).clamp(1, 3600)),
            absolute,
        };
        self.settings.send_replace(next);

        let mut task = self.task.lock().await;
        if !next.enabled {
            if let Some(handle) = task.take() {
                handle.abort();
                info!("Mouse jiggler stopped");
            }
            return;
        }

        if task.as_ref().is_some_and(|handle| !handle.is_finished()) {
            return;
        }

        *task = Some(self.spawn());
        info!(
            interval_secs = next.interval.as_secs(),
            absolute = next.absolute,
            "Mouse jiggler started"
        );
    }

    /// Stop the background task, if any. Called during runtime shutdown.
    pub async fn stop(&self) {
        if let Some(handle) = self.task.lock().await.take() {
            handle.abort();
        }
    }

    fn spawn(&self) -> JoinHandle<()> {
        let hid = self.hid.clone();
        let activity = self.activity.clone();
        let mut settings_rx = self.settings.subscribe();

        tokio::spawn(async move {
            loop {
                let settings = *settings_rx.borrow_and_update();
                if !settings.enabled {
                    break;
                }

                if !hid.is_available().await {
                    tokio::select! {
                        _ = tokio::time::sleep(UNAVAILABLE_RETRY) => continue,
                        _ = settings_rx.changed() => continue,
                    }
                }

                // Wait out the inactivity window. `interval` doubles as the
                // inactivity threshold and the cycle period, matching PiKVM.
                let idle = activity.idle_for();
                if idle < settings.interval {
                    let wait = settings.interval - idle;
                    tokio::select! {
                        _ = tokio::time::sleep(wait) => {}
                        _ = settings_rx.changed() => continue,
                        _ = activity.wait_for_input() => continue,
                    }
                }
                // The user may have come back while we were waiting.
                if activity.idle_for() < settings.interval {
                    continue;
                }

                if let Err(error) = jiggle(&hid, &activity, settings.absolute).await {
                    debug!("Mouse jiggle skipped: {}", error);
                }

                // The jiggle deliberately does not reset the activity clock, so
                // the next cycle sits one (jittered) interval after this one.
                let delay = jittered(settings.interval);
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = settings_rx.changed() => {}
                }
            }
            debug!("Mouse jiggler task finished");
        })
    }
}

/// Move the cursor away from its current position and back again.
///
/// Both mouse modes have the same hazard: the user can move the mouse during
/// [`MOVE_HOLD`]. Relative deltas are additive, so the reverse move would yank
/// the cursor back against the user's motion; absolute moves are
/// last-write-wins, so a stale return would teleport the cursor to where it was
/// before the user moved. Both cases are detected and the return is skipped —
/// the user's own input already supersedes the jiggle.
async fn jiggle(hid: &HidController, activity: &InputActivity, absolute: bool) -> Result<()> {
    if absolute {
        let origin = hid.last_absolute_position();
        let (dx, dy) = absolute_offset(hid.screen_resolution());
        let displaced = (
            (origin.0 + dx).clamp(0, ABS_MAX),
            (origin.1 + dy).clamp(0, ABS_MAX),
        );

        hid.send_mouse_unattributed(MouseEvent::move_abs(displaced.0, displaced.1))
            .await?;
        hold_or_input(activity, MOVE_HOLD).await;

        // `last_absolute_position` is updated as soon as an absolute move is
        // enqueued, so an unchanged value means the user did not move and the
        // cursor is still exactly where this jiggle put it.
        if hid.last_absolute_position() == displaced {
            hid.send_mouse_unattributed(MouseEvent::move_abs(origin.0, origin.1))
                .await?;
        }
    } else {
        let generation = activity.generation();
        hid.send_mouse_unattributed(MouseEvent::move_rel(RELATIVE_OFFSET, RELATIVE_OFFSET))
            .await?;
        hold_or_input(activity, MOVE_HOLD).await;

        if activity.generation() == generation {
            hid.send_mouse_unattributed(MouseEvent::move_rel(-RELATIVE_OFFSET, -RELATIVE_OFFSET))
                .await?;
        }
    }

    Ok(())
}

/// Sleep for `duration`, returning early as soon as real user input arrives.
async fn hold_or_input(activity: &InputActivity, duration: Duration) {
    tokio::select! {
        _ = tokio::time::sleep(duration) => {}
        _ = activity.wait_for_input() => {}
    }
}

/// Convert the absolute pixel offset into the 0..=32767 HID coordinate space.
fn absolute_offset((width, height): (u32, u32)) -> (i32, i32) {
    let axis = |extent: u32| -> i32 {
        let extent = f64::from(extent.max(1));
        ((f64::from(ABSOLUTE_OFFSET_PX) / extent) * f64::from(ABS_MAX))
            .round()
            .clamp(1.0, f64::from(ABS_MAX)) as i32
    };
    (axis(width), axis(height))
}

/// Randomize `interval` by ±[`JITTER_PERCENT`]%, so the wake cadence is not
/// perfectly periodic. Uses a clock-seeded xorshift instead of the `rand`
/// crate, which is only available with the desktop feature.
fn jittered(interval: Duration) -> Duration {
    let millis = interval.as_millis() as u64;
    let span = millis * JITTER_PERCENT / 100;
    if span == 0 {
        return interval;
    }

    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::from(elapsed.subsec_nanos()))
        .unwrap_or(0);
    let mut x = seed | 1;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    let hash = x.wrapping_mul(0x2545_F491_4F6C_DD1D);

    // Map onto 0..=2*span and shift down to -span..=span.
    let offset = (hash % (span * 2 + 1)) as i64 - span as i64;
    Duration::from_millis((millis as i64 + offset).max(1) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hid::{
        HidBackend, HidBackendRuntimeSnapshot, HidBackendType, KeyboardEvent, MouseEventType,
    };

    struct RecordingBackend {
        events: parking_lot::Mutex<Vec<MouseEvent>>,
        runtime: tokio::sync::watch::Sender<()>,
    }

    impl Default for RecordingBackend {
        fn default() -> Self {
            let (runtime, _receiver) = tokio::sync::watch::channel(());
            Self {
                events: parking_lot::Mutex::new(Vec::new()),
                runtime,
            }
        }
    }

    impl RecordingBackend {
        fn mouse_events(&self) -> Vec<(MouseEventType, i32, i32)> {
            self.events
                .lock()
                .iter()
                .map(|event| (event.event_type, event.x, event.y))
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl HidBackend for RecordingBackend {
        async fn init(&self) -> Result<()> {
            Ok(())
        }

        async fn send_keyboard(&self, _: KeyboardEvent) -> Result<()> {
            Ok(())
        }

        async fn send_mouse(&self, event: MouseEvent) -> Result<()> {
            self.events.lock().push(event);
            Ok(())
        }

        async fn reset(&self) -> Result<()> {
            Ok(())
        }

        async fn shutdown(&self) -> Result<()> {
            Ok(())
        }

        fn runtime_snapshot(&self) -> HidBackendRuntimeSnapshot {
            HidBackendRuntimeSnapshot {
                initialized: true,
                ..Default::default()
            }
        }

        fn subscribe_runtime(&self) -> tokio::sync::watch::Receiver<()> {
            self.runtime.subscribe()
        }
    }

    /// A controller that accepts input and records what reaches the backend.
    async fn harness() -> (Arc<HidController>, Arc<RecordingBackend>) {
        #[cfg(unix)]
        let controller = HidController::new(HidBackendType::None, None);
        #[cfg(not(unix))]
        let controller = HidController::new(HidBackendType::None);

        let backend = Arc::new(RecordingBackend::default());
        *controller.backend.write().await = Some(backend.clone() as Arc<dyn HidBackend>);
        controller.backend_available.store(true, Ordering::Release);
        controller.set_screen_resolution(1920, 1080).await;
        controller.start_event_worker().await;
        (Arc::new(controller), backend)
    }

    /// The event worker processes sends on a blocking thread, so poll instead of
    /// assuming they have landed by the time the sender returns.
    async fn wait_for_events(backend: &RecordingBackend, count: usize) -> Vec<(MouseEventType, i32, i32)> {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let events = backend.mouse_events();
            if events.len() >= count || Instant::now() >= deadline {
                return events;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[test]
    fn idle_clock_tracks_real_input_only() {
        let activity = InputActivity::new();
        assert!(activity.idle_for() >= Duration::from_secs(0));
        let before = activity.generation();
        activity.touch();
        assert_eq!(activity.generation(), before + 1);
        assert!(activity.idle_for() < Duration::from_millis(100));
    }

    #[test]
    fn absolute_offset_scales_with_resolution_and_never_vanishes() {
        let (dx, dy) = absolute_offset((1920, 1080));
        // A fixed pixel offset is a larger share of the shorter axis, so it maps
        // to more HID units on that axis.
        assert!(dy > dx, "the shorter axis must yield the larger offset");
        assert_eq!(dx, ((100.0 / 1920.0) * 32767.0f64).round() as i32);
        assert_eq!(dy, ((100.0 / 1080.0) * 32767.0f64).round() as i32);

        // Degenerate resolutions must still produce a visible movement.
        assert_eq!(absolute_offset((0, 0)), (ABS_MAX, ABS_MAX));
        assert_eq!(absolute_offset((1, 1)), (ABS_MAX, ABS_MAX));
    }

    #[test]
    fn jitter_stays_within_a_quarter_of_the_interval() {
        let interval = Duration::from_secs(60);
        for _ in 0..64 {
            let jiggled = jittered(interval);
            assert!(jiggled >= Duration::from_secs(45), "{jiggled:?}");
            assert!(jiggled <= Duration::from_secs(75), "{jiggled:?}");
        }

        // Too short to jitter meaningfully: returned unchanged.
        assert_eq!(jittered(Duration::from_millis(3)), Duration::from_millis(3));
    }

    #[tokio::test]
    async fn absolute_jiggle_returns_to_the_origin() {
        let (hid, backend) = harness().await;
        hid.send_mouse(MouseEvent::move_abs(8000, 8000)).await.unwrap();
        wait_for_events(&backend, 1).await;

        jiggle(&hid, &hid.activity(), true).await.unwrap();

        let events = wait_for_events(&backend, 3).await;
        let actions: Vec<_> = events.iter().map(|(kind, x, y)| (*kind, *x, *y)).collect();
        assert_eq!(
            actions,
            vec![
                (MouseEventType::MoveAbs, 8000, 8000),
                (MouseEventType::MoveAbs, 8000 + 1707, 8000 + 3034),
                (MouseEventType::MoveAbs, 8000, 8000),
            ]
        );
        assert_eq!(hid.last_absolute_position(), (8000, 8000));
    }

    #[tokio::test]
    async fn absolute_jiggle_yields_to_input_during_the_hold() {
        let (hid, backend) = harness().await;
        hid.send_mouse(MouseEvent::move_abs(8000, 8000)).await.unwrap();
        wait_for_events(&backend, 1).await;

        // The user drags the pointer while the cursor is displaced.
        let user_hid = hid.clone();
        let user = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            user_hid
                .send_mouse(MouseEvent::move_abs(20000, 20000))
                .await
                .unwrap();
        });

        jiggle(&hid, &hid.activity(), true).await.unwrap();
        user.await.unwrap();

        // The return move must be dropped: it would otherwise teleport the
        // cursor back to where it was before the user moved.
        let events = wait_for_events(&backend, 3).await;
        let last = events.last().copied().unwrap();
        assert_eq!(last, (MouseEventType::MoveAbs, 20000, 20000));
        assert_eq!(hid.last_absolute_position(), (20000, 20000));
    }

    #[tokio::test]
    async fn relative_jiggle_yields_to_input_during_the_hold() {
        let (hid, backend) = harness().await;

        let user_hid = hid.clone();
        let user = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            user_hid.send_mouse(MouseEvent::move_rel(50, 0)).await.unwrap();
        });

        jiggle(&hid, &hid.activity(), false).await.unwrap();
        user.await.unwrap();

        // Without the guard the reverse delta would cancel the user's movement.
        let events = wait_for_events(&backend, 2).await;
        let actions: Vec<_> = events.iter().map(|(kind, x, y)| (*kind, *x, *y)).collect();
        assert_eq!(
            actions,
            vec![
                (MouseEventType::Move, RELATIVE_OFFSET, RELATIVE_OFFSET),
                (MouseEventType::Move, 50, 0),
            ]
        );
    }

    #[tokio::test]
    async fn relative_jiggle_returns_when_left_alone() {
        let (hid, backend) = harness().await;

        jiggle(&hid, &hid.activity(), false).await.unwrap();

        let events = wait_for_events(&backend, 2).await;
        let actions: Vec<_> = events.iter().map(|(kind, x, y)| (*kind, *x, *y)).collect();
        assert_eq!(
            actions,
            vec![
                (MouseEventType::Move, RELATIVE_OFFSET, RELATIVE_OFFSET),
                (MouseEventType::Move, -RELATIVE_OFFSET, -RELATIVE_OFFSET),
            ]
        );
    }

    #[tokio::test]
    async fn apply_config_starts_and_stops_the_background_task() {
        let (hid, _backend) = harness().await;
        let jiggler = MouseJiggler::new(hid.clone(), hid.activity());

        jiggler
            .apply_config(
                &MouseJigglerConfig {
                    enabled: true,
                    interval_secs: 60,
                },
                false,
            )
            .await;
        assert!(jiggler.task.lock().await.is_some());

        // Applying an unrelated interval change must not restart the task.
        let handle = jiggler.task.lock().await.as_ref().unwrap().id();
        jiggler
            .apply_config(
                &MouseJigglerConfig {
                    enabled: true,
                    interval_secs: 30,
                },
                false,
            )
            .await;
        assert_eq!(jiggler.task.lock().await.as_ref().unwrap().id(), handle);

        jiggler
            .apply_config(
                &MouseJigglerConfig {
                    enabled: false,
                    interval_secs: 30,
                },
                false,
            )
            .await;
        assert!(jiggler.task.lock().await.is_none());
    }
}
