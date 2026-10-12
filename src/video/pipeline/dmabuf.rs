//! RKMPP-only capture/encode worker. Raw buffer ownership never crosses into a
//! latest-frame slot or a network subscriber. Other encoders use shared.rs.
use super::*;
use crate::video::capture::status::capture_recovery_status;
use crate::video::codec::registry::EncoderRegistry;
use hwcodec::rkmpp_dmabuf::{DmaEncoder, DmaEncoderConfig, DmaFormat};

pub(super) fn eligible(config: &SharedVideoPipelineConfig) -> bool {
    if std::env::var("ONE_KVM_RKMPP_DMABUF").as_deref() == Ok("0") {
        return false;
    }
    // The UVC per-frame mapping needed by older BSPs costs more than copying
    // compressed packets in our current tests. Keep JPEG DMA opt-in; raw DMA
    // remains automatic. The existing JPEG hardware transcode is the default.
    if config.input_format == PixelFormat::Mjpeg
        && std::env::var("ONE_KVM_RKMPP_MJPEG_DMABUF").as_deref() != Ok("1")
    {
        return false;
    }
    let registry = EncoderRegistry::global();
    let selected = match config.encoder_backend {
        Some(backend) => registry.encoder_with_backend(config.output_codec, backend),
        None => registry.best_available_encoder(config.output_codec),
    };
    rkmpp_dma_eligible(
        selected.map(|e| e.backend),
        config.output_codec,
        config.input_format,
    )
}

pub(super) fn prepare(
    stream: &CaptureStream,
    config: &SharedVideoPipelineConfig,
) -> Result<DmaEncoder> {
    let buffers = stream
        .export_dmabufs()
        .map_err(|e| AppError::VideoError(e.to_string()))?;
    DmaEncoder::new(
        DmaEncoderConfig {
            width: config.resolution.width,
            height: config.resolution.height,
            stride: stream.stride(),
            format: match stream.format() {
                PixelFormat::Nv12 => DmaFormat::Nv12,
                PixelFormat::Bgr24 => DmaFormat::Bgr24,
                PixelFormat::Yuyv => DmaFormat::Yuyv,
                PixelFormat::Rgb24 => DmaFormat::Rgb24,
                PixelFormat::Mjpeg => DmaFormat::Mjpeg,
                _ => return Err(AppError::VideoError("Unsupported DMA format".into())),
            },
            hevc: config.output_codec == VideoEncoderType::H265,
            fps: config.fps,
            bitrate_kbps: config.bitrate_kbps(),
            gop: config.gop_size().max(1),
        },
        buffers,
    )
    .map_err(AppError::VideoError)
}

enum CaptureEncoder {
    Dma(DmaEncoder),
    Copy(Box<EncoderThreadState>),
}

// Field order is intentional, including during unwinding: destroy the encoder
// and its imported FDs before STREAMOFF/unmap/REQBUFS(0).
struct ActiveCapture {
    encoder: Option<CaptureEncoder>,
    stream: CaptureStream,
}

impl ActiveCapture {
    fn fallback(&mut self, config: &SharedVideoPipelineConfig) -> Result<()> {
        drop(self.encoder.take());
        self.encoder = Some(CaptureEncoder::Copy(Box::new(build_encoder_state(config)?)));
        Ok(())
    }
}

struct Completion(Arc<SharedVideoPipeline>);
impl Drop for Completion {
    fn drop(&mut self) {
        self.0.running_flag.store(false, Ordering::Release);
        self.0.clear_cmd_tx();
        let _ = self.0.encoder_done.send(true);
        let _ = self.0.running.send(false);
        info!("RKMPP capture/encode worker stopped and device resources released");
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn start(
    pipeline: Arc<SharedVideoPipeline>,
    stream: CaptureStream,
    encoder: DmaEncoder,
    config: SharedVideoPipelineConfig,
    device: std::path::PathBuf,
    buffer_count: u32,
    bridge: BridgeContext,
) -> Result<()> {
    let (tx, rx) = mpsc::unbounded_channel();
    *pipeline.cmd_tx.write() = Some(tx);
    pipeline.running_flag.store(true, Ordering::Release);
    let _ = pipeline.encoder_done.send(false);
    let _ = pipeline.running.send(true);
    let worker = pipeline.clone();
    info!(
        "RKMPP DMA candidate: device={} format={:?} resolution={:?} stride={} fps={} source_fps={:?} gop={} bitrate_kbps={}",
        device.display(),
        stream.format(),
        stream.resolution(),
        stream.stride(),
        config.fps,
        stream.source_fps(),
        config.gop_size(),
        config.bitrate_kbps()
    );
    let active = ActiveCapture {
        encoder: Some(CaptureEncoder::Dma(encoder)),
        stream,
    };
    let result = std::thread::Builder::new()
        .name("rkmpp-dmabuf".into())
        .spawn(move || {
            let _completion = Completion(worker.clone());
            if let Err(error) = run(&worker, active, config, device, buffer_count, bridge, rx) {
                error!("RKMPP DMA worker failed: {}", error);
            }
        });
    if let Err(error) = result {
        drop(Completion(pipeline));
        return Err(AppError::VideoError(format!(
            "Failed to start RKMPP DMA worker: {error}"
        )));
    }
    info!("RKMPP DMA capture path active: no CPU raw-frame copies, delivery capacity probed at send time (ONE_KVM_RKMPP_DMABUF=0 disables it)");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run(
    pipeline: &Arc<SharedVideoPipeline>,
    initial: ActiveCapture,
    mut config: SharedVideoPipelineConfig,
    device: std::path::PathBuf,
    buffer_count: u32,
    bridge: BridgeContext,
    mut commands: mpsc::UnboundedReceiver<PipelineCmd>,
) -> Result<()> {
    let policy = CaptureRecoveryPolicy::new(config.control_mode);
    let mut active = Some(initial);
    let mut allow_dma = true;
    let mut failures = 0u32;
    let mut idle_since: Option<Instant> = None;
    let buffer_pool = Arc::new(FrameBufferPool::new(2)); // allocated only on fallback
    let mut fps_frames = 0u32;
    let mut fps_start = Instant::now();
    let errors = LogThrottler::with_secs(5);
    // Temporary diagnostics: one line every 5 s when ONE_KVM_VIDEO_STATS=1.
    // capture_fps comes from the V4L2 buffer sequence, i.e. how many frames the
    // driver actually produced, which separates "the camera only delivers
    // 30 fps" from "the pipeline drops frames the camera did deliver".
    let perf_logging = std::env::var("ONE_KVM_VIDEO_STATS").as_deref() == Ok("1");
    let mut frame_sequence: u64 = 0;
    let mut perf_start = Instant::now();
    let mut perf_loops = 0u64;
    let mut perf_encoded = 0u64;
    let mut perf_delivered = 0u64;
    let mut perf_probe_miss = 0u64;
    let mut perf_first_sequence: Option<u64> = None;
    let mut perf_last_sequence: u64 = 0;
    let mut perf_dequeue_ns = 0u64;
    let mut perf_encode_ns = 0u64;
    let mut perf_keyframes = 0u64;
    let mut perf_bytes = 0u64;

    while pipeline.running_flag.load(Ordering::Acquire) {
        if pipeline.subscriber_count() == 0 {
            if idle_since.get_or_insert_with(Instant::now).elapsed()
                >= Duration::from_secs(AUTO_STOP_GRACE_PERIOD_SECS)
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
            continue;
        }
        idle_since = None;

        while let Ok(command) = commands.try_recv() {
            let PipelineCmd::SetBitrate { preset } = command;
            // Preserve Custom values and preset-specific GOPs across fallback/reopen.
            config.bitrate_preset = preset;
            if let Some(capture) = active.as_mut() {
                match capture.encoder.as_mut().expect("active encoder") {
                    CaptureEncoder::Dma(encoder) => {
                        if let Err(error) =
                            encoder.reconfigure(config.bitrate_kbps(), config.gop_size().max(1))
                        {
                            warn!(
                                "RKMPP DMA reconfigure failed, using copy encoder: {}",
                                error
                            );
                            capture.fallback(&config)?;
                            allow_dma = false;
                            pipeline.keyframe_requested.store(true, Ordering::Release);
                        }
                    }
                    CaptureEncoder::Copy(encoder) => {
                        pipeline.apply_cmd(encoder, PipelineCmd::SetBitrate { preset })?
                    }
                }
            }
        }

        if active.is_none() {
            match open_capture_stream_for_retry(
                &device,
                config.resolution,
                config.input_format,
                config.fps,
                buffer_count.max(2),
                Duration::from_secs(2),
                bridge.clone(),
                config.control_mode,
                is_device_lost_message,
            ) {
                CaptureOpenResult::Opened(stream) => {
                    if stream.resolution() != config.resolution
                        || stream.format() != config.input_format
                    {
                        *pipeline.pending_sync_geometry.lock() =
                            Some((stream.resolution(), stream.format()));
                        break;
                    }
                    config.align_source_fps(stream.source_fps());
                    // Update only timing: a concurrently queued bitrate command
                    // must retain the user's latest preset in the shared config.
                    pipeline.config.blocking_write().fps = config.fps;
                    let encoder = if allow_dma && stream.supports_rkmpp_dmabuf() {
                        match prepare(&stream, &config) {
                            Ok(encoder) => CaptureEncoder::Dma(encoder),
                            Err(error) => {
                                warn!("RKMPP DMA reopen failed, using copy encoder: {}", error);
                                allow_dma = false;
                                CaptureEncoder::Copy(Box::new(build_encoder_state(&config)?))
                            }
                        }
                    } else {
                        CaptureEncoder::Copy(Box::new(build_encoder_state(&config)?))
                    };
                    active = Some(ActiveCapture {
                        encoder: Some(encoder),
                        stream,
                    });
                    pipeline.keyframe_requested.store(true, Ordering::Release);
                }
                CaptureOpenResult::NoSignal(status) => {
                    failures = failures.saturating_add(1);
                    let delay = policy.retry_delay(failures);
                    pipeline.notify_state(PipelineStateNotification::no_signal(
                        status,
                        Some(delay.as_millis() as u64),
                    ));
                    wait_for_source_change(&bridge, delay, || {
                        pipeline.running_flag.load(Ordering::Acquire)
                    });
                    continue;
                }
                CaptureOpenResult::DeviceLost(reason) => {
                    pipeline.mark_device_lost(reason);
                    break;
                }
                CaptureOpenResult::Fatal => break,
            }
        }

        let capture = active.as_mut().expect("opened capture");
        // Only skip capture when nobody is subscribed.  Delivery capacity must
        // not be probed here: this point is microseconds after the previous
        // send, so the one-slot subscriber channel is still occupied and the
        // probe fails on every other iteration, which silently costs the
        // delivery of that frame (60 fps source -> 30 fps delivered).  Capacity
        // is reserved at the delivery point below instead.
        let result = if pipeline.subscriber_count() == 0 {
            match capture.encoder.as_mut().expect("active encoder") {
                CaptureEncoder::Dma(_) => capture
                    .stream
                    .with_next_dmabuf(|_, _, _| ())
                    .map(|_| Ok(Vec::new())),
                CaptureEncoder::Copy(_) => {
                    let mut raw = buffer_pool.take(0);
                    let result = capture.stream.next_into(&mut raw).map(|_| Ok(Vec::new()));
                    buffer_pool.put(raw);
                    result
                }
            }
        } else {
            match capture.encoder.as_mut().expect("active encoder") {
                CaptureEncoder::Dma(encoder) => {
                    let pts = pipeline.pts_ms();
                    let dequeue_started = Instant::now();
                    capture
                        .stream
                        .with_next_dmabuf(|index, bytes_used, fresh_fd| {
                            let dequeue_wait = dequeue_started.elapsed();
                            let keyframe =
                                pipeline.keyframe_requested.swap(false, Ordering::AcqRel);
                            let encode_started = Instant::now();
                            // The callback holds the dequeue lease until native encode
                            // completes (or destroys MPP on error), before QBUF.
                            let encoded = unsafe {
                                encoder.encode(index, bytes_used, fresh_fd, pts, keyframe)
                            };
                            (encoded, dequeue_wait, encode_started.elapsed())
                        })
                        .map(|(meta, (packet, dequeue_wait, encode_time))| {
                            frame_sequence = meta.sequence;
                            perf_dequeue_ns += dequeue_wait.as_nanos() as u64;
                            perf_encode_ns += encode_time.as_nanos() as u64;
                            packet
                                .map(|packet| {
                                    let data = Bytes::from(packet);
                                    let is_keyframe = match config.output_codec {
                                        VideoEncoderType::H264 => {
                                            h264_bitstream::is_keyframe(&data)
                                        }
                                        VideoEncoderType::H265 => {
                                            h265_bitstream::is_keyframe(&data)
                                        }
                                        _ => false,
                                    };
                                    let (data, is_keyframe) = pipeline
                                        .inspect_and_parameterize_packet(
                                            config.output_codec,
                                            data,
                                            is_keyframe,
                                        );
                                    perf_bytes += data.len() as u64;
                                    if is_keyframe {
                                        perf_keyframes += 1;
                                    }
                                    vec![EncodedVideoFrame {
                                        data,
                                        pts_ms: pts,
                                        is_keyframe,
                                        sequence: pipeline.sequence.fetch_add(1, Ordering::Relaxed)
                                            + 1,
                                        duration: Duration::from_micros(
                                            1_000_000 / config.fps.max(1) as u64,
                                        ),
                                        codec: config.output_codec,
                                    }]
                                })
                                .map_err(AppError::VideoError)
                        })
                }
                CaptureEncoder::Copy(encoder) => {
                    let mut raw = buffer_pool.take(0);
                    capture.stream.next_into(&mut raw).map(|meta| {
                        frame_sequence = meta.sequence;
                        let frame = VideoFrame::from_pooled(
                            Arc::new(FrameBuffer::new(raw, Some(buffer_pool.clone()))),
                            config.resolution,
                            config.input_format,
                            capture.stream.stride(),
                            meta.sequence,
                        );
                        pipeline.encode_frame_sync(encoder, &frame)
                    })
                }
            }
        };

        match result {
            Ok(Ok(frames)) => {
                failures = 0;
                pipeline.notify_state(PipelineStateNotification::streaming(
                    config.resolution,
                    config.input_format,
                    config.fps,
                ));
                fps_frames += frames.len() as u32;
                perf_loops += 1;
                perf_encoded += frames.len() as u64;
                if perf_first_sequence.is_none() {
                    perf_first_sequence = Some(frame_sequence);
                }
                perf_last_sequence = frame_sequence;
                // Reserve delivery capacity at the moment of delivery.  The
                // consumer has had a whole frame interval to drain the previous
                // batch here, so a healthy subscriber always has a free slot;
                // a genuinely full slot only skips this one delivery, exactly
                // like the old per-subscriber try_send drop.
                let reservations = pipeline.reserve_outputs();
                if reservations.is_none() {
                    perf_probe_miss += 1;
                }
                let reservations = reservations.unwrap_or_default();
                if !reservations.is_empty() {
                    perf_delivered += frames.len() as u64;
                }
                pipeline.broadcast_encoded_batch(
                    frames.into_iter().map(Arc::new).collect::<Vec<_>>().into(),
                    reservations,
                );
            }
            Ok(Err(error)) => {
                if matches!(capture.encoder, Some(CaptureEncoder::Dma(_))) {
                    warn!("RKMPP DMA encode failed; disabling DMA for this pipeline and using copy encoder: {}", error);
                    capture.fallback(&config)?;
                    allow_dma = false;
                    pipeline.keyframe_requested.store(true, Ordering::Release);
                } else if errors.should_log("copy_encode") {
                    error!("RKMPP copy encode failed: {}", error);
                }
            }
            Err(CaptureReadError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {
                continue
            }
            Err(CaptureReadError::Io(error))
                if error.kind() == std::io::ErrorKind::InvalidData && allow_dma =>
            {
                warn!(
                    "Unsupported RKMPP DMA frame layout, using copy encoder: {}",
                    error
                );
                capture.fallback(&config)?;
                allow_dma = false;
                pipeline.keyframe_requested.store(true, Ordering::Release);
            }
            Err(error) => {
                let mut status = SignalStatus::NoSignal;
                if let CaptureReadError::Io(ref io) = error {
                    if classify_capture_io_error(io) == CaptureIoErrorKind::DeviceLost
                        || is_device_lost_message(&io.to_string())
                    {
                        pipeline.mark_device_lost(io.to_string());
                        break;
                    }
                    if errors.should_log("capture") {
                        warn!("RKMPP DMA capture recovery: {}", io);
                    }
                    status = capture_recovery_status(config.control_mode, io);
                }
                // ActiveCapture drops encoder/imports before the V4L2 stream.
                drop(active.take());
                failures = failures.saturating_add(1);
                let delay = policy.retry_delay(failures);
                pipeline.notify_state(PipelineStateNotification::no_signal(
                    status,
                    Some(delay.as_millis() as u64),
                ));
                if !matches!(error, CaptureReadError::SourceChanged) {
                    wait_for_source_change(&bridge, delay, || {
                        pipeline.running_flag.load(Ordering::Acquire)
                    });
                }
            }
        }
        if fps_start.elapsed() >= Duration::from_secs(1) {
            pipeline.stats.lock().current_fps =
                fps_frames as f32 / fps_start.elapsed().as_secs_f32();
            fps_frames = 0;
            fps_start = Instant::now();
        }
        if perf_logging && perf_start.elapsed() >= Duration::from_secs(5) {
            let elapsed = perf_start.elapsed().as_secs_f64();
            let capture_fps = match perf_first_sequence {
                Some(first) if perf_last_sequence > first => {
                    (perf_last_sequence - first) as f64 / elapsed
                }
                _ => 0.0,
            };
            let per_loop_ms = |ns: u64| {
                if perf_loops > 0 {
                    ns as f64 / perf_loops as f64 / 1_000_000.0
                } else {
                    0.0
                }
            };
            info!(
                "[DmaPerf] window_s={:.1} capture_fps={:.1} loops_fps={:.1} encoded_fps={:.1} delivered_fps={:.1} probe_miss={} dequeue_ms={:.2} encode_ms={:.2} keyframes={} mbps={:.2}",
                elapsed,
                capture_fps,
                perf_loops as f64 / elapsed,
                perf_encoded as f64 / elapsed,
                perf_delivered as f64 / elapsed,
                perf_probe_miss,
                per_loop_ms(perf_dequeue_ns),
                per_loop_ms(perf_encode_ns),
                perf_keyframes,
                perf_bytes as f64 * 8.0 / elapsed / 1_000_000.0
            );
            perf_start = Instant::now();
            perf_loops = 0;
            perf_encoded = 0;
            perf_delivered = 0;
            perf_probe_miss = 0;
            perf_first_sequence = None;
            perf_dequeue_ns = 0;
            perf_encode_ns = 0;
            perf_keyframes = 0;
            perf_bytes = 0;
        }
    }
    // Explicitly release in the worker before Completion publishes stopped.
    drop(active);
    Ok(())
}
