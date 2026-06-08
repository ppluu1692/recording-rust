/// main.rs
///
/// Pipeline tổng thể:
///
///  [Mic callback]    →  ringbuf  →  drain-mic    →  ch_mic    →  writer-mic    → mic.wav
///  [System callback] →  ringbuf  →  drain-system →  ch_system →  writer-system → system.wav
///
/// Khi muốn stream: thay WavSink bằng WebSocketSink / RtpSink trong spawn_writer_thread.
mod audio_device;
mod capture;
mod pipeline;
mod writer;
mod processor;

use anyhow::Result;
use cpal::traits::DeviceTrait;
use crossbeam_channel::bounded;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

// use pipeline::StreamConfig;
// use writer::{WavSink, spawn_writer_thread};

use crate::{pipeline::StreamConfig, processor::{MonoDownmixSink, ResamplerSink}, writer::{WavSink, spawn_writer_thread}};

/// Kích thước crossbeam channel (số AudioMessage tối đa trong hàng đợi).
/// 512 * 1024 samples * 4 bytes ≈ 2MB buffer — thoải mái cho writer bị slow.
const CHANNEL_CAPACITY: usize = 512;

fn main() -> Result<()> {
    println!("🎙️  Audio Recorder");
    println!("   Nhấn Ctrl+C để dừng và lưu file.\n");

    // ── 1. Host & devices ──────────────────────────────────────────────────
    let host = audio_device::get_host();
    audio_device::list_input_devices(&host)?;
    audio_device::list_output_devices(&host)?;
    println!();

    let mic_device = audio_device::get_microphone(&host)?;
    let sys_device = audio_device::get_system_audio(&host)?;

    println!("🎤 Mic    : {}", mic_device.description()?.name());
    println!("🔊 System : {}", sys_device.description()?.name());
    println!();

    // ── 2. Configs ─────────────────────────────────────────────────────────
    let mic_hw_cfg = audio_device::get_input_config(&mic_device)?;
    let sys_hw_cfg = audio_device::get_output_config(&sys_device)?;

    let mic_cfg = StreamConfig::new("mic", mic_hw_cfg.channels(), mic_hw_cfg.sample_rate());
    let sys_cfg = StreamConfig::new("system", sys_hw_cfg.channels(), sys_hw_cfg.sample_rate());

    println!(
        "   {} → {} ch, {} Hz",
        mic_cfg.label, mic_cfg.channels, mic_cfg.sample_rate
    );
    println!(
        "   {} → {} ch, {} Hz",
        sys_cfg.label, sys_cfg.channels, sys_cfg.sample_rate
    );
    println!();

    // ── 3. Channels ────────────────────────────────────────────────────────
    // bounded() để tránh memory leak nếu writer chậm hơn capture.
    let (mic_tx, mic_rx) = bounded(CHANNEL_CAPACITY);
    let (sys_tx, sys_rx) = bounded(CHANNEL_CAPACITY);

    // ── 4. Writer threads ──────────────────────────────────────────────────
    let mic_target_cfg = StreamConfig::new("mic", 1, 16000);
    let sys_target_cfg = StreamConfig::new("system", 1, 16000);

    let mic_wav_sink = Box::new(WavSink::new("mic_output.wav", &mic_target_cfg)?);
    let sys_wav_sink = Box::new(WavSink::new("system_output.wav", &sys_target_cfg)?);

    let mic_resampler = Box::new(ResamplerSink::new(mic_wav_sink, mic_cfg.sample_rate, 16000)?);
    let sys_resampler = Box::new(ResamplerSink::new(sys_wav_sink, sys_cfg.sample_rate, 16000)?);

    let mic_sink: Box<dyn writer::AudioSink> = Box::new(MonoDownmixSink::new(mic_resampler, mic_cfg.channels));
    let sys_sink: Box<dyn writer::AudioSink> = Box::new(MonoDownmixSink::new(sys_resampler, sys_cfg.channels));

    let mic_writer = spawn_writer_thread(mic_rx, mic_sink, "mic".into());
    let sys_writer = spawn_writer_thread(sys_rx, sys_sink, "system".into());

    // ── 5. Capture pipelines ───────────────────────────────────────────────
    // start_capture: tạo ring buffer + cpal stream + drain thread bên trong.
    let (mic_stream, mic_drain) =
        capture::start_capture(&mic_device, &mic_hw_cfg, mic_tx.clone(), "mic")?;

    let (sys_stream, sys_drain) =
        capture::start_capture(&sys_device, &sys_hw_cfg, sys_tx.clone(), "system")?;

    println!("⏺️  Đang ghi âm...\n");

    // ── 6. Ctrl+C handler ──────────────────────────────────────────────────
    let running = Arc::new(AtomicBool::new(true));
    {
        let r = running.clone();
        ctrlc::set_handler(move || {
            r.store(false, Ordering::SeqCst);
        })?;
    }

    let duration_limit = std::env::var("RECORD_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(std::time::Duration::from_secs);

    let start_time = std::time::Instant::now();

    while running.load(Ordering::SeqCst) {
        if let Some(limit) = duration_limit {
            if start_time.elapsed() >= limit {
                println!("\n⏱️  Tự động dừng sau {} giây.", limit.as_secs());
                break;
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // ── 7. Shutdown sequence ───────────────────────────────────────────────
    println!("\n⏹️  Đang dừng...");

    // 7a. Dừng cpal streams → không có samples mới vào ring buffer.
    capture::stop_capture(&mic_stream, &mic_tx)?;
    capture::stop_capture(&sys_stream, &sys_tx)?;

    // 7b. Chờ drain threads xử lý hết ring buffer rồi exit.
    let _ = mic_drain.join();
    let _ = sys_drain.join();

    // 7c. Chờ writer threads finalize file.
    if let Err(e) = mic_writer.join().unwrap() {
        eprintln!("mic writer error: {}", e);
    }
    if let Err(e) = sys_writer.join().unwrap() {
        eprintln!("system writer error: {}", e);
    }

    println!("\n✅ Đã lưu:");
    println!("   mic_output.wav");
    println!("   system_output.wav");

    Ok(())
}