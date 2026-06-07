use anyhow::Result;
use cpal::{
    traits::{DeviceTrait, StreamTrait},
    Device, SampleFormat, Stream, SupportedStreamConfig,
};
use crossbeam_channel::Sender;
use ringbuf::{
    traits::{Consumer, Producer, Split},
    HeapRb,
};
use std::{thread, time::Duration};

use crate::pipeline::AudioMessage;

const RING_CAPACITY: usize = 4096 * 16;
const DRAIN_INTERVAL_MS: u64 = 5;
const DRAIN_BATCH: usize = 1024;

pub fn start_capture(
    device: &Device,
    config: &SupportedStreamConfig,
    tx: Sender<AudioMessage>,
    label: &str,
) -> Result<(Stream, thread::JoinHandle<()>)> {
    let ring = HeapRb::<f32>::new(RING_CAPACITY);
    let (prod, cons) = ring.split();

    let stream = build_input_stream(device, config, prod, label)?;
    stream.play()?;

    let handle = spawn_drain_thread(cons, tx, label);

    Ok((stream, handle))
}

pub fn stop_capture(stream: &Stream, tx: &Sender<AudioMessage>) -> Result<()> {
    stream.pause()?;
    let _ = tx.send(AudioMessage::Stop);
    Ok(())
}

// ─── private ────────────────────────────────────────────────────────────────

fn build_input_stream(
    device: &Device,
    config: &SupportedStreamConfig,
    mut prod: impl Producer<Item = f32> + Send + 'static,
    label: &str,
) -> Result<Stream> {
    let label_err = label.to_string();

    let stream = match config.sample_format() {
        SampleFormat::F32 => {
            device.build_input_stream(
                config.config(),
                move |data: &[f32], _| {
                    let written = prod.push_slice(data);
                    if written < data.len() {

                    }
                },
                move |e| eprintln!("[{}] stream error: {}", label_err, e),
                None,
            )?
        }

        SampleFormat::I16 => {
            device.build_input_stream(
                config.config(),
                move |data: &[i16], _| {
                    let converted: Vec<f32> =
                        data.iter().map(|&s| s as f32 / i16::MAX as f32).collect();
                    let _ = prod.push_slice(&converted);
                },
                move |e| eprintln!("[{}] stream error: {}", label_err, e),
                None,
            )?
        }

        SampleFormat::U16 => {
            device.build_input_stream(
                config.config(),
                move |data: &[u16], _| {
                    let converted: Vec<f32> = data
                        .iter()
                        .map(|&s| (s as f32 / u16::MAX as f32) * 2.0 - 1.0)
                        .collect();
                    let _ = prod.push_slice(&converted);
                },
                move |e| eprintln!("[{}] stream error: {}", label_err, e),
                None,
            )?
        }

        fmt => anyhow::bail!("Sample format {:?} unsupported", fmt),
    };

    Ok(stream)
}

fn spawn_drain_thread(
    mut cons: impl Consumer<Item = f32> + Send + 'static,
    tx: Sender<AudioMessage>,
    label: &str,
) -> thread::JoinHandle<()> {
    let label = label.to_string();
    let mut batch = vec![0f32; DRAIN_BATCH];

    thread::Builder::new()
        .name(format!("drain-{}", label))
        .spawn(move || {
            let mut check_counter = 0;
            loop {
                let n = cons.pop_slice(&mut batch);

                if n > 0 {
                    let samples = batch[..n].to_vec();
                    if let Err(e) = tx.try_send(AudioMessage::Samples(samples)) {
                        match e {
                            crossbeam_channel::TrySendError::Full(_) => {
                                eprintln!("[{}] channel full, drop batch {} samples", label, n);
                            }
                            crossbeam_channel::TrySendError::Disconnected(_) => {
                                break;
                            }
                        }
                    }
                } else {
                    check_counter += 1;
                    if check_counter >= 100 {
                        check_counter = 0;
                        if let Err(crossbeam_channel::TrySendError::Disconnected(_)) =
                            tx.try_send(AudioMessage::Samples(Vec::new()))
                        {
                            break;
                        }
                    }
                }

                thread::sleep(Duration::from_millis(DRAIN_INTERVAL_MS));
            }
        })
        .expect("spawn drain thread failed")
}