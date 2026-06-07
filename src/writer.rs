use anyhow::Result;
use crossbeam_channel::Receiver;
use hound::{SampleFormat, WavSpec, WavWriter};
use std::{fs::File, io::BufWriter, thread};

use crate::pipeline::{AudioMessage, StreamConfig};

pub trait AudioSink: Send + 'static {
    fn write_samples(&mut self, samples: &[f32]) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn finalize(self: Box<Self>) -> Result<()>;
}

pub struct WavSink {
    writer: WavWriter<BufWriter<File>>,
}

impl WavSink {
    pub fn new(path: &str, cfg: &StreamConfig) -> Result<Self> {
        let spec = WavSpec {
            channels: cfg.channels,
            sample_rate: cfg.sample_rate,
            bits_per_sample: 32,
            sample_format: SampleFormat::Float,
        };
        let writer = WavWriter::create(path, spec)?;
        Ok(Self { writer })
    }
}

impl AudioSink for WavSink {
    fn write_samples(&mut self, samples: &[f32]) -> Result<()> {
        for &s in samples {
            self.writer.write_sample(s)?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn finalize(self: Box<Self>) -> Result<()> {
        self.writer.finalize()?;
        Ok(())
    }
}

pub fn spawn_writer_thread(
    rx: Receiver<AudioMessage>,
    mut sink: Box<dyn AudioSink>,
    label: String,
) -> thread::JoinHandle<Result<()>> {
    thread::Builder::new()
        .name(format!("writer-{}", label))
        .spawn(move || {
            loop {
                match rx.recv() {
                    Ok(AudioMessage::Samples(samples)) => {
                        sink.write_samples(&samples)?;
                    }
                    Ok(AudioMessage::Flush) => {
                        sink.flush()?;
                    }
                    Ok(AudioMessage::Stop) | Err(_) => {
                        for msg in rx.try_iter() {
                            if let AudioMessage::Samples(s) = msg {
                                sink.write_samples(&s)?;
                            }
                        }
                        break;
                    }
                }
            }

            sink.finalize()?;
            println!("[{}] writer finalized.", label);
            Ok(())
        })
        .expect("spawn writer thread failed")
}