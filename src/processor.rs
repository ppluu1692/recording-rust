use anyhow::Result;
use crate::writer::AudioSink;
use rubato::{Resampler, Fft, FixedSync};
use rubato::audioadapter_buffers::direct::SequentialSliceOfSlices;

pub struct MonoDownmixSink {
    inner: Box<dyn AudioSink>,
    channels: u16,
}

impl MonoDownmixSink {
    pub fn new(inner: Box<dyn AudioSink>, channels: u16) -> Self {
        Self { inner, channels }
    }
}

impl AudioSink for MonoDownmixSink {
    fn write_samples(&mut self, samples: &[f32]) -> Result<()> {
        if self.channels == 1 {
            self.inner.write_samples(samples)?;
            return Ok(());
        }
        let channels = self.channels as usize;
        let mut mono = Vec::with_capacity(samples.len() / channels);
        for frame in samples.chunks_exact(channels) {
            let sum: f32 = frame.iter().sum();
            mono.push(sum / channels as f32);
        }
        self.inner.write_samples(&mono)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }

    fn finalize(self: Box<Self>) -> Result<()> {
        self.inner.finalize()
    }
}

pub struct ResamplerSink {
    inner: Box<dyn AudioSink>,
    resampler: Fft<f32>,
    input_chunk_size: usize,
    input_buffer: Vec<f32>,
    output_buffer: Vec<f32>,
}

impl ResamplerSink {
    pub fn new(inner: Box<dyn AudioSink>, input_rate: u32, output_rate: u32) -> Result<Self> {
        let chunk_size = 1024;
        let resampler = Fft::<f32>::new(
            input_rate as usize,
            output_rate as usize,
            chunk_size,
            2,
            1,
            FixedSync::Input,
        ).map_err(|e| anyhow::anyhow!("Failed to construct resampler: {:?}", e))?;

        let input_chunk_size = resampler.input_frames_next();
        let output_buffer_size = resampler.output_frames_max();

        Ok(Self {
            inner,
            resampler,
            input_chunk_size,
            input_buffer: Vec::with_capacity(input_chunk_size),
            output_buffer: vec![0.0; output_buffer_size],
        })
    }
}

impl AudioSink for ResamplerSink {
    fn write_samples(&mut self, samples: &[f32]) -> Result<()> {
        self.input_buffer.extend_from_slice(samples);

        while self.input_buffer.len() >= self.input_chunk_size {
            let input_chunk = &self.input_buffer[..self.input_chunk_size];

            let input_slices = &[input_chunk];
            let input_adapter = SequentialSliceOfSlices::new(input_slices, 1, self.input_chunk_size)
                .map_err(|e| anyhow::anyhow!("Failed to create input adapter: {:?}", e))?;

            let out_len = self.output_buffer.len();
            let mut output_slices = [&mut self.output_buffer[..]];
            let mut output_adapter = SequentialSliceOfSlices::new_mut(&mut output_slices, 1, out_len)
                .map_err(|e| anyhow::anyhow!("Failed to create output adapter: {:?}", e))?;

            let (_, frames_written) = self.resampler
                .process_into_buffer(&input_adapter, &mut output_adapter, None)
                .map_err(|e| anyhow::anyhow!("Resampling error: {:?}", e))?;

            self.inner.write_samples(&self.output_buffer[..frames_written])?;

            self.input_buffer.drain(..self.input_chunk_size);
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }

    fn finalize(mut self: Box<Self>) -> Result<()> {
        if !self.input_buffer.is_empty() {
            let remaining = self.input_buffer.len();
            self.input_buffer.resize(self.input_chunk_size, 0.0);

            let input_slices = &[&self.input_buffer[..]];
            let input_adapter = SequentialSliceOfSlices::new(input_slices, 1, self.input_chunk_size)
                .map_err(|e| anyhow::anyhow!("Failed to create input adapter: {:?}", e))?;

            let out_len = self.output_buffer.len();
            let mut output_slices = [&mut self.output_buffer[..]];
            let mut output_adapter = SequentialSliceOfSlices::new_mut(&mut output_slices, 1, out_len)
                .map_err(|e| anyhow::anyhow!("Failed to create output adapter: {:?}", e))?;

            let (_, frames_written) = self.resampler
                .process_into_buffer(&input_adapter, &mut output_adapter, None)
                .map_err(|e| anyhow::anyhow!("Resampling error: {:?}", e))?;

            let ratio = frames_written as f64 / self.input_chunk_size as f64;
            let actual_output_len = ((remaining as f64 * ratio).round() as usize).min(frames_written);

            self.inner.write_samples(&self.output_buffer[..actual_output_len])?;
        }

        self.inner.finalize()?;
        Ok(())
    }
}
