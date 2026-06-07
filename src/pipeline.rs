#[derive(Debug)]
pub enum AudioMessage {
    Samples(Vec<f32>),
    Stop,
    Flush,
}

#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub label: String,
    pub channels: u16,
    pub sample_rate: u32,
}
 
impl StreamConfig {
    pub fn new(label: &str, channels: u16, sample_rate: u32) -> Self {
        Self {
            label: label.to_string(),
            channels,
            sample_rate,
        }
    }
}