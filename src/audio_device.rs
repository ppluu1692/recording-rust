use anyhow::{Context, Result};
use cpal::{
    traits::{DeviceTrait, HostTrait},
    Device, Host, SupportedStreamConfig,
};

pub struct RecordingDevices {
    pub mic: Option<Device>,
    pub speaker: Option<Device>,
}

pub fn get_host() -> Host {
    cpal::default_host()
}

pub fn get_default_mic(host: &Host) -> Option<Device> {
    host.default_input_device()
}

pub fn get_default_speaker(host: &Host) -> Option<Device> {
    host.default_output_device()
}

pub fn get_default_devices(host: &Host) -> RecordingDevices {
    RecordingDevices { 
        mic: get_default_mic(host), 
        speaker: get_default_speaker(host),
    }
}

pub fn get_input_config(device: &Device) -> Result<SupportedStreamConfig> {
    let config = device.default_input_config()?;
    Ok(config)
}

pub fn get_output_config(device: &Device) -> Result<SupportedStreamConfig> {
    let config = device.default_output_config()?;
    Ok(config)
}

pub fn list_input_devices(host: &cpal::Host) -> Result<()> {
    println!("── Input devices ──────────────────────");
    if let Ok(devices) = host.input_devices() {
        for (i, device) in devices.enumerate() {
            let name = match device.description() {
                Ok(description) => description.name().to_string(),
                Err(_) => "Unknown device".to_string(),
            };
            println!("  [{}] {}", i, name);
        }
    }
    Ok(())
}

pub fn list_output_devices(host: &Host) -> Result<()> {
    println!("── Output devices ──────────────────────");
    if let Ok(devices) = host.output_devices() {
        for (i, device) in devices.enumerate() {
            let name = match device.description() {
                Ok(description) => description.name().to_string(),
                Err(_) => "Unknown device".to_string(),
            };
            println!("  [{}] {}", i, name);
        }
    }
    Ok(())
}