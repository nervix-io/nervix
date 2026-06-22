use std::{
    env, fs,
    io::{self, Read},
    net::IpAddr,
    path::PathBuf,
};

use flate2::read::GzDecoder;

fn main() -> io::Result<()> {
    let source = PathBuf::from("dbip-city-lite-2026-06.mmdb.gz");
    println!("cargo:rerun-if-changed={}", source.display());

    let compressed = fs::File::open(&source)?;
    let mut decoder = GzDecoder::new(compressed);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR must be set"));
    let destination = out_dir.join("dbip-city-lite-2026-06.mmdb");
    fs::write(&destination, decompressed)?;
    let reader = maxminddb::Reader::open_readfile(&destination).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid MaxMind DB: {error}"),
        )
    })?;

    for sample_ip in [
        "8.8.8.8",
        "1.1.1.1",
        "9.9.9.9",
        "80.80.80.80",
        "185.199.108.133",
    ] {
        let ip = sample_ip.parse::<IpAddr>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid generated sample IP {sample_ip}: {error}"),
            )
        })?;
        let result = reader.lookup(ip).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("sample IP {sample_ip} does not resolve in MMDB: {error}"),
            )
        })?;
        let city = result
            .decode::<maxminddb::geoip2::City>()
            .map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("sample IP {sample_ip} is not a city record: {error}"),
                )
            })?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("sample IP {sample_ip} is missing from MMDB"),
                )
            })?;
        if city.location.latitude.is_none() || city.location.longitude.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("sample IP {sample_ip} has no city coordinates in MMDB"),
            ));
        }
    }
    Ok(())
}
