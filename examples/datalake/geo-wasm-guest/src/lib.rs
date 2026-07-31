//! GeoIP enrichment reference guest built on the `nervix-wasm-sdk` crate.
//!
//! Each input row's `source_ip` is resolved against an embedded DB-IP city
//! database into continent, country, region, city, and coordinates, then
//! extended with a geohash and the nearest of a fixed set of hubs. The ten
//! derived values form one shared generated column pool that every declared
//! `TO` relay references alongside the untouched input columns.
//!
//! The processor holds nothing between calls, so it needs neither a saved
//! state nor a quiesce flush: the SDK's defaults are correct for it.

use std::{net::IpAddr, sync::Arc};

use arrow_array::{
    Array, ArrayRef, RecordBatch, StringArray,
    builder::{Float64Builder, StringBuilder},
};
use geo::{Distance, Haversine, Point};
use geohash::{Coord, encode};
use maxminddb::{Reader, geoip2};
use nervix_wasm_sdk::{
    BranchContext, GuestContext, GuestError, InputBatch, OutputColumnRef, OutputEnvelope, Processor,
};

const DBIP_MMDB: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/dbip-city-lite.mmdb"));

const SOURCE_IP_FIELD: &str = "source_ip";
const GEOHASH_PRECISION: usize = 8;
const METERS_PER_KM: f64 = 1000.0;

/// The columns this guest derives, in the order every destination schema must
/// declare them after its input columns.
const GENERATED_FIELDS: &[&str] = &[
    "geoip_database",
    "geoip_continent",
    "geoip_country",
    "geoip_region",
    "geoip_city",
    "geoip_lat",
    "geoip_lon",
    "geoip_geohash",
    "nearest_hub",
    "distance_to_hub_km",
];

#[derive(Clone, Copy)]
struct Hub {
    name: &'static str,
    lat: f64,
    lon: f64,
}

const HUBS: &[Hub] = &[
    Hub {
        name: "sfo",
        lat: 37.7749,
        lon: -122.4194,
    },
    Hub {
        name: "ord",
        lat: 41.8781,
        lon: -87.6298,
    },
    Hub {
        name: "zrh",
        lat: 47.3769,
        lon: 8.5417,
    },
    Hub {
        name: "syd",
        lat: -33.8688,
        lon: 151.2093,
    },
];

struct ResolvedGeo {
    continent: String,
    country: String,
    region: String,
    city: String,
    lat: f64,
    lon: f64,
}

struct GeoIpResolver {
    reader: Reader<&'static [u8]>,
    source_ip_column: usize,
    input_columns: usize,
    output_relays: Vec<String>,
}

impl Processor for GeoIpResolver {
    fn create(branch: &BranchContext) -> Result<Self, GuestError> {
        let reader = Reader::from_source(DBIP_MMDB)
            .map_err(|error| GuestError::failed(format!("embedded GeoIP database is unusable: {error}")))?;
        let input_schema = branch.input_schema();
        let source_ip_column = input_schema
            .fields
            .iter()
            .position(|field| field.name == SOURCE_IP_FIELD)
            .ok_or_else(|| {
                GuestError::failed(format!(
                    "input schema '{}' has no '{SOURCE_IP_FIELD}' field to resolve",
                    input_schema.name
                ))
            })?;
        let input_columns = input_schema.fields.len();
        if branch.output_schemas().is_empty() {
            return Err(GuestError::failed(
                "geoip enrichment needs at least one destination relay",
            ));
        }
        // Every route reuses the same column layout, so a destination that does not declare the
        // input columns followed by the generated ones is rejected here rather than producing
        // misaligned output later.
        for output_schema in branch.output_schemas() {
            let expected = input_columns + GENERATED_FIELDS.len();
            if output_schema.fields.len() != expected {
                return Err(GuestError::failed(format!(
                    "destination schema '{}' declares {} fields, but geoip enrichment produces \
                     {expected}: {input_columns} input columns followed by {}",
                    output_schema.name,
                    output_schema.fields.len(),
                    GENERATED_FIELDS.join(", ")
                )));
            }
        }

        Ok(Self {
            reader,
            source_ip_column,
            input_columns,
            output_relays: branch
                .output_schemas()
                .iter()
                .map(|schema| schema.name.clone())
                .collect(),
        })
    }

    fn process_batch(
        &mut self,
        ctx: &mut GuestContext<'_>,
        input: InputBatch,
    ) -> Result<(), GuestError> {
        ctx.domain_time();
        let [batch] = input.batches() else {
            return Err(GuestError::failed(format!(
                "geoip enrichment expects exactly one record batch per envelope, got {}",
                input.batches().len()
            )));
        };

        let mut output = OutputEnvelope::new();
        let columns = (0..self.input_columns)
            .map(|column_index| OutputColumnRef::Input {
                column_index: column_index as u32,
            })
            .chain(
                self.enrich(batch)?
                    .into_iter()
                    .map(|array| OutputColumnRef::Generated {
                        column_index: output.add_generated_column(array, false),
                    }),
            )
            .collect::<Vec<_>>();

        // Every destination receives the same rows, but only the first carries the ACK, NACK, and
        // message-error sets so one input row is never settled twice.
        for (index, relay) in self.output_relays.iter().enumerate() {
            let mut acks = input.acks().clone();
            if index > 0 {
                acks.acked.clear();
                acks.nacked.clear();
                acks.message_errors.clear();
            }
            output.add_route(relay.clone(), columns.clone(), acks);
        }
        ctx.emit(output)
    }
}

impl GeoIpResolver {
    fn enrich(&self, batch: &RecordBatch) -> Result<Vec<ArrayRef>, GuestError> {
        let source_ip = batch
            .column(self.source_ip_column)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                GuestError::failed(format!("'{SOURCE_IP_FIELD}' is not a UTF-8 string column"))
            })?;

        let mut database = StringBuilder::new();
        let mut continent = StringBuilder::new();
        let mut country = StringBuilder::new();
        let mut region = StringBuilder::new();
        let mut city = StringBuilder::new();
        let mut latitude = Float64Builder::new();
        let mut longitude = Float64Builder::new();
        let mut geohash = StringBuilder::new();
        let mut hub = StringBuilder::new();
        let mut distance_km = Float64Builder::new();

        for row in 0..batch.num_rows() {
            let address = if source_ip.is_valid(row) {
                source_ip.value(row)
            } else {
                ""
            };
            let geo = self.resolve(address);
            let (nearest, distance) = nearest_hub(geo.lat, geo.lon);
            database.append_value(self.reader.metadata.database_type.as_str());
            continent.append_value(geo.continent.as_str());
            country.append_value(geo.country.as_str());
            region.append_value(geo.region.as_str());
            city.append_value(geo.city.as_str());
            latitude.append_value(geo.lat);
            longitude.append_value(geo.lon);
            geohash.append_value(geo_hash(geo.lat, geo.lon));
            hub.append_value(nearest);
            distance_km.append_value(distance);
        }

        Ok(vec![
            Arc::new(database.finish()),
            Arc::new(continent.finish()),
            Arc::new(country.finish()),
            Arc::new(region.finish()),
            Arc::new(city.finish()),
            Arc::new(latitude.finish()),
            Arc::new(longitude.finish()),
            Arc::new(geohash.finish()),
            Arc::new(hub.finish()),
            Arc::new(distance_km.finish()),
        ])
    }

    /// An address the database cannot place is enriched as unknown rather than failed, because one
    /// unroutable address must not reject the batch around it.
    fn resolve(&self, source_ip: &str) -> ResolvedGeo {
        let Ok(address) = source_ip.parse::<IpAddr>() else {
            return ResolvedGeo::unknown();
        };
        let Ok(result) = self.reader.lookup(address) else {
            return ResolvedGeo::unknown();
        };
        let Ok(Some(city)) = result.decode::<geoip2::City>() else {
            return ResolvedGeo::unknown();
        };
        let (Some(lat), Some(lon)) = (city.location.latitude, city.location.longitude) else {
            return ResolvedGeo::unknown();
        };

        ResolvedGeo {
            continent: city.continent.code.unwrap_or("ZZ").to_string(),
            country: city.country.iso_code.unwrap_or("ZZ").to_string(),
            region: city
                .subdivisions
                .first()
                .and_then(|subdivision| subdivision.names.english)
                .unwrap_or("unknown")
                .to_string(),
            city: city.city.names.english.unwrap_or("unknown").to_string(),
            lat,
            lon,
        }
    }
}

impl ResolvedGeo {
    fn unknown() -> Self {
        Self {
            continent: "ZZ".to_string(),
            country: "ZZ".to_string(),
            region: "unknown".to_string(),
            city: "unknown".to_string(),
            lat: 0.0,
            lon: 0.0,
        }
    }
}

nervix_wasm_sdk::export_processor!(GeoIpResolver);

fn geo_hash(lat: f64, lon: f64) -> String {
    encode(Coord { x: lon, y: lat }, GEOHASH_PRECISION).unwrap_or_default()
}

fn nearest_hub(lat: f64, lon: f64) -> (&'static str, f64) {
    let mut best = HUBS[0];
    let mut best_distance = distance_km(lat, lon, best.lat, best.lon);
    for hub in &HUBS[1..] {
        let distance = distance_km(lat, lon, hub.lat, hub.lon);
        if distance < best_distance {
            best = *hub;
            best_distance = distance;
        }
    }
    (best.name, best_distance)
}

fn distance_km(left_lat: f64, left_lon: f64, right_lat: f64, right_lon: f64) -> f64 {
    Haversine.distance(
        Point::new(left_lon, left_lat),
        Point::new(right_lon, right_lat),
    ) / METERS_PER_KM
}
