//! Layer: test harness.
//! - **Owns.** Event observations and delivery outcomes through the production Redis emitter.
//! - **Depends on.** The connector contract, counting RESP peer and retained host double.
//! - **Must not know.** The server's metrics, graph, retries or acknowledgement maps.

use meticulous::{OptionExt as _, ResultExt as _};
use nervix_connector::{
    EmitterConnector as _, EmitterRecord, EmitterRecordId, UndeclaredConnectorEvent,
};
use nervix_connector_fake::FakeEmitterHost;
use nervix_models::{ConnectorEvent, Timestamp};

use super::{counting_redis::CountingRedis, test_fixture::Fixture, *};

struct Pool(RedisCommandPool);

impl RedisPoolServices for Pool {
    fn pool(&self) -> RedisCommandPool {
        self.0.clone()
    }
    fn begin_pool_wait(&self) -> RedisPoolWait {
        RedisPoolWait::new(())
    }
}

async fn config(fixture: &Fixture, redis: &CountingRedis) -> RedisPubSubEmitterConfig {
    let pool = open_redis_command_pool(
        &[ClientConfigEntry {
            key: "addr".into(),
            value: redis.url(),
        }],
        ClientPoolBounds::new(1, std::num::NonZeroU32::MIN)
            .assured("both fixture pool bounds are one"),
        fixture.dns.clone(),
    )
    .await
    .assured("the fixture peer accepts the configured local command connection");
    RedisPubSubEmitterConfig {
        pool: RedisPoolHandle::new(Pool(pool)),
        channel: RedisChannelName::parse("declared_events")
            .assured("the fixed fixture channel name is valid"),
    }
}

fn record(index: usize) -> EmitterRecord {
    EmitterRecord::new(
        EmitterRecordId::new(index),
        None,
        b"event".to_vec(),
        Vec::new(),
        Timestamp::from_unix_nanos(0),
    )
}

#[nervix_primitives::test]
async fn successful_publish_counts_only_zero_receivers_and_keeps_every_record_delivered() {
    let fixture = Fixture::start().await;
    for (response, expected) in [(b":0\r\n".as_slice(), 2), (b":1\r\n".as_slice(), 0)] {
        let redis = CountingRedis::start_with_publish_response(response).await;
        let host = FakeEmitterHost::declaring(RedisPubSubEmitter::KIND.capabilities().events);
        let mut emitter = RedisPubSubEmitter::open(config(&fixture, &redis).await, host.host())
            .await
            .assured("the fixture host declares the event this emitter resolves");
        assert_eq!(
            host.observations().events[&ConnectorEvent::PublishedWithoutSubscribers],
            0
        );
        let result = emitter
            .publish(vec![record(0), record(1)])
            .await
            .into_parts();
        assert_eq!(
            result.delivered,
            vec![EmitterRecordId::new(0), EmitterRecordId::new(1)]
        );
        assert!(result.rejected.is_empty());
        assert!(result.infrastructure_error.is_none());
        assert_eq!(
            host.observations().events[&ConnectorEvent::PublishedWithoutSubscribers],
            expected
        );
    }
}

#[nervix_primitives::test]
async fn a_failed_publish_contributes_no_successful_zero_receiver_event() {
    let fixture = Fixture::start().await;
    let redis = CountingRedis::start_with_publish_response(b"-ERR unavailable\r\n").await;
    let host = FakeEmitterHost::declaring(RedisPubSubEmitter::KIND.capabilities().events);
    let mut emitter = RedisPubSubEmitter::open(config(&fixture, &redis).await, host.host())
        .await
        .assured("the fixture host declares the event this emitter resolves");
    let result = emitter.publish(vec![record(0)]).await.into_parts();
    assert!(result.delivered.is_empty());
    assert!(result.rejected.is_empty());
    assert!(result.infrastructure_error.is_some());
    assert_eq!(
        host.observations().events[&ConnectorEvent::PublishedWithoutSubscribers],
        0
    );
}

#[nervix_primitives::test]
async fn a_host_refuses_an_undeclared_event_during_emitter_construction() {
    let fixture = Fixture::start().await;
    let redis = CountingRedis::start().await;
    let host = FakeEmitterHost::default();
    let Err(error) = RedisPubSubEmitter::open(config(&fixture, &redis).await, host.host()).await
    else {
        panic!("a host declaring no events cannot construct this emitter");
    };
    let cause = error
        .frames()
        .find_map(|frame| frame.downcast_ref::<UndeclaredConnectorEvent>())
        .assured("the refused construction retains its typed event cause");
    assert_eq!(cause.event, ConnectorEvent::PublishedWithoutSubscribers);
    assert!(host.observations().events.is_empty());
}
