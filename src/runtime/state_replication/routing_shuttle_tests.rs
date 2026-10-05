//! Layer: test harness.
//! Owns: production frame routing during assignment replacement and state retirement.
//! May depend on: the production routing owner and the Shuttle model harness.
//! Must not know: transport framing, persisted storage or control-plane transactions.

use nervix_model_harness::shuttle::check_interleavings;

use super::{
    tests::{assigned, install, placed},
    *,
};

fn replacement_and_retirement_race_frames() {
    shuttle::future::block_on(async {
        let routes = Arc::new(StateReplicationRouting::default());
        let slot = Arc::new(ArcSwapOption::from(Some(assigned(1))));
        let first = placed(0, 0, 1);
        let second = placed(0, 0, 2);
        routes.register_assignment(&first.entity(), &slot);
        let first_state = install(&routes, &slot, &first);
        let retained = routes
            .resolve(&first)
            .assured("the first state publishes its route");
        let peer = named::<ClusterNodeName>("node-2");
        let frame = nervix_primitives::task::spawn({
            let routes = routes.clone();
            let first = first.clone();
            let peer = peer.clone();
            async move {
                routes.acknowledge(&first, &peer, 1);
            }
        });
        slot.store(Some(assigned(2)));
        let second_state = install(&routes, &slot, &second);
        routes.retire(&first);
        frame
            .await
            .assured("a model task panic fails the entire Shuttle execution");
        assert!(!retained.is_current());
        assert!(retained.state().is_none());
        assert_eq!(
            second_state.replication().with_progress(|p| p.held(&peer)),
            None,
            "an in-flight frame for the first identity never changes its successor"
        );
        let first_held = first_state.replication().with_progress(|p| p.held(&peer));
        assert!(matches!(first_held, None | Some(1)));
        routes.acknowledge(&second, &peer, 2);
        assert_eq!(
            second_state.replication().with_progress(|p| p.held(&peer)),
            Some(2)
        );
        let ending_frame = nervix_primitives::task::spawn({
            let routes = routes.clone();
            let second = second.clone();
            let peer = peer.clone();
            async move {
                routes.acknowledge(&second, &peer, 3);
            }
        });
        routes.retire_entity(&second.entity());
        ending_frame
            .await
            .assured("a model task panic fails the entire Shuttle execution");
        routes.acknowledge(&second, &peer, 4);
        let final_held = second_state.replication().with_progress(|p| p.held(&peer));
        assert!(
            matches!(final_held, Some(2 | 3)),
            "ending prevents later frame admission"
        );
    });
}

#[test]
fn shuttle_state_replacement_and_retirement_fence_frames_in_flight() {
    check_interleavings(replacement_and_retirement_race_frames);
}
