use super::*;

#[test]
fn algorithm_kind_round_trips_through_its_name() {
    let kinds = [
        AlgorithmKind::RoundRobin,
        AlgorithmKind::WeightedRoundRobin,
        AlgorithmKind::LeastConnections,
        AlgorithmKind::LeastResponseTime,
        AlgorithmKind::AdaptiveBalancing,
    ];

    for kind in kinds {
        assert_eq!(AlgorithmKind::from_str_name(kind.as_str()), Some(kind));
    }
    assert_eq!(AlgorithmKind::from_str_name("unknown"), None);
}
