//! Candidate correlation precedence and ambiguity contracts.

use watchdog_domain::{
    ChildSessionId, Confidence, CorrelationBasis, CorrelationCandidate, CorrelationDecision,
    CorrelationPolicy, EvidenceTrust, MainSessionId, SessionId, SessionIdentity, correlate,
};

fn parent(value: u128) -> SessionIdentity {
    SessionIdentity::Main(MainSessionId::from(SessionId::from_uuid(
        uuid::Uuid::from_u128(value),
    )))
}

fn candidate(
    parent: SessionIdentity,
    basis: CorrelationBasis,
    trust: EvidenceTrust,
    confidence: u16,
) -> CorrelationCandidate {
    CorrelationCandidate::new(
        parent,
        basis,
        trust,
        Confidence::new(confidence).expect("fixture confidence should be valid"),
        "bounded fixture evidence",
    )
    .expect("fixture candidate should be valid")
}

#[test]
fn exact_native_relation_beats_a_higher_confidence_heuristic() {
    let exact_parent = parent(1);
    let heuristic_parent = parent(2);
    let decision = correlate(
        &[
            candidate(
                heuristic_parent,
                CorrelationBasis::WeakHeuristic,
                EvidenceTrust::Heuristic,
                9_900,
            ),
            candidate(
                exact_parent,
                CorrelationBasis::ExactNative,
                EvidenceTrust::Authoritative,
                8_000,
            ),
        ],
        CorrelationPolicy::default(),
    );

    let CorrelationDecision::Selected(selection) = decision else {
        panic!("exact native evidence should select one parent");
    };
    assert_eq!(selection.parent(), exact_parent);
    assert_eq!(selection.basis(), CorrelationBasis::ExactNative);
    assert_eq!(selection.rejected().len(), 1);
}

#[test]
fn close_heuristic_candidates_remain_ambiguous() {
    let decision = correlate(
        &[
            candidate(
                parent(1),
                CorrelationBasis::UniqueWorktree,
                EvidenceTrust::Heuristic,
                8_200,
            ),
            candidate(
                parent(2),
                CorrelationBasis::UniqueWorktree,
                EvidenceTrust::Heuristic,
                7_800,
            ),
        ],
        CorrelationPolicy::default(),
    );

    assert!(matches!(decision, CorrelationDecision::Ambiguous { .. }));
}

#[test]
fn mcp_registration_replaces_a_previous_heuristic_selection() {
    let child = SessionIdentity::Child(ChildSessionId::from(SessionId::from_uuid(
        uuid::Uuid::from_u128(7),
    )));
    let first = correlate(
        &[candidate(
            parent(1),
            CorrelationBasis::UniqueWorktree,
            EvidenceTrust::Heuristic,
            8_500,
        )],
        CorrelationPolicy::default(),
    );
    let enriched = correlate(
        &[
            candidate(
                parent(1),
                CorrelationBasis::UniqueWorktree,
                EvidenceTrust::Heuristic,
                8_500,
            ),
            candidate(
                parent(2),
                CorrelationBasis::McpRegistration,
                EvidenceTrust::Authoritative,
                10_000,
            ),
        ],
        CorrelationPolicy::default(),
    );

    assert_eq!(child.kind(), watchdog_domain::SessionKind::Child);
    assert!(matches!(first, CorrelationDecision::Selected(_)));
    let CorrelationDecision::Selected(selection) = enriched else {
        panic!("MCP registration should enrich correlation");
    };
    assert_eq!(selection.parent(), parent(2));
    assert_eq!(selection.basis(), CorrelationBasis::McpRegistration);
}

#[test]
fn conflicting_authoritative_relations_never_guess() {
    let decision = correlate(
        &[
            candidate(
                parent(1),
                CorrelationBasis::ExactNative,
                EvidenceTrust::Authoritative,
                10_000,
            ),
            candidate(
                parent(2),
                CorrelationBasis::McpRegistration,
                EvidenceTrust::Authoritative,
                10_000,
            ),
        ],
        CorrelationPolicy::default(),
    );

    assert!(matches!(decision, CorrelationDecision::Ambiguous { .. }));
}

#[test]
fn conflicting_explicit_relations_never_guess_regardless_of_trust() {
    let decision = correlate(
        &[
            candidate(
                parent(1),
                CorrelationBasis::McpRegistration,
                EvidenceTrust::Corroborating,
                9_000,
            ),
            candidate(
                parent(2),
                CorrelationBasis::McpRegistration,
                EvidenceTrust::Corroborating,
                9_000,
            ),
        ],
        CorrelationPolicy::default(),
    );

    assert!(matches!(decision, CorrelationDecision::Ambiguous { .. }));
}

#[test]
fn supporting_and_rejected_evidence_are_both_retained() {
    let selected_parent = parent(1);
    let decision = correlate(
        &[
            candidate(
                selected_parent,
                CorrelationBasis::ProcessAncestry,
                EvidenceTrust::Corroborating,
                9_000,
            ),
            candidate(
                selected_parent,
                CorrelationBasis::UniqueWorktree,
                EvidenceTrust::Heuristic,
                8_000,
            ),
            candidate(
                parent(2),
                CorrelationBasis::WeakHeuristic,
                EvidenceTrust::Heuristic,
                7_000,
            ),
        ],
        CorrelationPolicy::default(),
    );

    let CorrelationDecision::Selected(selection) = decision else {
        panic!("one strong candidate should be selected");
    };
    assert_eq!(selection.supporting().len(), 1);
    assert_eq!(selection.rejected().len(), 1);
}

#[test]
fn correlation_policy_rejects_a_zero_or_impossible_gap() {
    let confidence = Confidence::new(6_000).expect("confidence should be valid");
    assert!(CorrelationPolicy::new(confidence, 0).is_err());
    assert!(CorrelationPolicy::new(confidence, 10_001).is_err());
}
