use std::{cmp::Ordering, collections::BTreeMap};

use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;

use crate::{BoundedText, Confidence, DomainInputError, EvidenceTrust, SessionIdentity};

/// Strongest evidence class supporting a parent candidate.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CorrelationBasis {
    /// Compatible path or timestamp without a stronger runtime link.
    WeakHeuristic,
    /// Bounded runtime transcript metadata names the parent.
    TranscriptReference,
    /// One startup directory or worktree uniquely matches.
    UniqueWorktree,
    /// Verified host process ancestry links the sessions.
    ProcessAncestry,
    /// A parent explicitly registered the delegation over MCP.
    McpRegistration,
    /// The native runtime supplied the parent/session relation.
    ExactNative,
}

impl CorrelationBasis {
    const fn rank(self) -> u8 {
        match self {
            Self::WeakHeuristic => 1,
            Self::TranscriptReference => 2,
            Self::UniqueWorktree => 3,
            Self::ProcessAncestry => 4,
            Self::McpRegistration => 5,
            Self::ExactNative => 6,
        }
    }

    const fn is_explicit(self) -> bool {
        matches!(self, Self::ExactNative | Self::McpRegistration)
    }
}

/// One bounded, provenance-preserving parent candidate.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CorrelationCandidate {
    parent: SessionIdentity,
    basis: CorrelationBasis,
    trust: EvidenceTrust,
    confidence: Confidence,
    evidence: BoundedText<1_024>,
}

impl CorrelationCandidate {
    /// Construct a candidate with a non-empty bounded evidence summary.
    ///
    /// # Errors
    ///
    /// Returns [`DomainInputError`] for empty or oversized evidence.
    pub fn new(
        parent: SessionIdentity,
        basis: CorrelationBasis,
        trust: EvidenceTrust,
        confidence: Confidence,
        evidence: impl Into<String>,
    ) -> Result<Self, DomainInputError> {
        let evidence = BoundedText::new("correlation_evidence", evidence)?;
        if evidence.is_empty() {
            return Err(DomainInputError::Empty {
                field: "correlation_evidence",
            });
        }
        Ok(Self {
            parent,
            basis,
            trust,
            confidence,
            evidence,
        })
    }

    /// Candidate parent.
    #[must_use]
    pub const fn parent(&self) -> SessionIdentity {
        self.parent
    }

    /// Strongest link represented by this candidate.
    #[must_use]
    pub const fn basis(&self) -> CorrelationBasis {
        self.basis
    }

    /// Source trust used in deterministic ordering.
    #[must_use]
    pub const fn trust(&self) -> EvidenceTrust {
        self.trust
    }

    /// Candidate confidence.
    #[must_use]
    pub const fn confidence(&self) -> Confidence {
        self.confidence
    }

    /// Bounded diagnostic summary retained with the decision.
    #[must_use]
    pub fn evidence(&self) -> &str {
        self.evidence.as_str()
    }
}

/// Thresholds applied to inferred relations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct CorrelationPolicy {
    minimum_confidence: Confidence,
    minimum_gap: u16,
}

impl CorrelationPolicy {
    /// Construct correlation thresholds in basis points.
    ///
    /// # Errors
    ///
    /// Returns [`CorrelationPolicyError`] when the required gap is zero or
    /// exceeds the confidence range.
    pub const fn new(
        minimum_confidence: Confidence,
        minimum_gap: u16,
    ) -> Result<Self, CorrelationPolicyError> {
        if minimum_gap == 0 {
            return Err(CorrelationPolicyError::ZeroGap);
        }
        if minimum_gap > 10_000 {
            return Err(CorrelationPolicyError::GapOutOfRange);
        }
        Ok(Self {
            minimum_confidence,
            minimum_gap,
        })
    }
}

impl<'de> Deserialize<'de> for CorrelationPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawPolicy {
            minimum_confidence: Confidence,
            minimum_gap: u16,
        }

        let raw = RawPolicy::deserialize(deserializer)?;
        Self::new(raw.minimum_confidence, raw.minimum_gap).map_err(de::Error::custom)
    }
}

impl Default for CorrelationPolicy {
    fn default() -> Self {
        Self {
            minimum_confidence: Confidence::new(6_000).unwrap_or_else(|_| unreachable!()),
            minimum_gap: 1_000,
        }
    }
}

/// Invalid inferred-correlation thresholds.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum CorrelationPolicyError {
    /// A zero gap would silently accept tied heuristics.
    #[error("Correlation confidence gap must be positive")]
    ZeroGap,
    /// Confidence gaps use the same basis-point range as confidence.
    #[error("Correlation confidence gap exceeds 10000 basis points")]
    GapOutOfRange,
}

/// Selected parent plus all evidence rejected by the deterministic choice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrelationSelection {
    selected: CorrelationCandidate,
    supporting: Vec<CorrelationCandidate>,
    rejected: Vec<CorrelationCandidate>,
}

impl CorrelationSelection {
    /// Selected parent.
    #[must_use]
    pub const fn parent(&self) -> SessionIdentity {
        self.selected.parent
    }

    /// Basis of the selected candidate.
    #[must_use]
    pub const fn basis(&self) -> CorrelationBasis {
        self.selected.basis
    }

    /// Winning candidate including bounded evidence.
    #[must_use]
    pub const fn selected(&self) -> &CorrelationCandidate {
        &self.selected
    }

    /// Additional evidence supporting the selected parent.
    #[must_use]
    pub fn supporting(&self) -> &[CorrelationCandidate] {
        &self.supporting
    }

    /// Non-selected evidence retained for diagnostics.
    #[must_use]
    pub fn rejected(&self) -> &[CorrelationCandidate] {
        &self.rejected
    }
}

/// Correlation result that never silently resolves close or conflicting evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CorrelationDecision {
    /// One unique candidate passed all gates.
    Selected(CorrelationSelection),
    /// Candidates materially conflict or lack the required gap.
    Ambiguous {
        /// Deterministically ordered candidates retained for diagnostics.
        candidates: Vec<CorrelationCandidate>,
    },
    /// No candidate reached the configured threshold.
    Unassigned {
        /// Rejected evidence retained for diagnostics.
        candidates: Vec<CorrelationCandidate>,
    },
}

/// Select a parent using basis, trust, confidence, then stable parent identity.
#[must_use]
pub fn correlate(
    candidates: &[CorrelationCandidate],
    policy: CorrelationPolicy,
) -> CorrelationDecision {
    let mut all_ranked = candidates.to_vec();
    all_ranked.sort_by(|left, right| compare_candidate(right, left));
    let mut best_by_parent = BTreeMap::<SessionIdentity, CorrelationCandidate>::new();
    for candidate in &all_ranked {
        best_by_parent
            .entry(candidate.parent)
            .and_modify(|existing| {
                if compare_candidate(candidate, existing).is_gt() {
                    *existing = candidate.clone();
                }
            })
            .or_insert_with(|| candidate.clone());
    }
    let mut ranked: Vec<_> = best_by_parent.into_values().collect();
    ranked.sort_by(|left, right| compare_candidate(right, left));
    if ranked.is_empty() {
        return CorrelationDecision::Unassigned {
            candidates: all_ranked,
        };
    }

    let explicit_parents = ranked
        .iter()
        .filter(|candidate| candidate.basis.is_explicit())
        .map(|candidate| candidate.parent)
        .collect::<std::collections::BTreeSet<_>>();
    if explicit_parents.len() > 1 {
        return CorrelationDecision::Ambiguous {
            candidates: all_ranked,
        };
    }

    let selected = ranked[0].clone();
    if !selected.basis.is_explicit()
        && selected.confidence.basis_points() < policy.minimum_confidence.basis_points()
    {
        return CorrelationDecision::Unassigned {
            candidates: all_ranked,
        };
    }
    if !selected.basis.is_explicit()
        && ranked.get(1).is_some_and(|runner_up| {
            selected.basis == runner_up.basis
                && selected
                    .confidence
                    .basis_points()
                    .saturating_sub(runner_up.confidence.basis_points())
                    < policy.minimum_gap
        })
    {
        return CorrelationDecision::Ambiguous {
            candidates: all_ranked,
        };
    }

    let supporting = all_ranked
        .iter()
        .filter(|candidate| candidate.parent == selected.parent && **candidate != selected)
        .cloned()
        .collect();
    let rejected = all_ranked
        .into_iter()
        .filter(|candidate| candidate.parent != selected.parent)
        .collect();
    CorrelationDecision::Selected(CorrelationSelection {
        selected,
        supporting,
        rejected,
    })
}

fn compare_candidate(left: &CorrelationCandidate, right: &CorrelationCandidate) -> Ordering {
    left.basis
        .rank()
        .cmp(&right.basis.rank())
        .then_with(|| trust_rank(left.trust).cmp(&trust_rank(right.trust)))
        .then_with(|| left.confidence.cmp(&right.confidence))
        .then_with(|| right.parent.session_id().cmp(&left.parent.session_id()))
        .then_with(|| left.evidence.as_str().cmp(right.evidence.as_str()))
}

const fn trust_rank(trust: EvidenceTrust) -> u8 {
    match trust {
        EvidenceTrust::Uncertain => 0,
        EvidenceTrust::Heuristic => 1,
        EvidenceTrust::Corroborating => 2,
        EvidenceTrust::Authoritative => 3,
    }
}
