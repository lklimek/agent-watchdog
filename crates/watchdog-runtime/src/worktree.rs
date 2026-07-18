use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use watchdog_domain::ChildSessionId;

/// Ownership index for attributing worktree filesystem activity.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct WorktreeOwners {
    owners: BTreeMap<PathBuf, BTreeSet<ChildSessionId>>,
}

impl std::fmt::Debug for WorktreeOwners {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorktreeOwners")
            .field("worktree_count", &self.owners.len())
            .finish()
    }
}

impl WorktreeOwners {
    /// Construct an empty ownership index.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            owners: BTreeMap::new(),
        }
    }

    /// Register one child as an owner of a validated worktree path.
    pub fn register(&mut self, worktree: impl Into<PathBuf>, child: ChildSessionId) {
        self.owners
            .entry(worktree.into())
            .or_default()
            .insert(child);
    }

    /// Attribute a change only when ownership or process evidence is unambiguous.
    #[must_use]
    pub fn attribute(
        &self,
        changed_path: &Path,
        process_evidence: Option<ChildSessionId>,
    ) -> Attribution {
        let Some((_, owners)) = self
            .owners
            .iter()
            .filter(|(worktree, _)| changed_path.starts_with(worktree))
            .max_by_key(|(worktree, _)| worktree.components().count())
        else {
            return Attribution::Unowned;
        };
        if owners.len() == 1 {
            let Some(child) = owners.first().copied() else {
                return Attribution::Unowned;
            };
            return Attribution::Child(child);
        }
        if let Some(child) = process_evidence.filter(|child| owners.contains(child)) {
            return Attribution::Child(child);
        }
        Attribution::Ambiguous
    }
}

/// Result of correlating a worktree change to child ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Attribution {
    /// Exactly one child receives activity.
    Child(ChildSessionId),
    /// Multiple owners exist and process evidence is insufficient.
    Ambiguous,
    /// No child owns the worktree.
    Unowned,
}
