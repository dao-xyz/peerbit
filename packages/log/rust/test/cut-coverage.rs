use super::{compare_clock, JoinPlan, LogGraphIndex, LogIndexEntry, ENTRY_TYPE_CUT};
use std::collections::HashMap;

fn entry(hash: &str, next: &[&str], cut: bool, head: bool) -> LogIndexEntry {
    LogIndexEntry::new(
        hash,
        "g",
        next.iter().map(|hash| hash.to_string()).collect(),
        u8::from(cut),
        5,
        1,
        1,
        head,
    )
}

// Frozen pre-change batch algorithm, including its global CUT-head map. Keep
// this independent of the reverse-edge index for correctness and cost controls.
fn exhaustive_plans(
    index: &LogGraphIndex,
    entries: &[&LogIndexEntry],
    reset: bool,
    cut_check: bool,
) -> Vec<JoinPlan> {
    let cut_heads_by_gid = cut_check.then(|| {
        let mut by_gid: HashMap<&str, Vec<&LogIndexEntry>> = HashMap::new();
        for hash in &index.heads {
            if let Some(entry) = index.entries.get(hash) {
                if entry.entry_type == ENTRY_TYPE_CUT {
                    by_gid.entry(entry.gid.as_str()).or_default().push(entry);
                }
            }
        }
        by_gid
    });
    entries
        .iter()
        .map(|entry| {
            if !reset && index.has(&entry.hash) {
                return JoinPlan {
                    skip: true,
                    missing_parents: Vec::new(),
                    cut_checked: cut_check,
                    covered_by_cut: false,
                };
            }
            let covered_by_cut = cut_heads_by_gid
                .as_ref()
                .and_then(|heads| heads.get(entry.gid.as_str()))
                .is_some_and(|heads| {
                    heads.iter().any(|cut| {
                        cut.next.iter().any(|hash| hash == &entry.hash)
                            && compare_clock(entry.wall_time, entry.logical, cut).is_lt()
                    })
                });
            JoinPlan {
                skip: false,
                missing_parents: if entry.entry_type == ENTRY_TYPE_CUT || covered_by_cut {
                    Vec::new()
                } else {
                    entry
                        .next
                        .iter()
                        .filter(|next| reset || !index.has(next))
                        .cloned()
                        .collect()
                },
                cut_checked: cut_check,
                covered_by_cut,
            }
        })
        .collect()
}

fn candidates() -> Vec<LogIndexEntry> {
    let mut candidates = Vec::new();
    for hash in ["old", "other", "absent", "cut", "node-0"] {
        for gid in ["g", "other-gid"] {
            for (wall_time, logical) in [(4, 9), (5, 0), (5, 1), (5, 2), (6, 0)] {
                for cut in [false, true] {
                    let mut candidate = entry(hash, &["old", "absent"], cut, true);
                    candidate.gid = gid.to_string();
                    candidate.wall_time = wall_time;
                    candidate.logical = logical;
                    candidates.push(candidate);
                }
            }
        }
    }
    candidates
}

fn assert_all_planners(index: &LogGraphIndex) {
    let candidates = candidates();
    let refs = candidates.iter().collect::<Vec<_>>();
    let hashes = candidates
        .iter()
        .map(|entry| entry.hash.clone())
        .collect::<Vec<_>>();
    let nexts = candidates
        .iter()
        .map(|entry| entry.next.clone())
        .collect::<Vec<_>>();
    let types = candidates
        .iter()
        .map(|entry| entry.entry_type)
        .collect::<Vec<_>>();
    let gids = candidates
        .iter()
        .map(|entry| entry.gid.clone())
        .collect::<Vec<_>>();
    let wall_times = candidates
        .iter()
        .map(|entry| entry.wall_time)
        .collect::<Vec<_>>();
    let logicals = candidates
        .iter()
        .map(|entry| entry.logical)
        .collect::<Vec<_>>();
    for reset in [false, true] {
        for cut_check in [false, true] {
            let expected = exhaustive_plans(index, &refs, reset, cut_check);
            assert_eq!(
                index.plan_join_entry_refs(&refs, reset, cut_check),
                expected
            );
            assert_eq!(
                index.plan_join_batch(
                    &hashes,
                    &nexts,
                    &types,
                    reset,
                    cut_check.then_some((
                        gids.as_slice(),
                        wall_times.as_slice(),
                        logicals.as_slice()
                    )),
                ),
                expected,
            );
            for (entry, expected) in candidates.iter().zip(expected) {
                assert_eq!(
                    index.plan_join(
                        &entry.hash,
                        &entry.next,
                        entry.entry_type,
                        reset,
                        cut_check.then_some(entry.gid.as_str()),
                        cut_check.then_some(entry.wall_time),
                        cut_check.then_some(entry.logical),
                    ),
                    expected,
                );
            }
        }
    }
}

#[test]
fn matches_exhaustive_cut_predicates_in_all_planners() {
    for cut in [false, true] {
        for head in [false, true] {
            for same_gid in [false, true] {
                for references_candidate in [false, true] {
                    let mut index = LogGraphIndex::new();
                    index.put(entry("old", &[], false, true));
                    let mut candidate = entry(
                        "cut",
                        &[if references_candidate { "old" } else { "other" }],
                        cut,
                        head,
                    );
                    if !same_gid {
                        candidate.gid = "other-gid".to_string();
                    }
                    index.put(candidate);
                    assert_all_planners(&index);
                    // The parent's absence must not lose a surviving CUT edge.
                    index.delete("old");
                    assert_all_planners(&index);
                }
            }
        }
    }
}

#[test]
fn follows_replacement_deletion_batching_and_head_transitions() {
    let mut index = LogGraphIndex::new();
    index.put(entry("cut", &["old", "other"], true, true));
    index.put(entry("new-head", &["cut"], false, true));
    assert_all_planners(&index);
    index.delete("new-head"); // Promotes the old CUT back to a head.
    assert_all_planners(&index);
    index.put(entry("cut", &["absent"], true, false));
    assert_all_planners(&index);
    index.put_join_batch(vec![
        entry("old", &[], false, true),
        entry("cut", &["old"], true, true),
        entry("new-head", &["cut"], false, true),
    ]);
    assert_all_planners(&index);
    index.delete_many(&["new-head".to_string(), "cut".to_string()]);
    assert_all_planners(&index);
    index.clear();
    assert_all_planners(&index);
}

#[test]
fn matches_exhaustive_plans_after_seeded_mutations_and_rebuild() {
    let mut index = LogGraphIndex::new();
    let mut seed = 0x4355_5400u32;
    for step in 0..128 {
        seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        let hash = format!("node-{}", seed % 12);
        if step % 7 == 0 {
            index.delete(&hash);
        } else {
            let mut row = entry(&hash, &["old", "other"], seed & 16 != 0, seed & 32 != 0);
            row.gid = if seed & 64 != 0 { "g" } else { "other-gid" }.to_string();
            row.wall_time = u64::from(seed % 7);
            row.logical = seed % 3;
            if step % 3 == 0 {
                row.next = vec![format!("node-{}", (seed + 1) % 12)];
            }
            if step % 5 == 0 {
                index.put_join_batch(vec![row]);
            } else {
                index.put(row);
            }
        }
        assert_all_planners(&index);
    }
    let rows = index.entries.values().cloned().collect();
    index.clear();
    index.put_join_batch(rows);
    assert_all_planners(&index);
}

#[test]
fn missing_cut_context_preserves_unchecked_behavior() {
    let mut index = LogGraphIndex::new();
    index.put(entry("cut", &["old"], true, true));
    for (gid, wall_time, logical) in [
        (None, Some(1), Some(0)),
        (Some("g"), None, Some(0)),
        (Some("g"), Some(1), None),
    ] {
        assert_eq!(
            index.plan_join(
                "old",
                &["missing".to_string()],
                0,
                false,
                gid,
                wall_time,
                logical
            ),
            JoinPlan {
                skip: false,
                missing_parents: vec!["missing".to_string()],
                cut_checked: false,
                covered_by_cut: false,
            },
        );
    }
}

#[test]
fn bounds_unrelated_head_work_without_repeating_wide_batch_scans() {
    let mut index = LogGraphIndex::new();
    for i in 0..1_000 {
        index.put(entry(
            &format!("cut-{i}"),
            &[&format!("old-{i}")],
            true,
            true,
        ));
    }
    assert!(index.reverse_cut_checks_are_cheaper(["old-0", "unseen"].into_iter(), false));
    assert_all_planners(&index);
    index.clear();
    for i in 0..1_000 {
        index.put(entry(&format!("branch-{i}"), &["old"], false, true));
    }
    assert!(!index.reverse_cut_checks_are_cheaper(["old", "old"].into_iter(), false));
    assert_all_planners(&index);
    // Already-present entries do not need coverage unless explicitly reset.
    index.put(entry("old", &[], false, true));
    assert!(index.reverse_cut_checks_are_cheaper(["old", "old"].into_iter(), false));
    assert!(!index.reverse_cut_checks_are_cheaper(["old", "old"].into_iter(), true));
}

#[test]
#[ignore = "matched microbenchmark; run explicitly with --release --ignored --nocapture"]
fn benchmark_cut_coverage_matched() {
    use std::{hint::black_box, time::Instant};
    const BATCH: usize = 128;
    const ITERATIONS: usize = 10;
    for scenario in [
        "unrelated-cut-same-gid",
        "unrelated-cut-distinct-gids",
        "nonhead-append-fanout",
        "current-head-append-fanout",
    ] {
        for count in [1_000, 10_000, 100_000] {
            let mut index = LogGraphIndex::new();
            for i in 0..count {
                let hash = format!("history-{i}");
                let mut row = if scenario.ends_with("append-fanout") {
                    entry(
                        &hash,
                        &["hot"],
                        false,
                        scenario == "current-head-append-fanout",
                    )
                } else {
                    entry(&hash, &[&format!("old-{i}")], true, true)
                };
                if scenario == "unrelated-cut-distinct-gids" {
                    row.gid = format!("gid-{i}");
                }
                index.put(row);
            }
            // One live unrelated head makes wide non-head adjacency a useful
            // control: a reverse-only scan would visit every historical child.
            index.put(entry("live", &[], false, true));
            let candidates = (0..BATCH)
                .map(|i| {
                    let hash = if scenario.ends_with("append-fanout") {
                        "hot".to_string()
                    } else if i % 2 == 0 {
                        format!("old-{i}")
                    } else {
                        format!("unseen-{i}")
                    };
                    let mut candidate = entry(&hash, &["missing"], false, true);
                    candidate.wall_time = 4;
                    if scenario == "unrelated-cut-distinct-gids" && i % 2 == 0 {
                        candidate.gid = format!("gid-{i}");
                    }
                    candidate
                })
                .collect::<Vec<_>>();
            let refs = candidates.iter().collect::<Vec<_>>();
            let expected = exhaustive_plans(&index, &refs, false, true);
            assert_eq!(index.plan_join_entry_refs(&refs, false, true), expected);
            for run in 0..7 {
                for optimized in if run % 2 == 0 {
                    [false, true]
                } else {
                    [true, false]
                } {
                    let started = Instant::now();
                    for _ in 0..ITERATIONS {
                        let plans = if optimized {
                            index.plan_join_entry_refs(black_box(&refs), false, true)
                        } else {
                            exhaustive_plans(&index, black_box(&refs), false, true)
                        };
                        black_box(plans);
                    }
                    println!(
                        "cut-coverage,{scenario},{count},{BATCH},{ITERATIONS},{run},{},{:.6}",
                        if optimized { "candidate" } else { "baseline" },
                        started.elapsed().as_secs_f64() * 1000.0
                    );
                }
            }
        }
    }
}
