//! Relay/ack decision helpers: ports of `stream/src/core/seek-routing.ts`
//! and the inline decision logic of `DirectStream.shouldIgnore`,
//! `DirectStream.onAck` (trace back-routing) and the `publishMessage`
//! flood/redundancy target selection. Pure functions over peer-hash strings;
//! the TS adapter extracts the inputs from the (already decoded) message and
//! executes the returned selections.

/// `DirectStream.shouldIgnore`.
pub fn should_ignore_data(
    seen_before: u32,
    acknowledged_mode: bool,
    redundancy: u8,
    hops: &[String],
    me: &str,
    signed_by_self: bool,
) -> bool {
    if acknowledged_mode {
        if hops.iter().any(|hop| hop == me) {
            return true;
        }
        return seen_before >= redundancy as u32;
    }
    if signed_by_self {
        return true;
    }
    seen_before > 0
}

/// `shouldAcknowledgeDataMessage` from core/seek-routing.ts.
pub fn should_acknowledge(is_recipient: bool, seen_before: u32, redundancy: u8) -> bool {
    is_recipient && seen_before < redundancy as u32
}

/// The ACK back-routing decision from `DirectStream.onAck`: our position in
/// the delivery trace and the previous hop (if any) to relay the ACK to.
pub fn ack_next_hop<'a>(trace: &'a [String], me: &str) -> (i64, Option<&'a str>) {
    let Some(my_index) = trace.iter().position(|hash| hash == me) else {
        return (-1, None);
    };
    let next = if my_index > 0 {
        Some(trace[my_index - 1].as_str())
    } else {
        None
    };
    (my_index as i64, next)
}

/// `computeSeekAckRouteUpdate` from core/seek-routing.ts: which edge to
/// learn from an observed ACK.
pub fn seek_ack_route_update<'a>(
    current: &'a str,
    upstream: Option<&'a str>,
    downstream: &'a str,
) -> (&'a str, &'a str) {
    (upstream.unwrap_or(current), downstream)
}

/// The flood-path receiver filter in `DirectStream.publishMessage`: keep the
/// candidate neighbours the frame should be forwarded to. Skips the inbound
/// peer, everyone who already signed the message and every hop already in
/// the delivery path (`selectSeekRelayTargets` semantics). Returns indices
/// into `candidates`.
pub fn filter_flood_targets(
    candidates: &[String],
    from: &str,
    signed: &[String],
    hops: &[String],
) -> Vec<u32> {
    let mut out = Vec::new();
    for (index, candidate) in candidates.iter().enumerate() {
        if candidate == from {
            continue;
        }
        if signed.iter().any(|hash| hash == candidate) || hops.iter().any(|hash| hash == candidate)
        {
            continue;
        }
        out.push(index as u32);
    }
    out
}

/// The relayed-SilentDelivery direct path in `publishMessage`: recipients we
/// should hand the message to over a direct stream. `connected` is the
/// neighbour set in insertion order.
pub fn filter_silent_relay_recipients(
    recipients: &[String],
    me: &str,
    from: &str,
    connected: &[String],
    hops: &[String],
) -> Vec<String> {
    let mut out = Vec::new();
    for recipient in recipients {
        if recipient == me {
            continue;
        }
        if recipient == from {
            continue; // never send back to previous hop
        }
        if !connected.iter().any(|hash| hash == recipient) {
            continue;
        }
        if hops.iter().any(|hash| hash == recipient) {
            continue; // recipient already signed/seen this message
        }
        out.push(recipient.clone());
    }
    out
}

/// The redundancy probing loop in `publishMessage`: pick additional
/// neighbours (in `peers` order) until the used set reaches `redundancy`.
pub fn select_redundancy_probes(peers: &[String], used: &[String], redundancy: u8) -> Vec<String> {
    let mut used_count = used.len();
    let mut out = Vec::new();
    for neighbour in peers {
        if used_count >= redundancy as usize {
            break;
        }
        if used.iter().any(|hash| hash == neighbour) || out.contains(neighbour) {
            continue;
        }
        used_count += 1;
        out.push(neighbour.clone());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn ignore_rules() {
        // acknowledged: my hop present → ignore
        assert!(should_ignore_data(
            0,
            true,
            2,
            &strings(&["me"]),
            "me",
            false
        ));
        // acknowledged: below redundancy → process
        assert!(!should_ignore_data(
            1,
            true,
            2,
            &strings(&["a"]),
            "me",
            false
        ));
        assert!(should_ignore_data(
            2,
            true,
            2,
            &strings(&["a"]),
            "me",
            false
        ));
        // non-acknowledged: signed by self → ignore
        assert!(should_ignore_data(0, false, 1, &[], "me", true));
        // non-acknowledged: only first sighting processes
        assert!(!should_ignore_data(0, false, 1, &[], "me", false));
        assert!(should_ignore_data(1, false, 1, &[], "me", false));
    }

    #[test]
    fn acknowledge_rules() {
        assert!(should_acknowledge(true, 0, 1));
        assert!(!should_acknowledge(true, 1, 1));
        assert!(!should_acknowledge(false, 0, 1));
        assert!(should_acknowledge(true, 1, 2));
    }

    #[test]
    fn ack_trace_back_routing() {
        let trace = strings(&["origin", "relay", "target"]);
        assert_eq!(ack_next_hop(&trace, "relay"), (1, Some("origin")));
        assert_eq!(ack_next_hop(&trace, "origin"), (0, None));
        assert_eq!(ack_next_hop(&trace, "unknown"), (-1, None));
    }

    #[test]
    fn seek_route_update_prefers_upstream() {
        assert_eq!(
            seek_ack_route_update("me", Some("up"), "down"),
            ("up", "down")
        );
        assert_eq!(seek_ack_route_update("me", None, "down"), ("me", "down"));
    }

    #[test]
    fn flood_filter() {
        let candidates = strings(&["a", "b", "c", "d"]);
        let kept = filter_flood_targets(&candidates, "a", &strings(&["b"]), &strings(&["c"]));
        assert_eq!(kept, vec![3]);
    }

    #[test]
    fn silent_relay_recipients() {
        let recipients = strings(&["me", "from", "offline", "hop", "ok"]);
        let out = filter_silent_relay_recipients(
            &recipients,
            "me",
            "from",
            &strings(&["hop", "ok"]),
            &strings(&["hop"]),
        );
        assert_eq!(out, strings(&["ok"]));
    }

    #[test]
    fn redundancy_probes() {
        let peers = strings(&["a", "b", "c"]);
        assert_eq!(
            select_redundancy_probes(&peers, &strings(&["a"]), 2),
            strings(&["b"])
        );
        assert_eq!(
            select_redundancy_probes(&peers, &strings(&["a", "b"]), 2),
            Vec::<String>::new()
        );
        assert_eq!(
            select_redundancy_probes(&peers, &[], 3),
            strings(&["a", "b", "c"])
        );
    }
}
