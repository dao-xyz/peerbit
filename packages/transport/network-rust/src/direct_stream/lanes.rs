//! Outbound lane scheduler: the queue/fairness core of
//! `packages/transport/stream/src/pushable-lanes.ts`.
//!
//! Scheduling identical to the TS `LaneQueue` in 'wrr' mode: per-lane FIFOs,
//! a repeating weighted-round-robin schedule built from `bias^(L-1-i)`
//! weights (lanes=4, bias=2 → [8,4,2,1]), a rotating cursor that probes up
//! to `schedule.len()` slots per shift, and a linear-scan fallback. The byte
//! high-water mark (`maxBufferedBytes`, overflow policy 'throw') is enforced
//! here; the host maps [`PushOutcome::Overflow`] onto the same Error the TS
//! pushable throws.
//!
//! Values never cross into this core: the host keeps the actual byte chunks
//! and enqueues `(sequence, byte_length, lane)` records. `shift` returns the
//! sequence to emit next, so all ordering/accounting/backpressure decisions
//! are made here while the JS side stays a byte pump.

use std::collections::VecDeque;

pub const DEFAULT_BIAS: u32 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushOutcome {
    Pushed(u64),
    /// Would exceed `max_buffered_bytes`; `would_be` mirrors the byte count
    /// in the TS overflow error message.
    Overflow {
        would_be: u64,
    },
}

pub struct LaneScheduler {
    lanes: Vec<VecDeque<(u64, u64)>>,
    lane_bytes: Vec<u64>,
    total_bytes: u64,
    schedule: Vec<usize>,
    cursor: usize,
    max_buffered_bytes: Option<u64>,
    next_sequence: u64,
}

fn clamp_lane(lane: usize, lanes: usize) -> usize {
    lane.min(lanes - 1)
}

impl LaneScheduler {
    pub fn new(lanes: usize, max_buffered_bytes: Option<u64>, bias: Option<u32>) -> Self {
        let lane_count = lanes.max(1);
        let bias = bias.unwrap_or(DEFAULT_BIAS);
        let mut schedule = Vec::new();
        for lane in 0..lane_count {
            let weight = (bias as u64).pow((lane_count - 1 - lane) as u32).max(1);
            for _ in 0..weight {
                schedule.push(lane);
            }
        }
        if schedule.is_empty() {
            schedule = (0..lane_count).collect();
        }
        LaneScheduler {
            lanes: (0..lane_count).map(|_| VecDeque::new()).collect(),
            lane_bytes: vec![0; lane_count],
            total_bytes: 0,
            schedule,
            cursor: 0,
            max_buffered_bytes,
            next_sequence: 0,
        }
    }

    pub fn push(&mut self, lane: usize, byte_length: u64) -> PushOutcome {
        if let Some(max) = self.max_buffered_bytes {
            if max > 0 {
                let would_be = self.total_bytes + byte_length;
                if would_be > max {
                    return PushOutcome::Overflow { would_be };
                }
            }
        }
        let lane = clamp_lane(lane, self.lanes.len());
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.lanes[lane].push_back((sequence, byte_length));
        self.lane_bytes[lane] += byte_length;
        self.total_bytes += byte_length;
        PushOutcome::Pushed(sequence)
    }

    pub fn shift(&mut self) -> Option<u64> {
        if self.is_empty() {
            return None;
        }
        let slots = self.schedule.len();
        for _ in 0..slots {
            let lane = self.schedule[self.cursor];
            self.cursor = (self.cursor + 1) % slots;
            if let Some((sequence, bytes)) = self.lanes[lane].pop_front() {
                self.lane_bytes[lane] -= bytes;
                self.total_bytes -= bytes;
                return Some(sequence);
            }
        }
        // (very unlikely) nothing was found despite size>0 – linear scan fallback
        for lane in 0..self.lanes.len() {
            if let Some((sequence, bytes)) = self.lanes[lane].pop_front() {
                self.lane_bytes[lane] -= bytes;
                self.total_bytes -= bytes;
                return Some(sequence);
            }
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        self.lanes.iter().all(|lane| lane.is_empty())
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn lane_bytes(&self, lane: usize) -> u64 {
        self.lane_bytes[clamp_lane(lane, self.lanes.len())]
    }

    pub fn max_buffered_bytes(&self) -> Option<u64> {
        self.max_buffered_bytes
    }

    pub fn clear(&mut self) {
        for lane in &mut self.lanes {
            lane.clear();
        }
        self.lane_bytes.iter_mut().for_each(|bytes| *bytes = 0);
        self.total_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn four_lane_schedule_is_8_4_2_1() {
        let scheduler = LaneScheduler::new(4, None, None);
        let counts: Vec<usize> = (0..4)
            .map(|lane| scheduler.schedule.iter().filter(|l| **l == lane).count())
            .collect();
        assert_eq!(counts, vec![8, 4, 2, 1]);
    }

    #[test]
    fn wrr_interleaves_lanes_without_starvation() {
        let mut scheduler = LaneScheduler::new(2, None, None);
        // schedule [0,0,1]: with both lanes saturated, lane 0 gets 2 of 3 slots
        let mut lane_of_seq = std::collections::HashMap::new();
        for i in 0..6 {
            if let PushOutcome::Pushed(seq) = scheduler.push(0, 1) {
                lane_of_seq.insert(seq, 0);
            }
            if let PushOutcome::Pushed(seq) = scheduler.push(1, 1) {
                lane_of_seq.insert(seq, 1);
            }
            let _ = i;
        }
        let mut order = Vec::new();
        while let Some(seq) = scheduler.shift() {
            order.push(lane_of_seq[&seq]);
        }
        assert_eq!(order.len(), 12);
        assert_eq!(&order[..6], &[0, 0, 1, 0, 0, 1]);
    }

    #[test]
    fn single_lane_drains_in_fifo_order() {
        let mut scheduler = LaneScheduler::new(4, None, None);
        let seqs: Vec<u64> = (0..5)
            .map(|_| match scheduler.push(2, 10) {
                PushOutcome::Pushed(seq) => seq,
                PushOutcome::Overflow { .. } => panic!("unexpected overflow"),
            })
            .collect();
        let mut drained = Vec::new();
        while let Some(seq) = scheduler.shift() {
            drained.push(seq);
        }
        assert_eq!(drained, seqs);
    }

    #[test]
    fn byte_budget_overflow() {
        let mut scheduler = LaneScheduler::new(4, Some(10), None);
        assert!(matches!(scheduler.push(0, 6), PushOutcome::Pushed(_)));
        assert_eq!(scheduler.push(1, 5), PushOutcome::Overflow { would_be: 11 });
        assert!(matches!(scheduler.push(1, 4), PushOutcome::Pushed(_)));
        assert_eq!(scheduler.total_bytes(), 10);
        scheduler.shift();
        assert_eq!(scheduler.total_bytes(), 4);
        assert!(matches!(scheduler.push(0, 6), PushOutcome::Pushed(_)));
    }

    #[test]
    fn lane_byte_accounting() {
        let mut scheduler = LaneScheduler::new(4, None, None);
        scheduler.push(0, 3);
        scheduler.push(3, 7);
        assert_eq!(scheduler.lane_bytes(0), 3);
        assert_eq!(scheduler.lane_bytes(3), 7);
        assert_eq!(scheduler.total_bytes(), 10);
        // out-of-range lanes clamp to the last lane (same as the TS clampLane)
        scheduler.push(9, 1);
        assert_eq!(scheduler.lane_bytes(3), 8);
    }
}
