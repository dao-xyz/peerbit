import assert from "node:assert/strict";
import test from "node:test";
import type { SupabaseClient } from "@supabase/supabase-js";
import { recoverConsumedRenewalAvailability } from "./dns-lease-renewal.ts";

type LeaseState = Record<string, unknown> & {
  id: string;
  updated_at: string;
  verify_available_at: string;
};

type Filter = {
  column: string;
  operator: "eq" | "gt" | "is";
  value: unknown;
};

function inMemoryClient(row: LeaseState) {
  let version = 0;
  const client = {
    from(table: string) {
      assert.equal(table, "dns_leases");
      const filters: Filter[] = [];
      let changes: Record<string, unknown> = {};
      const query = {
        update(next: Record<string, unknown>) {
          changes = next;
          return query;
        },
        eq(column: string, value: unknown) {
          filters.push({ column, operator: "eq", value });
          return query;
        },
        is(column: string, value: unknown) {
          filters.push({ column, operator: "is", value });
          return query;
        },
        gt(column: string, value: unknown) {
          filters.push({ column, operator: "gt", value });
          return query;
        },
        select() {
          return query;
        },
        async maybeSingle() {
          // Let concurrent recovery attempts both build their stale predicates
          // before either compare-and-set is evaluated.
          await Promise.resolve();
          const matches = filters.every((filter) => {
            const current = row[filter.column];
            if (filter.operator === "gt") {
              return Date.parse(String(current)) >
                Date.parse(String(filter.value));
            }
            return current === filter.value;
          });
          if (!matches) return { data: null, error: null };
          Object.assign(row, changes);
          row.updated_at = `recovered-${++version}`;
          return { data: { id: row.id }, error: null };
        },
      };
      return query;
    },
  };
  return client as unknown as SupabaseClient;
}

function reservedRow(): LeaseState {
  return {
    id: "5be593e3-ddd1-471f-8711-6b32fcdccb39",
    status: "active",
    updated_at: "2026-07-13T12:00:00.000Z",
    challenge_id: null,
    challenge_token_hash: null,
    challenge_expires_at: null,
    lease_expires_at: "2026-07-14T12:00:00.000Z",
    verify_available_at: "2026-07-13T12:01:00.000Z",
  };
}

test("reopens challenge issuance with an exact post-consumption CAS", async () => {
  const row = reservedRow();
  const result = await recoverConsumedRenewalAvailability(
    inMemoryClient(row),
    { id: row.id, reservedUpdatedAt: row.updated_at },
    new Date("2026-07-13T12:00:05.000Z"),
  );

  assert.deepEqual(result, { recovered: true });
  assert.equal(row.verify_available_at, "2026-07-13T12:00:05.000Z");
});

test("allows only one concurrent recovery of the consumed renewal", async () => {
  const row = reservedRow();
  const client = inMemoryClient(row);
  const reservation = { id: row.id, reservedUpdatedAt: row.updated_at };
  const results = await Promise.all([
    recoverConsumedRenewalAvailability(
      client,
      reservation,
      new Date("2026-07-13T12:00:05.000Z"),
    ),
    recoverConsumedRenewalAvailability(
      client,
      reservation,
      new Date("2026-07-13T12:00:06.000Z"),
    ),
  ]);

  assert.equal(results.filter((result) => result.recovered).length, 1);
  assert.equal(results.filter((result) => !result.recovered).length, 1);
});

test("does not overwrite a fresh challenge or a state transition", async () => {
  for (
    const changed of [
      { challenge_id: "f0389335-9540-49a3-8655-f3798373168b" },
      { status: "release_pending" },
      { updated_at: "2026-07-13T12:00:01.000Z" },
    ]
  ) {
    const row = Object.assign(reservedRow(), changed);
    const previousAvailability = row.verify_available_at;
    const result = await recoverConsumedRenewalAvailability(
      inMemoryClient(row),
      {
        id: row.id,
        reservedUpdatedAt: "2026-07-13T12:00:00.000Z",
      },
      new Date("2026-07-13T12:00:05.000Z"),
    );

    assert.deepEqual(result, { recovered: false });
    assert.equal(row.verify_available_at, previousAvailability);
  }
});

test("reports storage failures without masking the renewal error", async () => {
  const client = {
    from() {
      throw new Error("database unavailable");
    },
  } as unknown as SupabaseClient;
  const result = await recoverConsumedRenewalAvailability(
    client,
    {
      id: "5be593e3-ddd1-471f-8711-6b32fcdccb39",
      reservedUpdatedAt: "2026-07-13T12:00:00.000Z",
    },
  );

  assert.deepEqual(result, {
    recovered: false,
    error: "database unavailable",
  });
});
