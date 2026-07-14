import assert from "node:assert/strict";
import test from "node:test";
import {
  bearerToken,
  challengeUrl,
  deriveChallengeToken,
  parsePublicDnsTarget,
  validIdempotencyKey,
  validSecret,
} from "./dns-lease.ts";

test("accepts and normalizes public IP addresses", () => {
  assert.equal(parsePublicDnsTarget("1.1.1.1", "A"), "1.1.1.1");
  assert.equal(
    parsePublicDnsTarget("2606:4700:4700:0:0:0:0:1111", "AAAA"),
    "2606:4700:4700::1111",
  );
});

test("derives stable, context-bound server challenge tokens", async () => {
  const secret = "a".repeat(43);
  const first = await deriveChallengeToken(
    secret,
    "f99be573-053c-4615-b0c9-b10aaff870ee",
    "86d724e3-477c-4ac4-a9a8-616c72109737",
  );
  assert.match(first, /^[A-Za-z0-9_-]{43}$/);
  assert.equal(
    first,
    await deriveChallengeToken(
      secret,
      "f99be573-053c-4615-b0c9-b10aaff870ee",
      "86d724e3-477c-4ac4-a9a8-616c72109737",
    ),
  );
  assert.notEqual(
    first,
    await deriveChallengeToken(
      secret,
      "f99be573-053c-4615-b0c9-b10aaff870ee",
      "b8a56fb6-36f9-4fbc-9969-5aa1d7ff2415",
    ),
  );
  await assert.rejects(
    deriveChallengeToken("weak", "lease", "challenge"),
    /DNS_LEASE_CHALLENGE_SECRET/,
  );
});

test("rejects non-public, special, malformed, and mismatched addresses", () => {
  for (
    const address of [
      "127.0.0.1",
      "10.0.0.1",
      "100.64.1.2",
      "169.254.169.254",
      "192.0.2.1",
      "224.0.0.1",
    ]
  ) {
    assert.throws(
      () => parsePublicDnsTarget(address, "A"),
      /publicly routable/,
    );
  }
  for (
    const address of [
      "::1",
      "fe80::1",
      "fd00::1",
      "2001:1::1",
      "2001:db8::1",
      "2002::1",
      "3fff::1",
    ]
  ) {
    assert.throws(
      () => parsePublicDnsTarget(address, "AAAA"),
      /publicly routable/,
    );
  }
  assert.throws(() => parsePublicDnsTarget("1.1.1.1:80", "A"), /valid IPv4/);
  assert.throws(() => parsePublicDnsTarget("1.1.1.1", "AAAA"), /valid IPv6/);
});

test("constructs direct port-80 challenge URLs", () => {
  assert.equal(
    challengeUrl("1.1.1.1", "f99be573-053c-4615-b0c9-b10aaff870ee"),
    "http://1.1.1.1/.well-known/peerbit-dns/f99be573-053c-4615-b0c9-b10aaff870ee",
  );
  assert.equal(
    challengeUrl(
      "2606:4700:4700::1111",
      "f99be573-053c-4615-b0c9-b10aaff870ee",
    ),
    "http://[2606:4700:4700::1111]/.well-known/peerbit-dns/f99be573-053c-4615-b0c9-b10aaff870ee",
  );
});

test("parses bounded bearer tokens and idempotency keys", () => {
  const token = "x".repeat(32);
  assert.equal(
    bearerToken(
      new Request("https://example.test", {
        headers: { Authorization: `Bearer ${token}` },
      }),
    ),
    token,
  );
  assert.equal(bearerToken(new Request("https://example.test")), undefined);
  assert.equal(validIdempotencyKey("claim-0001"), true);
  assert.equal(validIdempotencyKey("bad key"), false);
  assert.equal(validSecret("a".repeat(43)), true);
  assert.equal(validSecret("short"), false);
});
