import assert from "node:assert/strict";
import test from "node:test";
import {
  createDnsRecord,
  deleteDnsRecord,
  findDnsRecords,
  getDnsRecord,
  verifyDnsZone,
} from "./cloudflare-dns.ts";

const zoneId = "a".repeat(32);
const recordId = "b".repeat(32);
const differentRecordId = "c".repeat(32);
const name = "p-0123456789abcdefabcd.nodes.peerchecker.com";
const config = {
  brokerUrl: "https://dns-broker.example.test",
  brokerSecret: "s".repeat(43),
  ttl: 300,
  requestTimeoutMs: 1_000,
};
const record = {
  id: recordId,
  name,
  type: "A",
  content: "1.1.1.1",
  comment: "Peerbit managed lease 5be593e3-ddd1-471f-8711-6b32fcdccb39",
  proxied: false,
};

test("authenticates to the broker and verifies its zone identity", async () => {
  let request: Request | undefined;
  const result = await verifyDnsZone(config, async (input, init) => {
    request = new Request(input, init);
    return Response.json({ zoneId });
  });
  assert.equal(result, zoneId);
  assert.equal(request?.url, "https://dns-broker.example.test/zone");
  assert.equal(request?.method, "POST");
  assert.equal(
    request?.headers.get("authorization"),
    "Bearer " + config.brokerSecret,
  );
  assert.deepEqual(await request?.json(), {});
});

test("creates an exact DNS-only record through the narrow broker", async () => {
  let request: Request | undefined;
  const result = await createDnsRecord(
    config,
    {
      id: "5be593e3-ddd1-471f-8711-6b32fcdccb39",
      domain: name,
      recordType: "A",
      address: "1.1.1.1",
    },
    async (input, init) => {
      request = new Request(input, init);
      return Response.json({ zoneId, record });
    },
  );
  assert.equal(result.zoneId, zoneId);
  assert.equal(result.record.id, recordId);
  assert.equal(request?.url, "https://dns-broker.example.test/records/create");
  assert.deepEqual(await request?.json(), {
    leaseId: "5be593e3-ddd1-471f-8711-6b32fcdccb39",
    name,
    type: "A",
    address: "1.1.1.1",
    ttl: 300,
  });
});

test("uses bounded POST contracts for list, get, and exact delete", async () => {
  const requests: Request[] = [];
  const responses = [
    { zoneId, records: [record] },
    { zoneId, record },
    { zoneId, deletedId: recordId },
  ];
  const fetchFn = async (input: string | URL | Request, init?: RequestInit) => {
    requests.push(new Request(input, init));
    return Response.json(responses.shift());
  };

  const listed = await findDnsRecords(config, name, "A", fetchFn);
  const fetched = await getDnsRecord(config, recordId, fetchFn);
  const deletedZone = await deleteDnsRecord(
    config,
    {
      id: recordId,
      leaseId: "5be593e3-ddd1-471f-8711-6b32fcdccb39",
      name,
      type: "A",
      address: "1.1.1.1",
    },
    fetchFn,
  );

  assert.equal(listed.records[0].id, recordId);
  assert.equal(fetched.record?.id, recordId);
  assert.equal(deletedZone, zoneId);
  assert.deepEqual(
    requests.map((request) => [request.method, new URL(request.url).pathname]),
    [
      ["POST", "/records/list"],
      ["POST", "/records/get"],
      ["POST", "/records/delete"],
    ],
  );
  assert.deepEqual(await requests[0].json(), { name, type: "A" });
  assert.deepEqual(await requests[1].json(), { recordId });
  assert.deepEqual(await requests[2].json(), {
    recordId,
    leaseId: "5be593e3-ddd1-471f-8711-6b32fcdccb39",
    name,
    type: "A",
    address: "1.1.1.1",
  });
});

test("represents an absent immutable record without hiding its zone", async () => {
  const result = await getDnsRecord(
    config,
    recordId,
    async () => Response.json({ zoneId, record: null }),
  );
  assert.equal(result.zoneId, zoneId);
  assert.equal(result.record, undefined);
  assert.equal(
    await deleteDnsRecord(
      config,
      {
        id: recordId,
        leaseId: "5be593e3-ddd1-471f-8711-6b32fcdccb39",
        name,
        type: "A",
        address: "1.1.1.1",
      },
      async () => Response.json({ zoneId, deletedId: null }),
    ),
    zoneId,
  );
});

test("rejects mismatched create and delete confirmations", async () => {
  await assert.rejects(
    createDnsRecord(
      config,
      {
        id: "5be593e3-ddd1-471f-8711-6b32fcdccb39",
        domain: name,
        recordType: "A",
        address: "1.1.1.1",
      },
      async () =>
        Response.json({
          zoneId,
          record: { ...record, content: "9.9.9.9" },
        }),
    ),
    /does not match/,
  );
  await assert.rejects(
    deleteDnsRecord(
      config,
      {
        id: recordId,
        leaseId: "5be593e3-ddd1-471f-8711-6b32fcdccb39",
        name,
        type: "A",
        address: "1.1.1.1",
      },
      async () => Response.json({ zoneId, deletedId: differentRecordId }),
    ),
    /different DNS record/,
  );
});

test("does not hide broker failures or missing zone identity", async () => {
  await assert.rejects(
    verifyDnsZone(
      config,
      async () =>
        Response.json(
          { error: { code: "UNAUTHORIZED", message: "denied" } },
          { status: 403 },
        ),
    ),
    /HTTP 403.*denied/,
  );
  await assert.rejects(
    verifyDnsZone(config, async () => Response.json({})),
    /zone identity/,
  );
});

test("requires Worker-format lowercase hexadecimal zone and record IDs", async () => {
  for (
    const invalidZoneId of [
      "short",
      "A".repeat(32),
      "g".repeat(32),
    ]
  ) {
    await assert.rejects(
      verifyDnsZone(
        config,
        async () => Response.json({ zoneId: invalidZoneId }),
      ),
      /invalid zone identity/,
    );
  }

  await assert.rejects(
    findDnsRecords(
      config,
      name,
      undefined,
      async () =>
        Response.json({
          zoneId,
          records: [{ ...record, id: "A".repeat(32) }],
        }),
    ),
    /invalid records/,
  );

  let called = false;
  const shouldNotFetch = async () => {
    called = true;
    return Response.json({ zoneId, record: null });
  };
  await assert.rejects(
    getDnsRecord(config, "not-a-provider-id", shouldNotFetch),
    /32 lowercase hexadecimal/,
  );
  await assert.rejects(
    deleteDnsRecord(
      config,
      {
        id: "A".repeat(32),
        leaseId: "5be593e3-ddd1-471f-8711-6b32fcdccb39",
        name,
        type: "A",
        address: "1.1.1.1",
      },
      shouldNotFetch,
    ),
    /32 lowercase hexadecimal/,
  );
  assert.equal(called, false);
});
