import { getMyIp } from "./domain.js";

export const createRecord = async (options: {
    domain: string;
    region?: string;
    hostedZoneId: string;
    credentials?: { accessKeyId: string; secretAccessKey: string };
}): Promise<void> => {
    const { Route53Client, ChangeResourceRecordSetsCommand } = await import(
        "@aws-sdk/client-route-53"
    );
    const { isIPv4, isIPv6 } = await import("net");

    const myIp = await getMyIp();
    const v4 = isIPv4(myIp);
    const v6 = isIPv6(myIp);

    if (!v6 && !v4) {
        throw new Error("Unknown ip type");
    }
    // TODO, make sure it works for ipv6 addresses with leading and trailing colon
    const client = new Route53Client({
        region: options.region,
        credentials: options.credentials
            ? {
                  accessKeyId: options.credentials.accessKeyId,
                  secretAccessKey: options.credentials.secretAccessKey,
              }
            : undefined,
    });
    const cmd = new ChangeResourceRecordSetsCommand({
        ChangeBatch: {
            Changes: [
                {
                    Action: "CREATE",
                    ResourceRecordSet: {
                        Name: options.domain,
                        Type: v4 ? "A" : "AAAA",
                        TTL: 60,
                        ResourceRecords: [{ Value: myIp }],
                    },
                },
            ],
        },
        HostedZoneId: options.hostedZoneId,
    });
    await client.send(cmd);
};
