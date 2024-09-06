/* eslint-disable no-console */

/* eslint-disable @typescript-eslint/naming-convention */
import { type PeerId } from "@libp2p/interface";
import { delay } from "@peerbit/time";

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
	const { getMyIp } = await import("./domain.js");

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

const setupUserData = (email: string, grantAccess: PeerId[] = []) => {
	const peerIdStrings = grantAccess.map((x) => x.toString());

	// better-sqlite3 force use to install build-essentials for `make` command, TOOD dont bundle better-sqlite3 by default?
	return `#!/bin/bash
cd /home/ubuntu
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash - &&\
sudo apt-get install -y nodejs
sudo apt-get install -y build-essential
npm install -g @peerbit/server
sudo peerbit domain test --email ${email}
peerbit start ${peerIdStrings.map((key) => `--ga ${key}`)} > log.txt 2>&1 &
`;
};
const PURPOSE_TAG_NAME = "Purpose";
const PURPOSE_TAG_VALUE = "Peerbit";

// Ubuntu Server 20.04 LTS (HVM), SSD Volume Type (64-bit (Arm))
export const AWS_LINUX_ARM_AMIs: Record<string, string> = {
	/* 	"af-south-1" */
	"ap-northeast-1": "ami-01444f83954203c6f",
	"ap-northeast-2": "ami-0ac62099928d25fec",
	"ap-northeast-3": "ami-0efdceaebc778c5f3",
	"ap-south-1": "ami-0df6182e39efe7c4d",
	"ap-southeast-1": "ami-01d87e25d3c65ec37",
	"ap-southeast-2": "ami-0641fc20c25fdd380",
	/* 	"ap-south-2", */
	/* 	"ap-southeast-3",
		"ap-southeast-4", */
	"ca-central-1": "ami-0a3e942fe4813672b",
	/* 	"cn-north-1",
		"cn-northwest-1", */
	"eu-central-1": "ami-0d85ad3aa712d96af",
	/* 	"eu-central-2", */
	"eu-north-1": "ami-0ff124a3d7381bfec",
	/* 	"eu-south-1",
		"eu-south-2", */
	"eu-west-1": "ami-09c59b011574e4c96",
	"eu-west-2": "ami-03e26d11b665ac7be",
	"eu-west-3": "ami-00771c0ac817397bd",
	/* 	"il-central-1",
		"me-central-1",
		"me-south-1", */
	"sa-east-1": "ami-08dc4b989f93eafb9",
	"us-east-1": "ami-097d5b19d4f1a7d1b",
	"us-east-2": "ami-0071e4b30f26879e2",
	/* 	"us-gov-east-1",
		"us-gov-west-1", */
	"us-west-1": "ami-0dca369228f3b2ce7",
	"us-west-2": "ami-0c79a55dda52434da",
};
export const launchNodes = async (properties: {
	region?: string;
	email: string;
	count?: number;
	size?: "micro" | "small" | "medium" | "large" | "xlarge" | "2xlarge";
	namePrefix?: string;
	grantAccess?: PeerId[];
}): Promise<
	{ instanceId: string; publicIp: string; name: string; region: string }[]
> => {
	if (properties.count && properties.count > 10) {
		throw new Error(
			"Unexpected node launch count: " +
				properties.count +
				". To prevent unwanted behaviour you can also launch 10 nodes at once",
		);
	}
	const count = properties.count || 1;

	const {
		EC2Client,
		CreateTagsCommand,
		RunInstancesCommand,
		DescribeSecurityGroupsCommand,
		CreateSecurityGroupCommand,
		AuthorizeSecurityGroupIngressCommand,
		DescribeInstancesCommand,
	} = await import("@aws-sdk/client-ec2");
	const client = new EC2Client({ region: properties.region });
	const regionString = await client.config.region();

	let securityGroupOut = (
		await client.send(
			new DescribeSecurityGroupsCommand({
				Filters: [
					{ Name: "tag:" + PURPOSE_TAG_NAME, Values: [PURPOSE_TAG_VALUE] },
				],
			}),
		)
	)?.SecurityGroups?.[0];
	if (!securityGroupOut) {
		securityGroupOut = await client.send(
			new CreateSecurityGroupCommand({
				GroupName: "peerbit-node",
				Description: "Security group for running Peerbit nodes",
			}),
		);
		await client.send(
			new CreateTagsCommand({
				Resources: [securityGroupOut.GroupId!],
				Tags: [{ Key: PURPOSE_TAG_NAME, Value: PURPOSE_TAG_VALUE }],
			}),
		);
		await client.send(
			new AuthorizeSecurityGroupIngressCommand({
				GroupId: securityGroupOut.GroupId,
				IpPermissions: [
					{
						IpRanges: [{ CidrIp: "0.0.0.0/0" }],
						IpProtocol: "tcp",
						FromPort: 80,
						ToPort: 80,
					}, // Frontend
					{
						IpRanges: [{ CidrIp: "0.0.0.0/0" }],
						IpProtocol: "tcp",
						FromPort: 443,
						ToPort: 443,
					}, // Frontend SSL
					{
						IpRanges: [{ CidrIp: "0.0.0.0/0" }],
						IpProtocol: "tcp",
						FromPort: 9002,
						ToPort: 9002,
					}, // HTTPS api
					{
						IpRanges: [{ CidrIp: "0.0.0.0/0" }],
						IpProtocol: "tcp",
						FromPort: 8082,
						ToPort: 8082,
					}, // HTTP api
					{
						IpRanges: [{ CidrIp: "0.0.0.0/0" }],
						IpProtocol: "tcp",
						FromPort: 4002,
						ToPort: 4005,
					}, // libp2p
					{
						IpRanges: [{ CidrIp: "0.0.0.0/0" }],
						IpProtocol: "tcp",
						FromPort: 22,
						ToPort: 22,
					}, // SSH
				],
			}),
		);
	}
	const instanceTag =
		"Peerbit" + (properties.namePrefix ? "-" + properties.namePrefix : "");
	let existingCounter =
		(
			await client.send(
				new DescribeInstancesCommand({
					Filters: [{ Name: "tag:Purpose", Values: [instanceTag] }],
				}),
			)
		).Reservations?.length || 0;

	console.log("Region: " + regionString);
	const instanceOut = await client.send(
		new RunInstancesCommand({
			ImageId: AWS_LINUX_ARM_AMIs[regionString],
			SecurityGroupIds: [securityGroupOut.GroupId!],
			InstanceType: ("t4g." + (properties.size || "micro")) as any, // TODO types
			UserData: Buffer.from(
				setupUserData(properties.email, properties.grantAccess),
			).toString("base64"),
			MinCount: count,
			MaxCount: count,
			// InstanceInitiatedShutdownBehavior: 'terminate' // to enable termination when node shutting itself down
		}),
	);

	if (!instanceOut.Instances || instanceOut.Instances.length === 0) {
		throw new Error("Failed to create instance");
	}

	const names: string[] = [];
	for (const instance of instanceOut.Instances) {
		existingCounter++;
		const name =
			(properties.namePrefix ? properties.namePrefix : "peerbit-node") +
			"-" +
			existingCounter;
		names.push(name);
		await client.send(
			new CreateTagsCommand({
				Resources: [instance.InstanceId!],
				Tags: [
					{ Key: "Name", Value: name },
					{ Key: "Purpose", Value: instanceTag },
				],
			}),
		);
	}

	// wait for instance ips to become available
	const info = await client.send(
		new DescribeInstancesCommand({
			InstanceIds: instanceOut.Instances.map((x) => x.InstanceId!),
		}),
	);
	const foundInstances = info
		.Reservations!.map((x) => x.Instances!.map((y) => y))
		.flat()!;
	const foundIps: string[] = [];
	for (const out of instanceOut.Instances) {
		const foundInstance = foundInstances.find(
			(x) => x!.InstanceId === out.InstanceId!,
		);
		if (!foundInstance!.PublicIpAddress) {
			await delay(3000);
			continue;
		}
		foundIps.push(foundInstance!.PublicIpAddress!);
	}
	let publicIps: string[] = foundIps;

	if (publicIps.length === 0) {
		throw new Error("Failed to resolve IPs for created instances");
	}

	return publicIps.map((v, ix) => {
		return {
			instanceId: instanceOut.Instances![ix].InstanceId!,
			publicIp: v,
			name: names[ix],
			region: regionString,
		};
	}); // TODO types
};

export const terminateNode = async (properties: {
	instanceId: string;
	region?: string;
}) => {
	const { EC2Client, TerminateInstancesCommand } = await import(
		"@aws-sdk/client-ec2"
	);
	const client = new EC2Client({ region: properties.region });
	await client.send(
		new TerminateInstancesCommand({ InstanceIds: [properties.instanceId] }),
	);
};
