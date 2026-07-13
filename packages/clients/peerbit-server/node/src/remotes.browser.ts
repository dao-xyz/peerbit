export const getRetiredAWSManagementError = (origin: {
	instanceId: string;
	region: string;
}): Error =>
	new Error(
		`Automatic AWS management has been retired. Terminate EC2 instance ${origin.instanceId} in region ${origin.region} from the AWS console, then remove the local remote entry.`,
	);
