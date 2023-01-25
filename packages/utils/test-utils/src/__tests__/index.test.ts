import { LSession } from "../session"
import { delay, waitFor } from '@dao-xyz/peerbit-time';
import { logger } from '@dao-xyz/libp2p-direct-sub'
logger.level = 'trace'
import { logger as logger2 } from '@dao-xyz/libp2p-direct-stream'

logger2.level = 'trace'
describe('session', () => {

	let session: LSession;
	beforeAll(async () => {
		session = await LSession.connected(3);
	})

	afterAll(async () => {
		await session.stop()
	})
	it('pubsub', async () => {
		let result: any = undefined;
		session.peers[0].directsub.listenForSubscribers("x");
		session.peers[1].directsub.listenForSubscribers("x");
		await session.peers[2].directsub.subscribe("x");
		session.peers[2].directsub.addEventListener('data', (evt) => {
			result = evt.detail;
		})

		await waitFor(() => session.peers[0].directsub.getSubscribers("x")?.size === 1)
		await waitFor(() => session.peers[1].directsub.getSubscribers("x")?.size === 1)

		session.peers[0].directsub.publish(new Uint8Array([1, 2, 3]), { topics: ['x'] })
		await waitFor(() => !!result)
	})
})