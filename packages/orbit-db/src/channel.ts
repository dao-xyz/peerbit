import { DirectChannel } from '@dao-xyz/ipfs-pubsub-1on1'
import Logger from 'logplease'
const logger = Logger.create('channel', { color: Logger.Colors.Yellow })
Logger.setLogLevel('ERROR')
export const getCreateChannel = async (ipfs, peer: string, getDirectConnection: (peer: string) => DirectChannel, onMessage: (message: { data: Uint8Array }) => void, onChannelCreated): Promise<DirectChannel> => {

    let channel = getDirectConnection(peer)
    if (!channel) {
        try {
            logger.debug(`Create a channel to ${peer}`)
            channel = await DirectChannel.open(ipfs, peer)
            channel.on('message', onMessage)
            logger.debug(`Channel created to ${peer}`)
            onChannelCreated(channel)
        } catch (e) {
            logger.error(e)
        }
    }

    // Wait for the direct channel to be fully connected
    await channel.connect()
    logger.debug(`Connected to ${peer}`)
    return channel

}