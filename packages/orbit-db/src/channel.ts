import Channel from 'ipfs-pubsub-1on1'
import Logger from 'logplease'
const logger = Logger.create('channel', { color: Logger.Colors.Yellow })
Logger.setLogLevel('ERROR')
export const getCreateChannel = async (ipfs, peer: string, getDirectConnection, onMessage: (message: { data: Uint8Array }) => void, onChannelCreated) => {

    let channel = getDirectConnection(peer)
    if (!channel) {
        try {
            logger.debug(`Create a channel to ${peer}`)
            channel = await Channel.open(ipfs, peer)
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