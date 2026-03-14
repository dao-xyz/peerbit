import {
	BinaryWriter,
	deserialize,
	field,
	fixedArray,
	option,
	serialize,
	variant,
	vec,
} from "@dao-xyz/borsh";
import type { PeerId } from "@libp2p/interface";
import {
	PreHash,
	PublicSignKey,
	SignatureWithKey,
	prehashFn,
	randomBytes,
	toBase64,
	verifyPrepared,
} from "@peerbit/crypto";
import { Uint8ArrayList } from "uint8arraylist";
import { type PeerRefs, coercePeerRefsToHashes } from "./keys.js";

export const ID_LENGTH = 32;

const WEEK_MS = 7 * 24 * 60 * 60 + 1000;

const SIGNATURES_SIZE_ENCODING = "u8"; // with 7 steps you know everyone in the world?, so u8 *should* suffice

/**
 * The default msgID implementation
 * Child class can override this.
 */
export const getMsgId = async (msg: Uint8ArrayList | Uint8Array) => {
	// first bytes fis discriminator,
	// next 32 bytes should be an id
	return toBase64(msg.subarray(0, 33)); // discriminator + id (33 bytes)
};

export const deliveryModeHasReceiver = (
	mode: DeliveryMode,
): mode is { to: string[] } => {
	if (mode instanceof SilentDelivery && mode.to.length > 0) {
		return true;
	}
	if (mode instanceof AcknowledgeDelivery && mode.to.length > 0) {
		return true;
	}
	return false;
};

export abstract class DeliveryMode {}

/**
 * when you just want to deliver at paths, but does not expect acknowledgement
 */
@variant(0)
export class SilentDelivery extends DeliveryMode {
	@field({ type: vec("string") })
	to: string[];

	@field({ type: "u8" })
	redundancy: number;

	constructor(properties: { to: PeerRefs; redundancy: number }) {
		super();
		this.to = coercePeerRefsToHashes(properties.to);
		this.redundancy = properties.redundancy;
	}
}

/**
 * Deliver and expect acknowledgement
 */
@variant(1)
export class AcknowledgeDelivery extends DeliveryMode {
	@field({ type: vec("string") })
	to: string[];

	@field({ type: "u8" })
	redundancy: number;

	constructor(properties: { to: PeerRefs; redundancy: number }) {
		super();
		const to = coercePeerRefsToHashes(properties.to);
		if (to.length === 0) {
			throw new Error(
				"Invalud value of property 'to', expecting either undefined or an array with more than one element",
			);
		}
		this.to = to;
		this.redundancy = properties.redundancy;
	}
}

/**
 * Flood (AnyWhere) but expect acknowledgement.
 *
 * This is primarily useful for control-plane probing / route discovery where
 * the sender does not have (or does not want) an explicit recipient list.
 */
@variant(5)
export class AcknowledgeAnyWhere extends DeliveryMode {
	@field({ type: "u8" })
	redundancy: number;

	constructor(properties: { redundancy: number }) {
		super();
		this.redundancy = properties.redundancy;
	}
}

/**
 * Delivery mode used for acknowledgement frames and debugging.
 * Carries a trace of hops.
 */
@variant(3)
export class TracedDelivery extends DeliveryMode {
	@field({ type: vec("string") })
	trace: string[];

	constructor(trace: string[]) {
		super();
		this.trace = trace;
	}
}

@variant(4)
export class AnyWhere extends DeliveryMode {}

@variant(0)
export class Signatures {
	@field({ type: vec(SignatureWithKey, SIGNATURES_SIZE_ENCODING) })
	signatures: SignatureWithKey[];

	constructor(signatures: SignatureWithKey[] = []) {
		this.signatures = signatures;
	}

	equals(other: Signatures) {
		return (
			this.signatures.length === other.signatures.length &&
			this.signatures.every((value, i) => other.signatures[i].equals(value))
		);
	}

	get publicKeys(): PublicSignKey[] {
		return this.signatures.map((x) => x.publicKey);
	}
}

abstract class PeerInfo {}

@variant(0)
export class MultiAddrinfo extends PeerInfo {
	@field({ type: vec("string") })
	multiaddrs: string[];

	constructor(multiaddrs: string[]) {
		super();
		this.multiaddrs = multiaddrs;
	}
}

export type WithTo = {
	to?: (string | PublicSignKey | PeerId)[] | Set<string>;
};

export type WithMode = {
	mode?: SilentDelivery | AcknowledgeDelivery | AcknowledgeAnyWhere | AnyWhere;
};

export type PriorityOptions = {
	priority?: number;
};

export type ResponsePriorityOptions = {
	responsePriority?: number;
};

export type ExpiresAtOptions = {
	expiresAt?: number;
};

export type IdOptions = {
	id?: Uint8Array;
};

export type WithExtraSigners = {
	extraSigners?: ((
		data: Uint8Array,
	) => Promise<SignatureWithKey> | SignatureWithKey)[];
};

const getDefaultPriorityFromMode = (mode: DeliveryMode) => {
	if (mode instanceof SilentDelivery) {
		return 0;
	}
	if (mode instanceof AnyWhere) {
		return 0;
	}
	if (mode instanceof AcknowledgeAnyWhere) {
		return 1;
	}

	if (mode instanceof AcknowledgeDelivery) {
		return 1;
	}
	if (mode instanceof TracedDelivery) {
		return 1;
	}
	throw new Error("Unexpected mode: " + mode.constructor.name);
};

@variant(0)
export class MessageHeader<T extends DeliveryMode = DeliveryMode> {
	@field({ type: fixedArray("u8", ID_LENGTH) })
	private _id: Uint8Array;

	@field({ type: "u64" })
	timestamp: bigint;

	@field({ type: "u64" })
	session: bigint;

	@field({ type: "u64" })
	expires: bigint;

	// Priority. Higher numbers are treated as higher priority by the transport scheduler.
	@field({ type: option("u32") })
	priority?: number;

	// Preferred priority for the response path to this message. Higher numbers are
	// treated as higher priority by the transport scheduler.
	@field({ type: option("u32") })
	responsePriority?: number;

	@field({ type: option(PeerInfo) })
	private _origin?: MultiAddrinfo;

	// Not signed, since we might want to modify it during transit
	@field({ type: option(DeliveryMode) })
	mode: T;

	// Not signed, since we might want to modify it during transit
	@field({ type: option(Signatures) })
	signatures: Signatures | undefined;

	constructor(properties: {
		origin?: MultiAddrinfo;
		expires?: number;
		session: number;
		id?: Uint8Array;
		mode: T;
		priority?: number;
		responsePriority?: number;
	}) {
		this._id = properties?.id || randomBytes(ID_LENGTH);
		this.expires = BigInt(properties?.expires || +new Date() + WEEK_MS);
		this.timestamp = BigInt(+new Date());
		this.session = BigInt(properties.session);
		this.signatures = new Signatures();
		this._origin = properties?.origin;
		this.mode = properties.mode;
		this.priority =
			properties.priority != null
				? properties.priority
				: getDefaultPriorityFromMode(this.mode);
		this.responsePriority = properties.responsePriority;
	}

	get id() {
		return this._id;
	}

	get origin(): MultiAddrinfo | undefined {
		return this._origin;
	}

	writeBytes(
		writer: BinaryWriter,
		properties: { includeMode: boolean; includeSignatures: boolean },
	) {
		writer.u8(0);
		BinaryWriter.uint8ArrayFixed(this._id, writer);
		writer.u64(this.timestamp);
		writer.u64(this.session);
		writer.u64(this.expires);
		if (this.priority != null) {
			writer.u8(1);
			writer.u32(this.priority);
		} else {
			writer.u8(0);
		}
		if (this.responsePriority != null) {
			writer.u8(1);
			writer.u32(this.responsePriority);
		} else {
			writer.u8(0);
		}
		if (this._origin != null) {
			writer.u8(1);
			serialize(this._origin, writer);
		} else {
			writer.u8(0);
		}
		if (properties.includeMode && this.mode != null) {
			writer.u8(1);
			serialize(this.mode, writer);
		} else {
			writer.u8(0);
		}
		if (properties.includeSignatures && this.signatures != null) {
			writer.u8(1);
			serialize(this.signatures, writer);
		} else {
			writer.u8(0);
		}
	}

	verify() {
		return this.expires >= +new Date();
	}
}

interface WithHeader {
	header: MessageHeader;
}

const serializeUnsigned = (obj: WithHeader): Uint8Array => {
	const mode = obj.header.mode;
	obj.header.mode = undefined as any;
	const signatures = obj.header.signatures;
	obj.header.signatures = undefined;
	const bytes = serialize(obj);
	obj.header.signatures = signatures;
	obj.header.mode = mode;
	return bytes;
};

const sign = async <T extends WithHeader>(
	obj: T,
	signer: (bytes: Uint8Array) => Promise<SignatureWithKey> | SignatureWithKey,
): Promise<T> => {
	const bytes =
		obj instanceof Message ? obj.getSignableBytes() : serializeUnsigned(obj);
	const signatures = obj.header.signatures;

	const signature = await signer(bytes);
	obj.header.signatures = new Signatures(
		signatures ? [...signatures.signatures, signature] : [signature],
	);
	return obj;
};

const verifyMultiSig = async (
	message: WithHeader,
	expectSignatures: boolean,
) => {
	const signatures = message.header.signatures;
	if (!signatures || signatures.signatures.length === 0) {
		return !expectSignatures;
	}
	const bytes =
		message instanceof Message
			? message.getSignableBytes()
			: serializeUnsigned(message);
	const preparedByPrehash = new Map<number, Uint8Array>();

	for (const signature of signatures.signatures) {
		let prepared = preparedByPrehash.get(signature.prehash);
		if (!prepared) {
			prepared =
				signature.prehash === PreHash.NONE
					? bytes
					: await prehashFn(bytes, signature.prehash);
			preparedByPrehash.set(signature.prehash, prepared);
		}
		if (!(await verifyPrepared(signature, prepared))) {
			return false;
		}
	}
	return true;
};

export abstract class Message<T extends DeliveryMode = DeliveryMode> {
	protected _cachedSignableBytes?: Uint8Array;
	protected _cachedPreparedSignableBytes?: Map<number, Uint8Array>;

	static from(bytes: Uint8ArrayList) {
		if (bytes.get(0) === DATA_VARIANT) {
			// Data
			return DataMessage.from(bytes);
		} else if (bytes.get(0) === ACKNOWLEDGE_VARIANT) {
			return ACK.from(bytes);
		} else if (bytes.get(0) === HELLO_VARIANT) {
			return Hello.from(bytes);
		} else if (bytes.get(0) === GOODBYE_VARIANT) {
			return Goodbye.from(bytes);
		}
		throw new Error("Unsupported");
	}

	abstract get header(): MessageHeader<T>;

	async sign(
		signer: (bytes: Uint8Array) => Promise<SignatureWithKey> | SignatureWithKey,
	): Promise<this> {
		return sign(this, signer);
	}

	getSignableBytes(): Uint8Array {
		return (
			this._cachedSignableBytes ??
			(this._cachedSignableBytes = serializeUnsigned(this))
		);
	}

	async getPreparedSignableBytes(prehash: PreHash): Promise<Uint8Array> {
		if (prehash === PreHash.NONE) {
			return this.getSignableBytes();
		}
		let prepared = this._cachedPreparedSignableBytes?.get(prehash);
		if (prepared) {
			return prepared;
		}
		prepared = await prehashFn(this.getSignableBytes(), prehash);
		if (!this._cachedPreparedSignableBytes) {
			this._cachedPreparedSignableBytes = new Map();
		}
		this._cachedPreparedSignableBytes.set(prehash, prepared);
		return prepared;
	}

	abstract bytes(): Uint8ArrayList | Uint8Array;
	/* abstract equals(other: Message): boolean; */
	_verified: boolean;

	async verify(expectSignatures: boolean): Promise<boolean> {
		return this._verified != null
			? this._verified
			: (this._verified =
					(await this.header.verify()) &&
					(await verifyMultiSig(this, expectSignatures)));
	}
}

// I pack data with this message
const DATA_VARIANT = 0;

const readU8At = (bytes: Uint8ArrayList, offset: number) => bytes.get(offset);

const readU32LEAt = (bytes: Uint8ArrayList, offset: number) =>
	bytes.getUint32(offset, true);

const skipBorshBytes = (bytes: Uint8ArrayList, offset: number) => {
	const length = readU32LEAt(bytes, offset);
	return offset + 4 + length;
};

const skipBorshString = (bytes: Uint8ArrayList, offset: number) =>
	skipBorshBytes(bytes, offset);

const skipStringVec = (bytes: Uint8ArrayList, offset: number) => {
	const length = readU32LEAt(bytes, offset);
	offset += 4;
	for (let i = 0; i < length; i++) {
		offset = skipBorshString(bytes, offset);
	}
	return offset;
};

const skipPeerInfo = (bytes: Uint8ArrayList, offset: number) => {
	const variant = readU8At(bytes, offset);
	offset += 1;
	if (variant === 0) {
		return skipStringVec(bytes, offset);
	}
	throw new Error(`Unsupported peer info variant: ${variant}`);
};

const skipDeliveryMode = (bytes: Uint8ArrayList, offset: number) => {
	const variant = readU8At(bytes, offset);
	offset += 1;
	if (variant === 0 || variant === 1) {
		offset = skipStringVec(bytes, offset);
		return offset + 1;
	}
	if (variant === 5) {
		return offset + 1;
	}
	if (variant === 3) {
		return skipStringVec(bytes, offset);
	}
	if (variant === 4) {
		return offset;
	}
	throw new Error(`Unsupported delivery mode variant: ${variant}`);
};

const skipPublicSignKey = (bytes: Uint8ArrayList, offset: number) => {
	const variant = readU8At(bytes, offset);
	offset += 1;
	if (variant === 0) {
		return offset + 32;
	}
	if (variant === 1) {
		return offset + 33;
	}
	throw new Error(`Unsupported public sign key variant: ${variant}`);
};

const skipSignatureWithKey = (bytes: Uint8ArrayList, offset: number) => {
	const variant = readU8At(bytes, offset);
	if (variant !== 0) {
		throw new Error(`Unsupported signature variant: ${variant}`);
	}
	offset += 1;
	offset = skipBorshBytes(bytes, offset);
	offset = skipPublicSignKey(bytes, offset);
	return offset + 1;
};

const skipSignatures = (bytes: Uint8ArrayList, offset: number) => {
	const variant = readU8At(bytes, offset);
	if (variant !== 0) {
		throw new Error(`Unsupported signatures variant: ${variant}`);
	}
	offset += 1;
	const length = readU8At(bytes, offset);
	offset += 1;
	for (let i = 0; i < length; i++) {
		offset = skipSignatureWithKey(bytes, offset);
	}
	return offset;
};

const getDataMessageDataFlagOffset = (bytes: Uint8ArrayList) => {
	let offset = 0;
	if (readU8At(bytes, offset) !== DATA_VARIANT) {
		throw new Error("Unsupported");
	}
	offset += 1;
	if (readU8At(bytes, offset) !== 0) {
		throw new Error("Unsupported message header variant");
	}
	offset += 1;
	offset += ID_LENGTH;
	offset += 8 * 3;
	if (readU8At(bytes, offset) === 1) {
		offset += 5;
	} else {
		offset += 1;
	}
	if (readU8At(bytes, offset) === 1) {
		offset += 5;
	} else {
		offset += 1;
	}
	if (readU8At(bytes, offset) === 1) {
		offset = skipPeerInfo(bytes, offset + 1);
	} else {
		offset += 1;
	}
	if (readU8At(bytes, offset) === 1) {
		offset = skipDeliveryMode(bytes, offset + 1);
	} else {
		offset += 1;
	}
	if (readU8At(bytes, offset) === 1) {
		offset = skipSignatures(bytes, offset + 1);
	} else {
		offset += 1;
	}
	return offset;
};

@variant(DATA_VARIANT)
export class DataMessage<
	T extends
		| SilentDelivery
		| AcknowledgeDelivery
		| AcknowledgeAnyWhere
		| AnyWhere =
		| SilentDelivery
		| AcknowledgeDelivery
		| AcknowledgeAnyWhere
		| AnyWhere,
> extends Message<T> {
	@field({ type: MessageHeader })
	private _header: MessageHeader<T>;

	@field({ type: option(Uint8Array) })
	private _data?: Uint8Array;

	private _dataBytes?: Uint8Array | Uint8ArrayList;

	constructor(properties: {
		header: MessageHeader<T>;
		data?: Uint8Array | Uint8ArrayList;
	}) {
		super();
		this._header = properties.header;
		this.setDataSource(properties.data);
	}

	get id(): Uint8Array {
		return this._header.id;
	}

	get header(): MessageHeader<T> {
		return this._header;
	}

	get data(): Uint8Array | undefined {
		if (this._data == null && this._dataBytes instanceof Uint8ArrayList) {
			this._data = this._dataBytes.subarray();
		}
		return this._data;
	}

	get dataByteLength(): number {
		return this._dataBytes?.byteLength ?? this._data?.byteLength ?? 0;
	}

	get hasData(): boolean {
		return this.dataByteLength > 0;
	}

	/** Manually ser/der for performance gains */
	bytes() {
		return this.serializeBytes({
			includeMode: true,
			includeSignatures: true,
		});
	}

	override getSignableBytes(): Uint8Array {
		return (
			this._cachedSignableBytes ??
			(this._cachedSignableBytes = this.serializeBytes({
				includeMode: false,
				includeSignatures: false,
			}).subarray())
		);
	}

	private setDataSource(data?: Uint8Array | Uint8ArrayList) {
		this._dataBytes = data;
		this._data = data instanceof Uint8Array ? data : undefined;
		this._cachedSignableBytes = undefined;
		this._cachedPreparedSignableBytes = undefined;
	}

	private serializeBytes(properties: {
		includeMode: boolean;
		includeSignatures: boolean;
	}) {
		const writer = new BinaryWriter();
		writer.u8(DATA_VARIANT);
		this._header.writeBytes(writer, properties);
		const data = this._dataBytes ?? this._data;
		if (data != null) {
			writer.u8(1);
			writer.u32(data.byteLength);
			return new Uint8ArrayList(writer.finalize(), data);
		} else {
			writer.u8(0);
		}
		return writer.finalize();
	}

	static from(bytes: Uint8ArrayList): DataMessage {
		const dataFlagOffset = getDataMessageDataFlagOffset(bytes);
		const hasData = readU8At(bytes, dataFlagOffset) === 1;
		const headerOnlyBytes = hasData
			? (() => {
					const prefix = bytes.subarray(0, dataFlagOffset);
					const out = new Uint8Array(prefix.byteLength + 1);
					out.set(prefix, 0);
					out[prefix.byteLength] = 0;
					return out;
				})()
			: bytes.subarray();
		const ret = deserialize(headerOnlyBytes, DataMessage);
		if (hasData) {
			const dataLength = readU32LEAt(bytes, dataFlagOffset + 1);
			const dataStart = dataFlagOffset + 5;
			ret.setDataSource(bytes.sublist(dataStart, dataStart + dataLength));
		}
		return ret;
	}
}

export const getMessageExpiresAt = (message: Message | DataMessage) =>
	Number(message.header.expires);

export const getMessageRemainingTime = (
	message: Message | DataMessage,
	now: number = Date.now(),
) => Math.max(0, getMessageExpiresAt(message) - now);

export const getResponsePriorityFromMessage = (message: Message | DataMessage) =>
	message.header.responsePriority ?? message.header.priority;

export type RequestTransportContext = {
	readonly expiresAt: number;
	readonly requestPriority?: number;
	readonly responsePriority?: number;
	remainingTime(now?: number): number;
	withResponseOptions<T extends object>(
		options: T & Partial<PriorityOptions & ExpiresAtOptions>,
	): T & PriorityOptions & ExpiresAtOptions;
};

export const createRequestTransportContext = (
	message: Message | DataMessage,
): RequestTransportContext => ({
	expiresAt: getMessageExpiresAt(message),
	requestPriority: message.header.priority,
	responsePriority: getResponsePriorityFromMessage(message),
	remainingTime: (now?: number) => getMessageRemainingTime(message, now),
	withResponseOptions: <T extends object>(
		options: T & Partial<PriorityOptions & ExpiresAtOptions>,
	) => ({
		...options,
		...inheritResponseTransportOptions(message, options),
	}),
});

export const inheritResponseTransportOptions = (
	message: Message | DataMessage,
	overrides?: PriorityOptions & ExpiresAtOptions,
): PriorityOptions & ExpiresAtOptions => ({
	priority:
		overrides?.priority != null
			? overrides.priority
			: getResponsePriorityFromMessage(message),
	expiresAt:
		overrides?.expiresAt != null
			? overrides.expiresAt
			: getMessageExpiresAt(message),
});

const ACKNOWLEDGE_VARIANT = 1;

@variant(ACKNOWLEDGE_VARIANT)
export class ACK extends Message {
	@field({ type: MessageHeader })
	header: MessageHeader<TracedDelivery>;

	@field({ type: fixedArray("u8", 32) })
	messageIdToAcknowledge: Uint8Array;

	@field({ type: "u8" })
	seenCounter: number; // Number of times a peer has received the messageIdToAcknowledge before

	constructor(properties: {
		messageIdToAcknowledge: Uint8Array;
		seenCounter: number;
		header: MessageHeader<TracedDelivery>;
	}) {
		super();
		this.header = properties.header;
		this.messageIdToAcknowledge = properties.messageIdToAcknowledge;
		this.seenCounter = Math.min(255, properties.seenCounter);
	}

	get id() {
		return this.header.id;
	}

	bytes() {
		return serialize(this);
	}

	static from(bytes: Uint8ArrayList): ACK {
		const result = deserialize(bytes.subarray(), ACK);
		if (
			!result.header.signatures ||
			result.header.signatures.signatures.length === 0
		) {
			throw new Error("Missing sender on ACK");
		}
		return result;
	}
}

const HELLO_VARIANT = 2;

@variant(HELLO_VARIANT)
export class Hello extends Message {
	@field({ type: MessageHeader })
	header: MessageHeader;

	@field({ type: vec("string") })
	joined: string[];

	constructor(properties: { joined: string[] }) {
		super();
		this.joined = properties.joined;
	}

	get id() {
		return this.header.id;
	}

	bytes() {
		return serialize(this);
	}

	static from(bytes: Uint8ArrayList): Hello {
		const result = deserialize(bytes.subarray(), Hello);
		if (
			!result.header.signatures ||
			result.header.signatures.signatures.length === 0
		) {
			throw new Error("Missing sender on Hello");
		}
		return result;
	}
}

const GOODBYE_VARIANT = 3;

@variant(GOODBYE_VARIANT)
export class Goodbye extends Message {
	@field({ type: MessageHeader })
	header: MessageHeader<SilentDelivery | AcknowledgeDelivery>;

	@field({ type: vec("string") })
	leaving: string[];

	constructor(properties: {
		leaving: string[];
		header: MessageHeader<SilentDelivery | AcknowledgeDelivery>;
	}) {
		super();
		this.header = properties.header;
		this.leaving = properties.leaving;
	}
	get id() {
		return this.header.id;
	}

	bytes() {
		return serialize(this);
	}

	static from(bytes: Uint8ArrayList): Goodbye {
		const result = deserialize(bytes.subarray(), Goodbye);
		if (
			!result.header.signatures ||
			result.header.signatures.signatures.length === 0
		) {
			throw new Error("Missing sender on Goodbye");
		}
		return result;
	}
}
