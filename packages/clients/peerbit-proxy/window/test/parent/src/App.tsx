import { createHost } from "@peerbit/proxy-window";
import { Peerbit } from "peerbit";
import { DirectSub } from "@peerbit/pubsub";
import { useEffect, useState } from "react";

const client = createHost(
	await Peerbit.create({
		libp2p: {
			services: { pubsub: (c) => new DirectSub(c, { emitSelf: true }) }
		}
	}),
	"*"
);

export const App = () => {
	const queryParameters = new URLSearchParams(window.location.search);

	const [frames, setFrames] = useState(0);
	useEffect(() => {
		setFrames(Number(queryParameters.get("frames")) || 0);
	}, []);
	return (
		<div>
			{Array.from(Array(frames), (e, i) => {
				return (
					<iframe
						data-testid={"pb" + i}
						key={i}
						id={"pb" + i}
						src={"http://localhost:5173/" /* + (i > 0 ? "?read=true" : "") */}
					></iframe>
				);
			})}
		</div>
	);
};
