import { createHost } from "@peerbit/proxy-window";
import { Peerbit } from "peerbit";
import { useEffect, useState } from "react";

const client = createHost(await Peerbit.create({ directory: "hello" }), "*");
export const App = () => {
	const queryParameters = new URLSearchParams(window.location.search);
	const [frames, setFrames] = useState(0);
	useEffect(() => {
		setFrames(Number(queryParameters.get("frames")) || 0);
	}, []);
	return (
		<div>
			{Array.from(Array(frames), (_e, i) => {
				return (
					<iframe
						data-testid={"pb" + i}
						key={i}
						id={"pb" + i}
						src={"http://localhost:5201/"}
					></iframe>
				);
			})}
		</div>
	);
};
