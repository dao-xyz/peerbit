import { debounceAccumulator } from "@peerbit/time";

type CounterWithKey = {
	counter: number;
	key: string;
};
export const debouncedAccumulatorSetCounter = (
	fn: (args: Map<string, CounterWithKey>) => any,
	delay: number,
) => {
	return debounceAccumulator<
		string,
		{ key: string },
		Map<string, CounterWithKey>
	>(
		fn,
		() => {
			const set = new Map<string, CounterWithKey>();
			let add = (props: { key: string }) => {
				let prev = set.get(props.key);
				if (prev != null) {
					prev.counter++;
				} else {
					set.set(props.key, {
						counter: 1,
						key: props.key,
					});
				}
			};
			return {
				add,
				delete: (key: string) => {
					let prev = set.get(key);
					if (prev != null) {
						if (prev.counter > 1) {
							prev.counter--;
						} else {
							set.delete(key);
						}
					}
					return prev != null;
				},
				size: () => set.size,
				value: set,
				clear: () => set.clear(),
				has: (key: string) => set.has(key),
			};
		},
		delay,
		{
			leading: false, // for the purposes of pubsub use, we don't want leading
		},
	);
};

export type DebouncedAccumulatorCounterMap = ReturnType<
	typeof debouncedAccumulatorSetCounter
>;
