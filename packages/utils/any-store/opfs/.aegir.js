export default {
	// global options
	debug: false,
	// test cmd options
	build: {
		bundle: true,
		bundlesize: false,
		bundlesizeMax: "100kB",
		types: true,
		config: {
			minify: true,
			outfile: "dist/peerbit/anystore-opfs-worker.min.js",
			banner: { js: "" },
			footer: { js: "" },
		},
	},
};
