/* const {
	override,
	addBabelPlugins,
	addExternalBabelPlugins,
	addWebpackPlugin,
} = require('customize-cra');
const { DefinePlugin } = require('webpack');
 */
const { addBabelPlugins, disableEsLint, override } = require("customize-cra");

module.exports = (config) => {
	let loaders = config.resolve;

	loaders.fallback = {
		/*     
		    
			 "path": require.resolve("path-browserify"),
			 "buffer": require.resolve("buffer"),
			 
			 */
		child_process: false,
		fs: false,
		assert: false,
		os: false,
		http: false,
		util: false,
		yargs: false,
		net: false,
		"aws-sdk": false,
		url: false,
		path: require.resolve("path-browserify"),
		crypto: require.resolve("crypto-browserify"),
		stream: require.resolve("stream-browserify"),
		timers: require.resolve("timers-browserify"),
	};
	disableEsLint();
	config.module.rules = [
		...config.module.rules,
		{
			test: /\.m?js/,
			resolve: {
				fullySpecified: false,
			},
		},
	];

	config.experiments = {
		topLevelAwait: true,
	};

	/* config.plugins.push(new webpack.DefinePlugin({
		process: { env: {} }
	})) */
	/*  addWebpackPlugin(
		 new DefinePlugin({
			 process: { env: {} }
		 }),
	 ) */

	/*  config.plugins.push(new webpack.ProvidePlugin({
		 Buffer: ['buffer', 'Buffer'],
	 }))
 
	 */
	/*     config.optimization.splitChunks = { chunks: 'all' };
	 */
	return override(
		...addBabelPlugins([
			"@babel/plugin-transform-typescript",
			{ allowNamespaces: true },
			/*  '@babel/plugin-syntax-dynamic-import'  */
		])
	)(config);
};
