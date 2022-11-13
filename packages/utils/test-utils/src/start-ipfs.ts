import { createController } from "ipfsd-ctl";
import testAPIs from "./test-apis.js";

/**
 * Start an IPFS instance
 * @param  {Object}  config  [IPFS configuration to use]
 * @return {[Promise<IPFS>]} [IPFS instance]
 */
export const startIpfs = async (
  type: "js-ipfs" | "go-ipfs" | string,
  config = {}
) => {
  const controllerConfig = testAPIs[type as "js-ipfs" | "go-ipfs"];
  if (!controllerConfig) {
    throw new Error(
      `Wanted API type ${JSON.stringify(
        type
      )} is unknown. Available types: ${Object.keys(testAPIs).join(", ")}`
    );
  }
  controllerConfig.ipfsOptions = config;
  controllerConfig.disposable = true;
  controllerConfig.test = true;

  // Spawn an IPFS daemon (type defined in)
  try {
    const ipfsd = await createController(controllerConfig);
    return ipfsd;
  } catch (err) {
    throw new Error(err as any);
  }
};
