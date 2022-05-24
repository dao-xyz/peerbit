export const delay = (ms: number) => new Promise(res => setTimeout(res, ms));
export const waitFor = async (fn: () => boolean, timeout: number = 60 * 1000) => {

    let startTime = +new Date;
    while (+new Date - startTime < timeout) {
        if (fn()) {
            return;
        }
        await delay(50);
    }
    throw new Error("Timed out")

};
