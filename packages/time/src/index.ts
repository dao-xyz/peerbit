export const delay = (ms: number) => new Promise(res => setTimeout(res, ms));
export const waitFor = async (fn: () => boolean | Promise<boolean>, timeout: number = 60 * 1000) => {

    let startTime = +new Date;
    while (+new Date - startTime < timeout) {
        if (await fn()) {
            return;
        }
        await delay(50);
    }
    throw new Error("Timed out")

};
export const waitForAsync = async (fn: () => Promise<boolean>, timeout: number = 60 * 1000) => {

    let startTime = +new Date;
    while (+new Date - startTime < timeout) {
        if (await fn()) {
            return;
        }
        await delay(50);
    }
    throw new Error("Timed out")

};
