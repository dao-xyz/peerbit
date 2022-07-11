export const delay = (ms: number, stopperCallback?: (stopper: () => void) => void) => {
    return new Promise((res, rej) => {
        if (stopperCallback)
            stopperCallback(() => res(true));
        setTimeout(res, ms)
    })
};


export const waitFor = async (fn: () => boolean | Promise<boolean>, timeout: number = 10 * 1000, stopperCallback?: (stopper: () => void) => void) => {

    let startTime = +new Date;
    let stop = false
    if (stopperCallback) {
        const stopper = () => { stop = true }
        stopperCallback(stopper);
    }
    while (+new Date - startTime < timeout) {
        if (stop) {
            return;
        }
        if (await fn()) {
            return;
        }
        await delay(50);

    }
    throw new Error("Timed out")

};
export const waitForAsync = async (fn: () => Promise<boolean>, timeout: number = 10 * 1000, stopperCallback?: (stopper: () => void) => void) => {

    let startTime = +new Date;
    let stop = false
    if (stopperCallback) {
        const stopper = () => { stop = true }
        stopperCallback(stopper);
    }
    while (+new Date - startTime < timeout) {
        if (stop) {
            return;
        }
        if (await fn()) {
            return;
        }
        await delay(50);
    }
    throw new Error("Timed out")
};
