import { delay } from '../index'
describe('delay', () => {
    test('delay', async () => {
        let startTime = +new Date;
        await delay(1000)
        expect(+new Date - startTime).toBeLessThan(1200);

    })
    test('stop early', async () => {

        let startTime = +new Date;
        await delay(5000, (stop) => {
            setTimeout(() => {
                stop();
            }, 1000);
        })
        expect(+new Date - startTime).toBeLessThan(1200);

    })
})


describe('waitFor', () => {
    test('waitFor', async () => {
        let startTime = +new Date;
        await delay(1000)
        expect(+new Date - startTime).toBeLessThan(1200);

    })
    test('stop early', async () => {

        let startTime = +new Date;
        await delay(5000, (stop) => {
            setTimeout(() => {
                stop();
            }, 1000);
        })
        expect(+new Date - startTime).toBeLessThan(1200);

    })
})