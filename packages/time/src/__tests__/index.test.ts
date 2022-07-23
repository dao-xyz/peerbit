import { delay } from '../index'
describe('delay', () => {
    it('delay', async () => {
        let startTime = +new Date;
        await delay(1000)
        expect(+new Date - startTime).toBeLessThan(1200);

    })
    it('stop early', async () => {

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
    it('waitFor', async () => {
        let startTime = +new Date;
        await delay(1000)
        expect(+new Date - startTime).toBeLessThan(1200);

    })
    it('stop early', async () => {

        let startTime = +new Date;
        await delay(5000, (stop) => {
            setTimeout(() => {
                stop();
            }, 1000);
        })
        expect(+new Date - startTime).toBeLessThan(1200);

    })
})