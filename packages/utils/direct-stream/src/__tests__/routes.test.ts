import { Routes } from "../routes";


describe('routes', () => {

	/* 
	┌─┐┌─┐
	│a││x│
	└┬┘└┬┘
	┌▽┐┌▽┐
	│b││y│
	└┬┘└─┘
	┌▽┐   
	│c│   
	└─┘   
	*/



	let routes: Routes;
	beforeAll(() => {
		routes = new Routes('_');
		routes.add('a', 'b');
		routes.add('b', 'c');
		routes.add('x', 'y');

	})
	it('will find path', () => {

		const path = routes.getPath('a', 'c');
		expect(path.map(x => x.id.toString())).toEqual(['a', 'b', 'c']);
	})

	it('missing node', () => {

		const path = routes.getPath('a', 'd');
		expect(path).toHaveLength(0)
	})
	it('missing path', () => {
		const path = routes.getPath('a', 'x');
		expect(path).toHaveLength(0)
	})
});
