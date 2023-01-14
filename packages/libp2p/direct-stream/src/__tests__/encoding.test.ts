import { DataMessage } from "../encoding.js";
import crypto from 'crypto'
import { Uint8ArrayList } from 'uint8arraylist'

describe('data', () => {


	/* 
		it('empty', () => {
	
			const data = new Uint8Array()
			const hb = new DataMessage({ data });
			const bytes = hb.serialize();
			expect(new Uint8Array(bytes)).toEqual(new Uint8Array([0, ...(hb.id as Uint8Array), 0, ...data]));
			const hbder = DataMessage.deserialize(new Uint8ArrayList(bytes));
			expect(hbder.equals(hb)).toBeTrue();
		})
	
		it('data', () => {
	
			const data = crypto.randomBytes(1e6)
			const hb = new DataMessage({ data });
			const bytes = hb.serialize();
			expect(bytes).toEqual(new Uint8Array([0, ...(hb.id as Uint8Array), 0, ...data]));
			const hbder = DataMessage.deserialize(new Uint8ArrayList(bytes));
			expect(hbder.equals(hb)).toBeTrue();
		})
	
		it('to', () => {
	
			const data = crypto.randomBytes(1e3)
			const hb = new DataMessage({ data, to: ['abc', 'xyz'] });
			const bytes = hb.serialize();
			expect(bytes[33]).toEqual(2);
			const hbder = DataMessage.deserialize(new Uint8ArrayList(bytes));
			expect(hbder.equals(hb)).toBeTrue();
		}) */
});
