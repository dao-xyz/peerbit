import crypto from "crypto";
export const randomBytes = (len: number) => crypto.randomBytes(len);
