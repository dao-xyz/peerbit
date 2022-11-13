import "jest-extended";

function fail(reason = "fail was called in a test.") {
    throw new Error(reason);
}

global.fail = fail;
