import {test} from "node:test";
import assert from "node:assert/strict";
import {parseExactJson, stringifyExactJson} from "../web/json.js";

test("integer boundaries and nested values round trip without changing type", () => {
  const source = '{"safe":9007199254740991,"large":9007199254740993,"limits":[18446744073709551615,-9223372036854775808],"fraction":1.25,"flag":false,"empty":null}';
  const value = parseExactJson(source);
  assert.equal(value.safe, 9007199254740991);
  assert.equal(value.large, 9007199254740993n);
  assert.deepEqual(value.limits, [18446744073709551615n, -9223372036854775808n]);
  assert.equal(stringifyExactJson(value), source);
  assert.deepEqual(parseExactJson(stringifyExactJson(value, 2)), value);
});

test("strings, escaped keys, duplicate keys and prototype names remain data", () => {
  const source = '{"text":"\\\"9007199254740993\\\" and \\\\ 1e400","x":1,"x":9007199254740993,"\\u0069d":9007199254740995,"__proto__":{"polluted":true},"constructor":null}';
  const value = parseExactJson(source);
  assert.equal(value.x, 9007199254740993n);
  assert.equal(value.id, 9007199254740995n);
  assert.equal(Object.getPrototypeOf(value), Object.prototype);
  assert.equal(Object.prototype.polluted, undefined);
  assert.deepEqual(parseExactJson(stringifyExactJson(value)), value);
  assert.equal(value.text, '\"9007199254740993\" and \\ 1e400');
});

test("invalid JSON and non-finite numeric overflow are rejected", () => {
  for (const source of ['{"id":01}', '[1,]', '{"x":NaN}', '{"x":undefined}', '"unterminated']) {
    assert.throws(() => parseExactJson(source), SyntaxError);
  }
  for (const source of ['1e400', '{"a":[-1e400]}']) assert.throws(() => parseExactJson(source), RangeError);
  assert.deepEqual(parseExactJson('["1e400",1e3,-0,1.5]'), ["1e400", 1000, -0, 1.5]);
});
