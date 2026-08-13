const test = require("node:test");
const assert = require("node:assert/strict");

const { bookDepth, bookLevel, safeHttpUrl } = require("../web/assets/app.js");

test("normalizes unordered CLOB levels before selecting top of book", () => {
  const book = {
    bids: [{ price: "0.01", size: "10" }, { price: "0.41", size: "2" }, { price: "0.20", size: "4" }],
    asks: [{ price: "0.99", size: "5" }, { price: "0.42", size: "3" }, { price: "0.60", size: "7" }],
  };

  assert.deepEqual(bookLevel(book, "bids"), { price: 0.41, size: 2 });
  assert.deepEqual(bookLevel(book, "asks"), { price: 0.42, size: 3 });
  assert.equal(bookDepth(book, "bids"), 16);
  assert.equal(bookDepth(book, "asks"), 15);
});

test("only permits absolute HTTP(S) resolution links", () => {
  assert.equal(safeHttpUrl("https://example.com/rules"), "https://example.com/rules");
  assert.equal(safeHttpUrl("javascript:alert(1)"), "#");
  assert.equal(safeHttpUrl("not a url"), "#");
});
