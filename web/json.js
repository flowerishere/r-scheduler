// Preserve integer JSON tokens that cannot be represented exactly by Number.
// Native JSON.parse still validates the grammar; the parallel token tree keeps
// number spelling without modifying quoted text or trusting user marker keys.
export function parseExactJson(text) {
  const value = JSON.parse(text);
  const tokens = JSON.parse(text.replace(/"(?:\\.|[^"\\])*"|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/g,
    (token) => token.startsWith('"') ? token : JSON.stringify(token)));
  function restore(item, source) {
    if (typeof item === "number") {
      if (!Number.isFinite(item)) throw new RangeError("JSON number exceeds the supported range");
      return /^-?\d+$/.test(source) && !Number.isSafeInteger(item) ? BigInt(source) : item;
    }
    if (item && typeof item === "object") {
      for (const key of Object.keys(item)) item[key] = restore(item[key], source[key]);
    }
    return item;
  }
  return restore(value, tokens);
}

// JSON values only. BigInt is emitted as a number token, never as a string.
export function stringifyExactJson(value, space = 0) {
  const gap = " ".repeat(Math.min(10, Math.max(0, space)));
  function encode(item, depth) {
    if (typeof item === "bigint") return item.toString();
    if (item === null || typeof item !== "object") return JSON.stringify(item);
    const array = Array.isArray(item);
    const entries = array ? item.map((entry) => encode(entry, depth + 1) ?? "null")
      : Object.keys(item).filter((key) => item[key] !== undefined).map((key) =>
        `${JSON.stringify(key)}:${gap ? " " : ""}${encode(item[key], depth + 1)}`);
    const [open, close] = array ? ["[", "]"] : ["{", "}"];
    if (!entries.length) return open + close;
    if (!gap) return open + entries.join(",") + close;
    const indent = gap.repeat(depth + 1);
    return `${open}\n${indent}${entries.join(`,\n${indent}`)}\n${gap.repeat(depth)}${close}`;
  }
  return encode(value, 0);
}
