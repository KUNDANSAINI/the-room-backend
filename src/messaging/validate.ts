/**
 * Server-side text normalization. Messages are plain text end-to-end:
 * the server never produces HTML and clients must render text as text
 * (the web client does, via React). We therefore do NOT HTML-escape here
 * (that would corrupt "<3" or "a < b") — instead we remove characters that
 * enable spoofing or rendering abuse.
 */

export type ValidationResult =
  | { ok: true; text: string }
  | { ok: false; reason: "too_long" | "invalid" };

export interface TextLimits {
  maxLength: number;
  maxLines: number;
}

/** Builds a global character-class RegExp from code points (keeps this source file pure ASCII). */
function charClass(items: Array<number | [number, number]>): RegExp {
  const hex = (n: number) => "\\" + "u" + n.toString(16).padStart(4, "0");
  const body = items.map((it) => (Array.isArray(it) ? `${hex(it[0])}-${hex(it[1])}` : hex(it))).join("");
  return new RegExp(`[${body}]`, "g");
}

const LS = String.fromCharCode(0x2028);
const PS = String.fromCharCode(0x2029);
/** Tab, vertical tab, form feed → space; Unicode line/paragraph separators → newline. */
const SPACING = charClass([0x09, 0x0b, 0x0c, 0x2028, 0x2029]);

const CONTROL = charClass([[0x00, 0x09], [0x0b, 0x1f], [0x7f, 0x9f]]);
// Bidi overrides/isolates (Trojan-source style spoofing), zero-width & invisible formatting.
// ZWJ (U+200D) and variation selectors are kept: emoji sequences need them.
const INVISIBLE = charClass([0xad, 0x61c, 0x115f, 0x1160, 0x17b4, 0x17b5, 0x180e, 0x200b, 0x200c, 0x200e, 0x200f, [0x202a, 0x202e], [0x2060, 0x2064], [0x2066, 0x206f], 0x3164, 0xfeff, 0xffa0, [0xfff9, 0xfffb]]);
const LONE_SURROGATE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g;
// "Zalgo": cap stacked combining marks.
const MARK_FLOOD = /(\p{M}{3})\p{M}+/gu;
const VISIBLE = /[\p{L}\p{N}\p{S}\p{P}]/u;

export function normalizeText(raw: string, limits: TextLimits): ValidationResult {
  if (typeof raw !== "string") return { ok: false, reason: "invalid" };

  let s = raw
    .replace(LONE_SURROGATE, "")
    .normalize("NFC")
    .replace(/\r\n?/g, "\n")
    .replace(SPACING, (c) => (c === LS || c === PS ? "\n" : " "))
    .replace(CONTROL, "")
    .replace(INVISIBLE, "")
    .replace(MARK_FLOOD, "$1");

  // Trim each line's trailing whitespace, collapse runs of blank lines, trim the whole message.
  s = s
    .split("\n")
    .map((l) => l.replace(/\s+$/u, ""))
    .join("\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();

  // Too many lines: keep the first N, fold the rest into the last line.
  const lines = s.split("\n");
  if (lines.length > limits.maxLines) {
    s = [...lines.slice(0, limits.maxLines - 1), lines.slice(limits.maxLines - 1).join(" ")].join("\n");
  }

  if (!s || !VISIBLE.test(s)) return { ok: false, reason: "invalid" };
  // Count code points, not UTF-16 units, so emoji aren't double-counted.
  let count = 0;
  for (const _ of s) if (++count > limits.maxLength) return { ok: false, reason: "too_long" };
  return { ok: true, text: s };
}

/** Case/space-insensitive fingerprint used for duplicate heuristics. */
export function fingerprint(text: string): string {
  return text.toLowerCase().replace(/\s+/g, " ").trim();
}
