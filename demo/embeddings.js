/**
 * Deterministic Semantic Embedding Generator
 * 
 * Uses signed feature hashing over word tokens and character 3-grams
 * with L2 unit normalization. Zero external dependencies.
 */

const DIMENSIONS = 16;

function simpleHash(str, seed = 0) {
  let h = 0x811c9dc5 ^ seed;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

/**
 * Generate a normalized float vector for any given text.
 * @param {string} text - Input text
 * @param {number} [dim=16] - Vector dimensions (default: 16)
 * @returns {number[]} Unit-normalized float vector
 */
function embedText(text, dim = DIMENSIONS) {
  if (!text || typeof text !== "string") {
    const zeroVec = new Array(dim).fill(0);
    zeroVec[0] = 1.0;
    return zeroVec;
  }

  const vec = new Array(dim).fill(0);
  const normalized = text.toLowerCase().replace(/[^a-z0-9_\-\s]/g, " ");
  const words = normalized.split(/\s+/).filter(Boolean);

  // 1. Word token projections
  for (const word of words) {
    const bucket = simpleHash(word, 1337) % dim;
    const sign = (simpleHash(word, 42) % 2 === 0) ? 1 : -1;
    vec[bucket] += 2.0 * sign;

    // Character 3-grams for subword similarity
    if (word.length >= 3) {
      for (let i = 0; i <= word.length - 3; i++) {
        const trigram = word.substring(i, i + 3);
        const tBucket = simpleHash(trigram, 997) % dim;
        const tSign = (simpleHash(trigram, 7) % 2 === 0) ? 1 : -1;
        vec[tBucket] += 0.5 * tSign;
      }
    }
  }

  // 2. Compute L2 norm
  let sumSq = 0;
  for (let i = 0; i < dim; i++) {
    sumSq += vec[i] * vec[i];
  }

  const norm = Math.sqrt(sumSq);
  if (norm === 0) {
    const fallback = new Array(dim).fill(0);
    fallback[0] = 1.0;
    return fallback;
  }

  // 3. Return unit-normalized vector rounded to 4 decimals
  return vec.map((v) => Number((v / norm).toFixed(4)));
}

module.exports = {
  embedText,
  DIMENSIONS,
};
