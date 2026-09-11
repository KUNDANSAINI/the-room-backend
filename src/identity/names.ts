import { randomInt } from "node:crypto";

// Curated, neutral word lists (no combination should read as offensive).
const ADJECTIVES = [
  "Blue", "Moon", "Red", "Quiet", "Amber", "Velvet", "Neon", "Paper", "Silver", "Wild", "Tiny", "Lucky",
  "Solar", "Misty", "Cosmic", "Golden", "Hidden", "Electric", "Mellow", "Frost", "Lunar", "Coral", "Echo",
  "Jade", "Crimson", "Sunny", "Rainy", "Swift", "Gentle", "Brave", "Clever", "Cozy", "Dusty", "Fuzzy",
  "Glass", "Honey", "Ivory", "Jolly", "Kind", "Lazy", "Magic", "Nimble", "Ocean", "Pixel", "Rusty",
  "Salty", "Stormy", "Sleepy", "Spicy", "Starry", "Sugar", "Thunder", "Violet", "Windy", "Zen", "Arctic",
  "Autumn", "Copper", "Maple", "Minty", "Orbit", "Plum", "Shadow", "Snowy", "Sonic", "Teal", "Urban",
];

const ANIMALS = [
  "Tiger", "Fox", "Panda", "Otter", "Heron", "Koala", "Lynx", "Moth", "Whale", "Raven", "Gecko", "Falcon",
  "Badger", "Orca", "Sparrow", "Yak", "Lemur", "Cricket", "Owl", "Wolf", "Bison", "Crane", "Squid", "Alpaca",
  "Beaver", "Camel", "Dolphin", "Eagle", "Ferret", "Gazelle", "Hedgehog", "Ibis", "Jaguar", "Kiwi", "Llama",
  "Marmot", "Narwhal", "Ocelot", "Penguin", "Quokka", "Rabbit", "Seal", "Toucan", "Walrus", "Zebra", "Bee",
  "Crow", "Deer", "Finch", "Goose", "Hare", "Iguana", "Kestrel", "Mole", "Newt", "Puffin", "Robin", "Salmon",
  "Tapir", "Viper", "Wren", "Stork", "Manta", "Hawk", "Moose", "Cobra", "Coyote", "Pelican", "Shrew",
];

/** e.g. "BlueTiger_2841". ~67 × 69 × 9000 ≈ 41M combinations per day. */
export function generatePublicName(): string {
  const adj = ADJECTIVES[randomInt(ADJECTIVES.length)];
  const animal = ANIMALS[randomInt(ANIMALS.length)];
  return `${adj}${animal}_${randomInt(1000, 10000)}`;
}

export const PUBLIC_NAME_RE = /^[A-Z][a-z]+[A-Z][a-z]+_\d{4}$/;
