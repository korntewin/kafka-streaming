import { MongoClient, Db } from "mongodb";

// Connection URL - adjust according to your MongoDB setup
const MONGODB_URI =
  process.env.MONGO_DB_URI ||
  "mongodb://root:example@localhost:27017/?authSource=admin";
const DB_NAME = process.env.MONGO_DB_NAME || "reviews_db";

let cachedClient: MongoClient | null = null;
let cachedDb: Db | null = null;

export async function connectToDatabase() {
  if (cachedClient && cachedDb) {
    return { client: cachedClient, db: cachedDb };
  }

  const client = new MongoClient(MONGODB_URI);
  await client.connect();
  const db = client.db(DB_NAME);

  cachedClient = client;
  cachedDb = db;

  return { client, db };
}

export async function getCollection(collectionName: string) {
  const { db } = await connectToDatabase();
  return db.collection(collectionName);
}
