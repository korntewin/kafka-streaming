import { NextRequest, NextResponse } from "next/server";
import { getCollection } from "@/lib/mongodb";

const MONGO_DB_COLLECTION = process.env.MONGO_DB_COLLECTION || "reviews";

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const page = parseInt(searchParams.get("page") || "1");
    const limit = parseInt(searchParams.get("limit") || "10");
    const skip = (page - 1) * limit;

    const collection = await getCollection(MONGO_DB_COLLECTION);

    const data = await collection.find({}).skip(skip).limit(limit).toArray();

    const total = await collection.countDocuments();

    return NextResponse.json({
      data,
      pagination: {
        page,
        limit,
        total,
        pages: Math.ceil(total / limit),
      },
    });
  } catch (error) {
    console.error("Error fetching data:", error);
    return NextResponse.json(
      { error: "Failed to fetch data" },
      { status: 500 },
    );
  }
}
