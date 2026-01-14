#!/usr/bin/env python3
"""
Seed football 'assets' from existing Champions League clubs.

Why this exists:
- Auctions pull from `assets`, not `clubs`.
- The stress test uses competition code "CL".
- Many code paths filter assets by competition metadata (e.g. UCL) and/or active flags.

Run after: python seed_openfootball_clubs.py
"""

import asyncio
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / ".env")

SPORT_KEY = "football"

# Stress test default: "CL" (Champions League). Internally some systems use "UCL".
COMPETITION_CODE = os.getenv("FOOTBALL_COMPETITION_CODE", "CL")
COMPETITION_SHORT = os.getenv("FOOTBALL_COMPETITION_SHORT", "UCL")
COMPETITION_NAME = os.getenv("FOOTBALL_COMPETITION_NAME", "UEFA Champions League")

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

async def main() -> None:
    mongo_url = os.environ.get("MONGO_URL")
    db_name = os.environ.get("DB_NAME")

    if not mongo_url or not db_name:
        print("❌ Missing MONGO_URL or DB_NAME in environment/.env")
        sys.exit(1)

    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]

    try:
        clubs = await db.clubs.find({}).to_list(length=2000)
        if not clubs:
            print("❌ No clubs found. Run: python seed_openfootball_clubs.py")
            sys.exit(1)

        # Replace existing football assets to keep runs deterministic
        deleted = await db.assets.delete_many({"sportKey": SPORT_KEY})
        print(f"🗑️  Deleted {deleted.deleted_count} existing football assets")

        now = utc_now_iso()
        assets = []

        for c in clubs:
            assets.append({
                "id": str(uuid.uuid4()),
                "sportKey": SPORT_KEY,

                # Common identity fields
                "name": c.get("name"),
                "externalId": c.get("id"),  # stable link back to club doc

                # Competition metadata (helps when auction filters by competition)
                # Keep BOTH forms because codebases often vary
                "competitionCode": COMPETITION_CODE,          # "CL"
                "competitionShort": COMPETITION_SHORT,        # "UCL"
                "competitions": [COMPETITION_NAME],           # ["UEFA Champions League"]

                # Common filter flags / type hints
                "assetType": "club",
                "isActive": True,

                # Optional UI fields
                "country": c.get("country"),
                "logo": c.get("logo"),

                # Preserve your meta blob too
                "meta": {
                    "country": c.get("country"),
                    "uefaId": c.get("uefaId"),
                    "source": "clubs",
                },

                "createdAt": now,
                "updatedAt": now,
            })

        res = await db.assets.insert_many(assets)
        print(f"✅ Inserted {len(res.inserted_ids)} football assets")

        # Sanity checks
        count = await db.assets.count_documents({"sportKey": SPORT_KEY})
        count_ucl = await db.assets.count_documents({"sportKey": SPORT_KEY, "competitionShort": COMPETITION_SHORT})
        print(f"📦 assets(sportKey=football): {count}")
        print(f"🏆 assets(football, competitionShort={COMPETITION_SHORT}): {count_ucl}")

    finally:
        client.close()

if __name__ == "__main__":
    asyncio.run(main())
