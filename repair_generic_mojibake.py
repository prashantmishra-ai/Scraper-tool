import argparse

from db import generic_collection, is_db_connected
from text_utils import normalize_text


TEXT_FIELDS = ("content_type", "extracted_data", "extra_info")


def main():
    parser = argparse.ArgumentParser(description="Repair mojibake in generic scraper MongoDB records.")
    parser.add_argument("--session-id", help="Only repair a single scraper session.")
    parser.add_argument("--apply", action="store_true", help="Write the fixes back to MongoDB.")
    args = parser.parse_args()

    if not is_db_connected():
        raise SystemExit("Database is not connected. Set MONGO_URI or start MongoDB, then run again.")

    query = {"session_id": args.session_id} if args.session_id else {}
    scanned = 0
    changed = 0

    for doc in generic_collection.find(query):
        scanned += 1
        updates = {}

        for field in TEXT_FIELDS:
            original = doc.get(field, "")
            repaired = normalize_text(original)
            if repaired != original:
                updates[field] = repaired

        if not updates:
            continue

        changed += 1
        print(f"{doc.get('_id')} -> {', '.join(sorted(updates))}")

        if args.apply:
            generic_collection.update_one({"_id": doc["_id"]}, {"$set": updates})

    mode = "applied" if args.apply else "dry-run"
    print(f"Finished {mode}: scanned {scanned} documents, found {changed} documents to update.")


if __name__ == "__main__":
    main()
