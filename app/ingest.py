# GOAL: endsong_*.json (in data/raw) -> data/listens.parquet
# DONE WHEN: listens.parquet exists with played_at, track_name, artist_name, album_name, ms_played, spotify_track_uri

import os, json, glob, pandas as pd

def main():
    files = glob.glob("data/raw/endsong*.json")
    if not files:
        print("No data/raw/endsong_*.json found"); return

    rows = [x for f in files for x in json.load(open(f, "r"))]
    df = pd.DataFrame(rows)

    df = pd.DataFrame({
        "played_at": pd.to_datetime(df["ts"], utc=True, errors="coerce"),
        "track_name": df.get("master_metadata_track_name"),
        "artist_name": df.get("master_metadata_album_artist_name"),
        "album_name": df.get("master_metadata_album_album_name"),
        "ms_played": df.get("ms_played"),
        "spotify_track_uri": df.get("spotify_track_uri")
    }).dropna(subset=["played_at"]).sort_values("played_at")

    os.makedirs("data", exist_ok=True)
    df.to_parquet("data/listens.parquet", index=False)
    print(f"saved {len(df):,} rows")

if __name__ == "__main__":
    main()
