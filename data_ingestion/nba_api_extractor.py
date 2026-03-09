import pandas as pd
from datetime import datetime
from nba_api.stats.endpoints import leaguedashplayerstats


def fetch_player_stats(season="2025-26"):
    """
    Fetch player statistics for a given NBA season.

    Args:
        season (str): NBA season format (e.g., "2023-24")

    Returns:
        pandas.DataFrame: Player statistics dataframe
    """

    print(f"[INFO] Fetching NBA player stats for season {season}...")

    stats = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season,
        season_type_all_star="Regular Season"
    )

    df = stats.get_data_frames()[0]

    print(f"[INFO] Retrieved {len(df)} player records")

    return df


def clean_player_stats(df):
    """
    Clean and standardize player statistics.

    Args:
        df (DataFrame): Raw player stats

    Returns:
        DataFrame: Cleaned stats
    """

    df = df.copy()

    df.columns = df.columns.str.lower()

    df["ingestion_timestamp"] = datetime.utcnow()

    selected_columns = [
        "player_id",
        "player_name",
        "team_abbreviation",
        "gp",
        "pts",
        "ast",
        "reb",
        "stl",
        "blk",
        "min",
        "ingestion_timestamp"
    ]

    df = df[selected_columns]

    return df


def save_to_csv(df, filename="player_stats.csv"):
    """
    Save dataframe locally for debugging or batch processing.
    """

    df.to_csv(filename, index=False)
    print(f"[INFO] Data saved to {filename}")


def main():
    """
    Main extraction pipeline
    """

    raw_df = fetch_player_stats()

    clean_df = clean_player_stats(raw_df)

    save_to_csv(clean_df)

    print("[INFO] Extraction complete")
    print(clean_df.head())


if __name__ == "__main__":
    main()