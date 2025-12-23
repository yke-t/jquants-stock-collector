"""
J-Quants Stock Data Collector - Main Entry Point

Usage:
    python main.py --start 2014-01-01
    python main.py --start 2024-01-01 --end 2024-12-23
    python main.py --start 2024-01-01 --no-resume
"""

import argparse
import os
import sys
from dotenv import load_dotenv

from src.client import JQuantsClient
from src.collector import DataCollector
from src.database import StockDatabase


def main():
    """Main entry point"""
    # Load environment variables
    load_dotenv()
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="J-Quants Stock Data Collector")
    parser.add_argument(
        "--start", 
        type=str, 
        default="2014-01-01",
        help="Start date (YYYY-MM-DD, default: 2014-01-01)"
    )
    parser.add_argument(
        "--end", 
        type=str, 
        default=None,
        help="End date (YYYY-MM-DD, default: today)"
    )
    parser.add_argument(
        "--db", 
        type=str, 
        default="stock_data.db",
        help="Database file path (default: stock_data.db)"
    )
    parser.add_argument(
        "--no-resume", 
        action="store_true",
        help="Do not resume from last sync point"
    )
    args = parser.parse_args()

    # Check authentication (V2 uses API key)
    api_key = os.getenv("JQUANTS_API_KEY") or os.getenv("JQUANTS_REFRESH_TOKEN")
    
    if not api_key:
        print("Error: Set JQUANTS_API_KEY in .env")
        print("  1. Copy .env.example to .env")
        print("  2. Get your API key from https://jpx-jquants.com/")
        print("  3. Set JQUANTS_API_KEY=your_api_key")
        sys.exit(1)

    print("=" * 50)
    print("J-Quants Stock Data Collector")
    print("=" * 50)
    print(f"Start date: {args.start}")
    print(f"End date: {args.end or 'today'}")
    print(f"Database: {args.db}")
    print(f"Resume: {not args.no_resume}")
    print("=" * 50)
    
    try:
        # 1. Initialize API client
        print("[INIT] Connecting to J-Quants API...")
        client = JQuantsClient()
        
        # 2. Initialize database
        print(f"[INIT] Opening database: {args.db}")
        db = StockDatabase(args.db)
        
        # 3. Create collector and run
        collector = DataCollector(client, db)
        collector.run(
            start_date=args.start,
            end_date=args.end,
            resume=not args.no_resume
        )
        
    except KeyboardInterrupt:
        print("\n\n[INFO] Interrupted by user. Progress saved.")
        print("[INFO] Run again to resume.")
        sys.exit(0)
    except Exception as e:
        print(f"\n[FATAL] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()