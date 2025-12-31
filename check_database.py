import os
from sqlalchemy import create_engine, text

# ---------------------------------------------------------
# Configuration: Database Connection Settings
# These must match the settings defined in docker-compose.yml
# ---------------------------------------------------------
DB_USER = "admin"
DB_PASS = "my_secure_password"
DB_NAME = "blockchain_gold"
DB_HOST = "localhost"
DB_PORT = "5432"

# Construct the PostgreSQL connection string
db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

print("--- Connecting to Gold Layer Database (PostgreSQL)... ---")

try:
    # Initialize database engine
    engine = create_engine(db_url)
    
    # Establish connection
    with engine.connect() as conn:
        # Step 1: Verify if the 'transactions' table exists
        # This prevents errors if Spark hasn't created the table yet
        result = conn.execute(text("SELECT to_regclass('public.transactions');"))
        
        if result.scalar() is None:
            print("❌ Table 'transactions' does not exist yet.")
            print("   (Spark might still be initializing or downloading drivers. Please wait...)")
        else:
            print("✅ Table found! Fetching the latest records...")
            
            # Step 2: Query the last 10 transactions sorted by timestamp
            query = text("SELECT * FROM transactions ORDER BY timestamp DESC LIMIT 10;")
            result = conn.execute(query)
            rows = result.fetchall()
            
            if not rows:
                print("⚠️ The table exists but is currently empty.")
                print("   (Spark is running but hasn't processed/written any data yet.)")
            else:
                print(f"\n📊 Displaying the last {len(rows)} transactions stored in the DB:\n")
                
                # Step 3: Format and print the output
                # Adjust column indices based on your Spark schema order
                print(f"{'TX HASH':<20} | {'VALUE (ETH)':<12} | {'RISK LEVEL'}")
                print("-" * 60)
                
                for row in rows:
                    # row[0] -> tx_hash
                    # row[3] -> value_eth
                    # row[9] -> security_status
                    tx_short = row[0][:15] + "..."
                    val = round(row[3], 4)
                    risk = row[9]
                    
                    print(f"{tx_short:<20} | {val:<12} | {risk}")

except Exception as e:
    print(f"Connection Error: {e}")
    print("Ensure your Docker container is running and ports are mapped correctly.")