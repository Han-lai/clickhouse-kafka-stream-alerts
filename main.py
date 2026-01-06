from generate_fake_data import generate_fake_data
from setup_clickhouse import setup_clickhouse

def main():
    print("Starting the Kafka-ClickHouse Alert Project...")
    
    # Step 1: Generate fake data
    generate_fake_data()
    
    # Step 2: Set up ClickHouse
    setup_clickhouse()
    
    # Add more steps as needed
    print("All tasks completed.")

if __name__ == "__main__":
    main()