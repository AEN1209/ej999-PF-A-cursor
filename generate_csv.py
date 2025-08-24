
import csv
import random
from datetime import datetime, timedelta

def generate_random_date(start_year, end_year):
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    time_difference = end_date - start_date
    random_days = random.randint(0, time_difference.days)
    return start_date + timedelta(days=random_days)

def generate_csv(filename="sales_data.csv", num_records=2000000):
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ["ID", "order_id", "customer_id", "total", "fecha"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for i in range(num_records):
            writer.writerow({
                "ID": i + 1,
                "order_id": random.randint(100000, 999999),
                "customer_id": random.randint(1000, 9999),
                "total": round(random.uniform(10.0, 1000.0), 2),
                "fecha": generate_random_date(2020, 2023).isoformat()
            })
    print(f"Generated {num_records} records in {filename}")

if __name__ == "__main__":
    generate_csv()
