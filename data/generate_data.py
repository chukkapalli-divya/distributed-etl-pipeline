import pandas as pd
import numpy as np

np.random.seed(42)
n = 1000

regions = ["North", "South", "East", "West", "Central"]
categories = ["Electronics", "Clothing", "Food", "Health", "Sports"]
statuses = ["completed", "pending", "cancelled", "refunded"]

df = pd.DataFrame({
    "transaction_id": [f"TXN{str(i).zfill(7)}" for i in range(1, n + 1)],
    "user_id": np.random.randint(1000, 9999, n),
    "product_id": [f"PROD{np.random.randint(100, 999)}" for _ in range(n)],
    "category": np.random.choice(categories, n),
    "region": np.random.choice(regions, n),
    "amount": np.round(np.random.uniform(5.0, 500.0, n), 2),
    "quantity": np.random.randint(1, 20, n),
    "status": np.random.choice(statuses, n, p=[0.75, 0.10, 0.10, 0.05]),
    "date": pd.date_range("2024-01-01", periods=n, freq="1h").strftime("%Y-%m-%d"),
    "timestamp": pd.date_range("2024-01-01", periods=n, freq="1h").strftime("%Y-%m-%d %H:%M:%S"),
})

# Inject nulls to simulate real-world dirty data
null_indices = np.random.choice(n, 30, replace=False)
df.loc[null_indices[:10], "amount"] = None
df.loc[null_indices[10:20], "region"] = None
df.loc[null_indices[20:], "category"] = None

# Inject duplicates
duplicates = df.sample(20, random_state=42)
df = pd.concat([df, duplicates], ignore_index=True)

df.to_csv("data/sample_data.csv", index=False)
print(f"Generated {len(df)} rows including nulls and duplicates")
