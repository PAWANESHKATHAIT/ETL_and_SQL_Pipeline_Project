
# ETL Pipeline and SQL Queries

## 1. ETL Pipeline Simulation

The task involves building a mini ETL pipeline to process product review data.

### Steps:
1. **Extract**: Load product reviews from a CSV file into a DataFrame.
2. **Transform**:
    - Remove reviews with null rating or review_text.
    - Filter ratings outside the range 1-5.
    - Assign sentiment labels based on review text.
3. **Load**: Output cleaned and enriched data to a CSV/JSON file.

### Technologies Used:
- Python (Pandas)

## 2. Data Deduplication

This task involves deduplicating a user data CSV based on email.

### Steps:
1. Load the CSV into a DataFrame.
2. Drop duplicate entries based on 'email' and keep the most recent one.
3. Save the cleaned data to a new file.

### Technologies Used:
- Python (Pandas)

## 3. Data Joins and Filtering (SQL)

### Problem:
Find all users who have made more than 3 orders in the last 90 days, along with their total spend.

### Query:
```sql
SELECT users.user_id, users.name, SUM(orders.amount) AS total_spent
FROM users
JOIN orders ON users.user_id = orders.user_id
WHERE orders.order_date > DATE('now', '-90 days')
GROUP BY users.user_id
HAVING COUNT(orders.order_id) > 3;
```

### Technologies Used:
- SQLite

## 4. Identify Data Gaps (SQL)

### Problem:
Find missing hourly readings in the past 24 hours for each sensor.

### Query:
```sql
WITH hours AS (
    SELECT datetime('now', '-24 hours') AS start_time, datetime('now') AS end_time
)
SELECT sensor_id, timestamp
FROM hours
LEFT JOIN sensor_logs ON timestamp BETWEEN start_time AND end_time AND sensor_logs.sensor_id = sensor_logs.sensor_id
WHERE sensor_logs.timestamp IS NULL;
```

### Technologies Used:
- SQLite

## 5. Customer Purchases & Memberships (SQL)

### Problem:
Find Gold members who have made total purchases over $250.

### Query:
```sql
SELECT purchases.customer_id, SUM(purchases.amount) AS total_spent, members.membership_level
FROM purchases
JOIN members ON purchases.customer_id = members.customer_id
WHERE members.membership_level = 'Gold'
GROUP BY purchases.customer_id
HAVING SUM(purchases.amount) > 250;
```

### Technologies Used:
- SQLite
