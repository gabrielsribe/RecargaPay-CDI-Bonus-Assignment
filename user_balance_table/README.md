## Daily Balance Calculation

### Purpose

The goal of this module is to compute the daily balance per `account_id` using raw transactional events provided in a Change Data Capture (CDC) format. This is a foundational step for the CDI Bonus calculation, ensuring a reliable historical view of account balances, even for days with no transactions.

---

### Input Schema

The input dataset contains financial events with the following columns:

| Column Name        | Type             | Description |
|--------------------|------------------|-------------|
| `event_time`       | `timestamp`      | Timestamp of the transaction |
| `user_id`          | `string`         | User identifier (not used here) |
| `account_id`       | `string`         | Account identifier |
| `amount`           | `decimal(10,2)`  | Transaction amount (positive = credit, negative = debit) |
| `transaction_type` | `string`         | Transaction category (not used here) |
| `cdc_operation`    | `string`         | CDC operation (`insert`, `update`, `delete`) |
| `cdc_sequence_num` | `long`           | Event sequence for change tracking |
| `source_system`    | `string`         | Source of the transaction (not used) |

---

### Processing Logic

1. **Filter Valid Events**
   - Include only `insert` and `update` operations from the CDC.
   - Ignore `delete` operations, as financial flows are cumulative.

2. **Extract Event Date**
   - Derive the `event_date` by truncating the `event_time` to a date.

3. **Aggregate Daily Movements**
   - Group by `account_id` and `event_date`, summing all `amount`s.
   - Credits (positive) and debits (negative) are included.

4. **Create a Full Calendar**
   - Generate a date range from the minimum to maximum `event_date` found in the data.

5. **Build All Account-Date Combinations**
   - Use a cross join between all `account_id`s and the calendar to ensure full coverage.

6. **Fill Missing Data**
   - Join with the transaction data and fill missing `daily_amount`s with `0.00`.

7. **Calculate Daily Balance**
   - Apply a cumulative window function sum() per `account_id`, ordered by `event_date`.

---

### Output Schema

| Column Name     | Type            | Description |
|------------------|-----------------|-------------|
| `account_id`     | `string`        | Account identifier |
| `event_date`     | `date`          | Reference date |
| `daily_amount`   | `decimal(10,2)` | Net change in balance for the day |
| `daily_balance`  | `decimal(10,2)` | Accumulated balance up to that day |

---

### Notes

- This pipeline delivers complete temporal continuity of balances, even when no transactions occur on a given day.
- CDC rows are treated as independent balance-changing events.
- The result is a clean daily record of balances, suitable for applying business logic such as CDI interest eligibility checks.
- This dataset will be used in the following CDI interest calculation
