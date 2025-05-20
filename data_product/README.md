## Data Product Documentation
This document was written as if it were for a real production environment, providing all the necessary information for users to understand, trust, and effectively consume the data product.

## Data Product: `cdi_payout`
This data product provides the **daily CDI bonus payout** for eligible accounts. It is designed to run automatically every day and generate the amount of CDI interest to be credited to each user's wallet.

**Table Schema**
|Column Name|Type|Description|
|--|--|--|
|date|DATE|The reference date when the interest is calculated and paid out.|
|account_id|STRING|Unique identifier of the account receiving the CDI interest.|
|cdi_interest_value|DOUBLE|The amount of CDI interest (in currency) to be deposited into the account.|

## **Business Logic**

**Source Data** - The logic uses two curated source tables:

- `wallet_history`: Provides the daily balance for each account.
- `cdi_interest_history`: Contains the official daily CDI interest rate.

**Eligibility and Calculation:**

  For each account, the pipeline identifies the portion of the wallet balance that:

- Exceeds R$100 and
- Has remained unmoved for at least 24 hours.

 Then, this eligible amount is then multiplied by the corresponding CDI interest rate for that day.

 The result is the `cdi_interest_value`, which represents the bonus to be credited to the account.

**Execution Schedule:**

- The pipeline will run every day at 03:00:00 UTC

**Data Quality and Validations**

To ensure the reliability of this mission-critical output, the following data quality checks are applied:-

- Null checks: Ensures no record has missing values in all fields (date, account_id, cdi_interest_value).
- Value checks: Ensures that cdi_interest_value is always greater than or equal to 0.

Any record that fails these checks is broke the pipeline and alerts will be made available to product owners.

**Data Product Owners:** 
- João Silva
- Maria 
- Pedro