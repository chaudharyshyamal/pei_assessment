### PEI LDE Assessment

- Point 5 in assessment says to write in SQL, but in note it says use PySpark, so I have written snippet for point 5 in both SQL and PySpark.
- I have kept all columns, as use case is not limited to the mentioned use cases, as the assessment is open ended, we should keep all columns, once final set of KPIs are decided we can remove unwanted columns. This will help in making the logic efficient in terms of time and cost.
- Select required columns and do the filtering, then join, this will lead to good performance.
- As caching is not possible in Free Edition of Databricks, I have not done that, it will give major performance improvement. Like final joined dataframe should be cachedm, as it's being used multiple times.
- Once the final set of KPIs and data size is known, we can decide the joining strategy, wether to use Sort Merge Join or Shuffle Hash Join.