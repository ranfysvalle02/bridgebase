# bridgebase

---

**MongoDB as the “Man in the Middle”**  
Ever hear someone say, “Oh, Mongo’s fine, but don’t try to treat it like a SQL database—it’ll be slow!”? This project puts that claim to the test by taking an SQL-like query, parsing it in Python, running that translated query in MongoDB, and comparing performance with a real Postgres query. The surprising result is that Mongo remains quite competitive—even when acting as a “man in the middle,” translating SQL to a Mongo `.find()` behind the scenes.

---

### The Experiment
A small Flask app is set up to take in a query like `SELECT * FROM users`, call a Ray actor, and fire off two tasks in parallel:

1. **Mongo-in-the-middle**:  
   - Parses the SQL string (using `sqlparse`) to figure out the `FROM`, `WHERE`, `LIMIT`, etc.  
   - Translates that into Mongo’s query language (`db.collection.find()`).  

2. **Native Postgres**:  
   - Sends the same SQL string directly to PostgreSQL.  

Both tasks run simultaneously, and the total time is measured along with individual runtimes.

---

### Sample Results
For a test with 100,000 rows/documents, the output might look like this:

```json
{
  "mongo_in_the_middle_count": 100000,
  "mongo_in_the_middle_time": 0.07788958399999046,
  "native_postgres_count": 100000,
  "native_postgres_time": 0.03617708299998412,
  "total_parallel_time": 0.2251650000000609
}
```

Postgres is faster (0.036s vs. Mongo’s 0.078s), but it’s still only about twice as fast—not an order-of-magnitude difference. Even with half a million rows:

```json
{
  "mongo_in_the_middle_count": 500000,
  "mongo_in_the_middle_time": 0.43239766699980464,
  "native_postgres_count": 500000,
  "native_postgres_time": 0.15205995899987101,
  "total_parallel_time": 1.1581368340000608
}
```

Mongo remains surprisingly competitive. It’s being forced to parse and translate an SQL string, and yet it keeps up reasonably well.

---

### Why It Matters
- **Mongo Overhead Isn’t Extreme**  
  Converting SQL to a Mongo query does add some processing time, but the measured gap is relatively small.

- **Parallel Execution**  
  Ray tasks handle both queries at once. The total time is roughly the duration of the slower query plus a little overhead, which is beneficial when both databases need to be queried for the same request.

- **Real-World Impact**  
  A few hundred milliseconds difference is often acceptable in practical apps—especially with caching or indexing. If there’s a desire to maintain a consistent SQL interface for multiple data stores, Mongo can still deliver decent performance in this setup.

Overall, MongoDB proves it can keep pace—even when forced to do extra work to handle SQL translations. It won’t dethrone Postgres as the king of relational queries, but it’s not as far behind as some might assume. Despite the additional overhead of parsing and translating SQL commands into MongoDB's query language, the database demonstrates competitive speeds, closely trailing PostgreSQL in various test scenarios.