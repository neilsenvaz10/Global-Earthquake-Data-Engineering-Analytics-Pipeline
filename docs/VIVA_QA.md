# QuakeTrack viva questions and answers

1. **What is data engineering?** Building reliable systems that collect, organize,
   transform and deliver data for other people or applications to use.
2. **What does QuakeTrack do?** It ingests USGS earthquake observations, preserves
   their source responses, validates them, loads PostgreSQL and provides analytics.
3. **What is ETL?** Extract data from a source, transform it into a useful validated
   structure, and load it into a destination database.
4. **Why use an API?** It offers a documented machine-readable interface. USGS
   provides time, magnitude, coordinate and source-update information.
5. **What format does USGS return?** GeoJSON FeatureCollections. A point uses
   longitude, latitude and depth, so coordinate order must be handled carefully.
6. **Why preserve raw JSON?** It lets us inspect what the source returned, replay
   transformations and investigate errors without relying on a later API response.
7. **What does SHA-256 establish?** A content fingerprint detects whether archived
   bytes changed. It does not independently prove the scientific truth of the data.
8. **Why divide downloads into windows?** Smaller windows bound work, simplify
   retries and support resume. A response reaching the API cap is split further.
9. **How are time boundaries handled?** Internally ranges are start-inclusive and
   end-exclusive UTC. Because USGS uses inclusive endpoints, requests end one
   millisecond before the internal exclusive end.
10. **What is data validation?** Checking types, timestamps, required fields and
    coordinate ranges before accepting an event. Invalid events are quarantined.
11. **Why retain null magnitudes and depths?** They are optional source fields.
    Unknown is different from zero, and averages should ignore unknown values.
12. **Are negative magnitudes or depths invalid?** No. Small events can have
    negative magnitudes, and source depth conventions can yield negative depths.
    The pipeline preserves valid finite values rather than clipping them.
13. **What is deduplication?** Resolving repeated representations of the same
    logical event, identified by its stable USGS event ID.
14. **What is idempotency?** Repeating the same work has the same stored result.
    Repeated loading reports unchanged events rather than inserting duplicates.
15. **How do updates differ from duplicates?** A later source-update timestamp
    can replace an event's stored representation. Older or equal revisions cannot
    regress the current record.
16. **Why PostgreSQL?** It provides transactions, constraints, indexes, SQL
    analytics and reliable relational links between events and ingestion history.
17. **What is the primary key?** `earthquakes.event_id`, a unique nonempty USGS ID.
18. **What are foreign keys used for?** Linking event rows, runs, chunks, source
    archives and rejected events so provenance remains consistent.
19. **What is a transaction?** A unit of work that commits together or rolls back.
    Each persistence chunk commits events, provenance and counters atomically.
20. **Why use migrations?** They version schema changes explicitly. Checksums
    detect edits to migrations already applied to an existing database.
21. **What is a data warehouse, and is this one?** A warehouse organizes historical
    data for analysis, often across sources. This project is a small analytical
    pipeline over a curated PostgreSQL catalog, not a large enterprise warehouse.
22. **What is Airflow?** An orchestrator that tracks task dependencies, scheduling,
    retries and run history. It runs ETL code rather than storing earthquake data
    in its own metadata database.
23. **What is a DAG?** A directed acyclic graph. Here the tasks plan a window,
    ingest and check quality, then record the outcome.
24. **Why is the DAG paused?** Recurring ingestion needs explicit approval. The
    configured hourly schedule does not mean it is currently running automatically.
25. **What does `catchup=False` do?** It prevents the scheduler from automatically
    creating every historical scheduled interval. Application-level incremental
    catch-up is separately bounded.
26. **What is incremental ingestion?** Processing a bounded new time range and
    a recent overlap rather than downloading the entire catalog each time.
27. **Why overlap incremental windows?** Recently published events can be revised.
    Re-reading an overlap finds those changes; stable IDs prevent duplicate rows.
28. **When does a checkpoint advance?** Only after database loading and post-load
    quality checks succeed. A failed attempt must not falsely report coverage.
29. **How are API failures handled?** Timeouts and bounded retries handle transient
    problems. Permanent failures are recorded and surfaced, not silently ignored.
30. **How does the dashboard access data?** A separate read-only layer builds bound
    PostgreSQL queries from a shared filter object; Streamlit renders Plotly figures
    and tables. It does not load raw archives as a fallback.
31. **What prevents SQL injection?** User values are bound parameters. Dynamic
    sort columns, intervals and measurements come from explicit allowlists.
32. **How do you keep dashboard memory bounded?** SQL handles filtering and
    aggregation. Tables are paginated, map/scatter reads are capped at 5,000,
    and complete CSV selections must contain no more than 10,000 events.
33. **Why cache results?** It avoids repeated database work on widget reruns.
    Results expire after 60 seconds; Refresh data clears them immediately.
34. **What does read-only mean here?** Dashboard connections default to read-only
    transactions, and queries explicitly request that mode. Integration tests
    verify PostgreSQL rejects a DELETE through that connection. The underlying
    local ingestion role still has write permissions for the separate ETL.
35. **How are tests isolated?** Offline tests use explicit mocks. Opt-in PostgreSQL
    tests create a new uniquely named database and reset only synthetic fixtures
    there, preserving the development database and its real source records.
36. **How do you test Streamlit?** AppTest runs the actual entry point and widget
    interactions. Offline tests cover empty/error states; a separate live check
    renders every view against real database results and verifies filters and CSV.
37. **What does freshness mean?** Event occurrence, source revision, stored-row
    ingestion, workflow completion and coverage checkpoint age measure different
    things. A successful historical replay does not make old observations recent.
38. **Does successful database history prove the scheduler is live?** No. A
    scheduler can be stopped or its DAG paused. The dashboard labels application
    history explicitly and links to Airflow for current scheduler state.
39. **Can place descriptions identify countries reliably?** No. They are free
    text and can describe oceans or distances from landmarks. We do not invent
    normalized country fields from them.
40. **What are the main limitations?** The verified catalog has 40 events on two
    dates, uses different magnitude thresholds, and is incomplete. Mixed magnitude
    scales complicate comparison, basemap tiles require internet, and no forecasts
    or hazard conclusions are supported.

41. **What is medallion architecture?** A way to organize raw data as Bronze,
    validated data as Silver, and analytical outputs as Gold. Here it is a logical
    mapping onto existing files, a PostgreSQL table and ordinary SQL views.
42. **Why not copy all data into three directories?** The existing archives and
    curated table already serve those roles. Logical layers avoid unnecessary
    duplicate storage; the optional small Spark snapshot is a reproducibility artifact.
43. **Are these layers Delta Lake or Iceberg?** No. Those are specific table
    formats and technologies. This project uses local GeoJSON and PostgreSQL.
44. **What is a Spark DataFrame?** A distributed-capable table with a schema and
    named columns. This demo supplies explicit types and nullability instead of inference.
45. **Why use an explicit schema?** Small inputs or all-null columns can produce
    incorrect inference. Explicit timestamp/double/string types make behavior predictable.
46. **What are transformations and actions?** Transformations such as filter and
    groupBy build a lazy plan. Actions such as count and collect execute that plan.
47. **What does local[2] mean?** Spark executes on one machine using two local
    execution threads. It demonstrates Spark APIs without a distributed cluster.
48. **What does withColumn do here?** It derives UTC event dates and magnitude
    categories while retaining the original measurements, including negatives and nulls.
49. **What is Spark SQL?** SQL executed by Spark over DataFrames registered as
    temporary views. The example groups events by UTC month and computes counts and averages.
50. **How do you establish Spark/SQL parity?** Export Silver rows and PostgreSQL
    Gold aggregates in the same repeatable-read snapshot. Compare group keys/counts,
    nulls and averages, allowing tiny floating-point rounding differences.
51. **Why is Spark optional?** PostgreSQL and Python handle this sample efficiently.
    Spark is a syllabus demonstration; neither Airflow ingestion nor Streamlit needs it.
52. **What is a data contract?** A versioned agreement about field names, types,
    required values, UTC, valid ranges, uniqueness, nulls, updates and provenance.
53. **Why are raw numeric strings accepted but processed strings rejected?**
    Normalization may convert source representations. Silver is expected to have
    already completed that conversion, so its contract requires numeric values.
54. **How is the compliance percentage calculated?** Valid supplied processed
    rows divided by all supplied processed rows. Invalid fields/provenance and all
    rows in repeated-ID groups fail compliance. Empty input has no percentage.
55. **Is 100% Silver compliance the same as no source problems?** No. Rejected
    source features may already have been quarantined. This report measures the
    supplied curated rows, not every raw source feature or catalog completeness.
56. **What does governance mean in this project?** Clear ownership/source, documented
    field policies, immutable provenance, reproducibility, quality evidence and controlled access.
57. **How is PostgreSQL least privilege applied?** Streamlit uses a separate login
    with only required reads. It receives no ETL password and cannot modify permanent
    project tables, even if client read-only mode is turned off.
58. **Why test SQLSTATE 42501?** It is PostgreSQL's insufficient-privilege result.
    It proves ACL denial, rather than relying only on a client transaction setting.
59. **Why keep ETL credentials separate?** ETL must write validated observations;
    a dashboard needs only reads. Separate identities reduce the dashboard's authority.
60. **Why use ordinary Gold views?** The dataset is small. Views remain current
    automatically and avoid extra storage and materialized-view refresh scheduling.
