# CS570 Big Data Processing - Complete Study Notes

**Course:** CS570 - Big Data Processing
**Semester:** Spring 2026
**Instructor:** Dr. Ragnar Lesch

---

# Table of Contents

1. [Environment Setup](#1-environment-setup)
2. [PySpark Fundamentals](#2-pyspark-fundamentals)
3. [HDFS - Hadoop Distributed File System](#3-hdfs---hadoop-distributed-file-system)
4. [MapReduce Concepts](#4-mapreduce-concepts)
5. [Spark Execution & Optimization](#5-spark-execution--optimization)
6. [Lab Exercises & Answers](#6-lab-exercises--answers)
7. [Key Comparisons](#7-key-comparisons)

---

# 1. Environment Setup

## 1.1 Tools Installed

| Tool | Version | Location | Purpose |
|------|---------|----------|---------|
| Python 3.11 | 3.11.x | Windows (venv_CS570) | PySpark programming |
| Java 17 | Temurin 17.0.13 | Windows | PySpark runtime |
| Java 11 | Adopt OpenJDK 11 | Windows + WSL2 | Hadoop & Scala |
| PySpark | 3.5.3 | Windows + WSL2 | Spark Python API |
| Scala | 3.7.4 | Windows | Functional programming |
| sbt | 1.x | Windows | Scala build tool |
| Hadoop | 3.4.2 | WSL2 Ubuntu | Distributed storage (HDFS) |
| Jupyter Lab | Latest | WSL2 Ubuntu | Run lab notebooks |

## 1.2 Windows Setup

### Python Virtual Environment
```bash
# Create virtual environment
python -m venv venv_CS570

# Activate (PowerShell)
venv_CS570\Scripts\Activate.ps1

# Deactivate
deactivate

# Install PySpark (use 3.5.3, NOT 4.x - has Java issues)
pip install pyspark==3.5.3
```

### Java 17 (Eclipse Temurin)
- Downloaded `.msi` from Adoptium website
- Check "Add to PATH" and "Set JAVA_HOME" during install
- Verify: `java -version`

### Scala (via Coursier)
```bash
# Install coursier, then:
cs setup --jvm adopt:11
# Installs: Java 11, Scala, sbt
# Verify: scala -version, sbt --version
```

## 1.3 WSL2 Ubuntu Setup (for Hadoop)

### Why WSL2?
- Hadoop is designed for Linux, doesn't work properly in Windows
- WSL2 gives you a real Linux environment inside Windows
- Think of it as a lightweight Linux virtual machine

### Install WSL2
```powershell
# In PowerShell as Administrator
wsl --install
# Restart computer, then set up Ubuntu username/password
```

### Install Hadoop in Ubuntu
```bash
# Update packages
sudo apt update

# Install Java 11
sudo apt install openjdk-11-jdk -y

# Download and install Hadoop
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz
tar -xzvf hadoop-3.4.2.tar.gz
sudo mv hadoop-3.4.2 /usr/local/hadoop

# Set environment variables
echo 'export HADOOP_HOME=/usr/local/hadoop' >> ~/.bashrc
echo 'export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin' >> ~/.bashrc
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh
source ~/.bashrc

# Verify
hadoop version
```

### Configure HDFS
```bash
# core-site.xml - tells Hadoop to use HDFS at localhost:9000
cat > $HADOOP_HOME/etc/hadoop/core-site.xml << 'XMLEOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
XMLEOF

# hdfs-site.xml - sets replication to 1 (single machine)
cat > $HADOOP_HOME/etc/hadoop/hdfs-site.xml << 'XMLEOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
</configuration>
XMLEOF
```

### Setup SSH (required for HDFS)
```bash
sudo apt install openssh-server -y
sudo service ssh start
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
ssh localhost  # type "yes", then "exit"
```

### Start HDFS
```bash
hdfs namenode -format    # ONLY ONCE! Erases all data if run again
start-dfs.sh             # Start HDFS
jps                      # Verify: NameNode, DataNode, SecondaryNameNode
```

### Python Virtual Environment in Ubuntu
```bash
sudo apt install python3-venv python3-full -y
python3 -m venv ~/hadoop_env
source ~/hadoop_env/bin/activate
pip install jupyterlab pyspark==3.5.3 pandas
jupyter lab --no-browser
```

## 1.4 Architecture Overview

```
+-----------------------------------------------------------+
|                      WINDOWS                              |
|  +------------------+    +--------------------------+     |
|  | VSCode           |    | Browser                  |     |
|  | - Scala code     |    | - Jupyter Lab            |     |
|  | - Python code    |    |   (localhost:8888)        |     |
|  +------------------+    +--------------------------+     |
|                                   |                       |
|  +-----------------------------------------------------+ |
|  |              WSL2 (Ubuntu Linux)                     | |
|  |  +------------+  +------------+  +--------------+   | |
|  |  | Terminal 1 |  | Terminal 2 |  | Hadoop HDFS  |   | |
|  |  | Jupyter    |  | jps, hdfs  |  | Running      |   | |
|  |  | Lab Server |  | commands   |  |              |   | |
|  |  +------------+  +------------+  +--------------+   | |
|  +-----------------------------------------------------+ |
+-----------------------------------------------------------+
```

---

# 2. PySpark Fundamentals

## 2.1 SparkSession - Entry Point

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \       # Run locally, all CPU cores
    .appName("cs570-week1") \   # Name your app
    .getOrCreate()              # Create new or reuse existing

# Test Spark is working
spark.range(10).count()  # Returns: 10
```

| Part | Meaning |
|------|---------|
| `.builder` | Start building session |
| `.master("local[*]")` | Run locally, `*` = all cores |
| `.appName("name")` | App name (shows in Spark UI) |
| `.getOrCreate()` | Create or reuse session |

## 2.2 Spark Configuration

```python
# Auto-display DataFrames in notebooks (no need for .show())
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

# Limit preview to 5 rows
spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 5)

# Show up to 100 columns
spark.conf.set("spark.sql.debug.maxToStringFields", 100)

# Pandas display settings
import pandas as pd
pd.set_option("display.max_columns", None)     # Show all columns
pd.set_option("display.width", 200)             # Wider output
pd.set_option("display.max_colwidth", None)     # Don't truncate text
```

## 2.3 Transformations vs Actions

### Transformations (LAZY - nothing executes until action)

| Transformation | What It Does |
|---------------|-------------|
| `select()` | Select columns |
| `filter()` / `where()` | Filter rows |
| `groupBy()` | Group data |
| `join()` | Join DataFrames |
| `orderBy()` | Sort data |
| `withColumn()` | Add/modify column |
| `map()` | Transform each element |

### Actions (EAGER - execute immediately, return results)

| Action | What It Does |
|--------|-------------|
| `count()` | Count rows |
| `show()` | Display data |
| `collect()` | Return all data to driver |
| `take(n)` | Return first n rows |
| `first()` | Return first row |
| `write()` | Save to disk |

### Example
```python
# Transformations (LAZY - nothing happens yet)
df2 = df.filter(df.country == "US")
df3 = df2.select("user_id", "page")

# Action (EAGER - NOW Spark executes everything)
df3.count()  # This triggers the actual computation
```

## 2.4 Lazy Evaluation

**Definition:** Spark doesn't execute transformations immediately. It waits until an action is called, then executes everything at once.

```python
# Step 1: Spark just RECORDS this (no execution)
df2 = df.filter(df.country == "US")

# Step 2: Spark just RECORDS this too (still no execution)
df3 = df2.select("user_id", "page")

# Step 3: ACTION! Now Spark executes ALL steps above
df3.count()
```

**Why Lazy Evaluation?**

| Benefit | Explanation |
|---------|-------------|
| Optimization | Spark rearranges operations for efficiency |
| Reduced passes | Combines multiple operations into one pass |
| Skip unnecessary work | Only loads columns/rows that are needed |
| Fault tolerance | Can rebuild data by replaying the plan |

**Analogy:** Like writing a shopping list (transformations) and making ONE trip to the store (action), instead of going to the store for each item separately.

## 2.5 Reading Data

```python
# Read from local file
df = spark.read.csv("weather_data.csv", header=True, inferSchema=True)

# Read from HDFS
df = spark.read.csv("hdfs://localhost:9000/user/fsehaye/week2/weather_data.txt",
                     header=False, inferSchema=True)

# Rename columns
df = df.toDF("city", "date", "temperature")
```

## 2.6 Aggregations

```python
from pyspark.sql import functions as F

# Single aggregation
max_temps = df.groupBy("city").agg(
    F.max("temperature").alias("max_temperature")
)

# Multiple aggregations
multi_metrics = df.groupBy("city").agg(
    F.max("temperature").alias("max_temp"),
    F.min("temperature").alias("min_temp"),
    F.avg("temperature").alias("avg_temp"),
    F.count("temperature").alias("num_days")
)
```

## 2.7 Caching

```python
# Cache DataFrame in memory
df_cached = df.cache()

# First execution: reads from disk + caches in RAM
df_cached.groupBy("city").agg(F.max("temperature")).show()  # Slower

# Second execution: reads from RAM (cached)
df_cached.groupBy("city").agg(F.max("temperature")).show()  # Faster!
```

**Key Insight:**
- First run: Reads from HDFS (disk) - slow
- Second run: Reads from RAM (cached) - fast (10-100x)
- This is Spark's main advantage over Hadoop MapReduce

---

# 3. HDFS - Hadoop Distributed File System

## 3.1 What is HDFS?

A distributed file system that:
- Stores files across multiple machines (nodes)
- Breaks files into **blocks** (default 128MB each)
- **Replicates** blocks for fault tolerance (default 3 copies)
- Designed for big data (petabytes)

```
Regular File System:              HDFS (Distributed):
+-------------+                   +-------------+
|  file.txt   |                   |  file.txt   |
|  (1 copy)   |                   |  Block 1 --> Node A, Node B, Node C
|  (1 machine)|                   |  Block 2 --> Node B, Node C, Node D
+-------------+                   |  Block 3 --> Node A, Node C, Node D
                                  +-------------+
                                  (3 copies of each block on different machines)
```

## 3.2 HDFS Architecture

```
+------------------+
|    NameNode      |  METADATA (file -> block mapping)
|                  |  Knows where every block is stored
|  file.txt:       |  Single point of failure!
|   Block1 -> A,B  |
|   Block2 -> B,C  |
+------------------+
        |
   +----+----+----+
   |         |    |
+------+ +------+ +------+
|Data  | |Data  | |Data  |
|Node A| |Node B| |Node C|  ACTUAL DATA (block files)
|Block1| |Block1| |Block2|
|      | |Block2| |      |
+------+ +------+ +------+
```

**Key Components:**
- **NameNode:** Manages metadata (file-to-block mapping). Single point of failure.
- **DataNode:** Stores actual data blocks. Can have many in a cluster.
- **SecondaryNameNode:** Creates checkpoints of NameNode metadata. NOT a backup.

## 3.3 HDFS Commands

| Command | Purpose | Example |
|---------|---------|---------|
| `-ls` | List directory | `hdfs dfs -ls /` |
| `-mkdir -p` | Create directory | `hdfs dfs -mkdir -p /user/fsehaye/week2` |
| `-put` | Upload to HDFS | `hdfs dfs -put file.txt /path/` |
| `-put -f` | Upload (overwrite) | `hdfs dfs -put -f file.txt /path/` |
| `-get` | Download from HDFS | `hdfs dfs -get /path/file.txt local.txt` |
| `-cat` | View file content | `hdfs dfs -cat /path/file.txt` |
| `-rm` | Delete file | `hdfs dfs -rm /path/file.txt` |
| `-rm -r` | Delete directory | `hdfs dfs -rm -r /path/folder` |
| `-stat` | File statistics | `hdfs dfs -stat "%b %o %r %n" /path/file` |
| `-setrep` | Change replication | `hdfs dfs -setrep 2 /path/file` |
| `fsck` | Check block health | `hdfs fsck /path/file -files -blocks -locations` |

### Stat Format Codes

| Code | Meaning | Example |
|------|---------|---------|
| `%b` | File size in bytes | 245 |
| `%o` | Block size | 134217728 (128MB) |
| `%r` | Replication factor | 1 |
| `%n` | File name | sample.txt |

## 3.4 Block Storage

### How HDFS Stores Files Physically

```
LOGICAL VIEW (what you see):
  /user/fsehaye/week2/sample.txt

PHYSICAL STORAGE (what's actually on disk):
  /tmp/hadoop-fsehaye/dfs/data/current/BP-.../current/finalized/subdir0/subdir0/
    +-- blk_1073741826           <-- Your actual data (raw content)
    +-- blk_1073741826_1002.meta <-- Checksum (data integrity)
```

**Why this abstraction?**
- Files can be larger than any single disk
- Blocks can be replicated independently
- Different blocks processed in parallel
- Only damaged blocks need re-replication (not entire files)
- Load balancing across nodes

### HDFS fsck Output Explained

```
/user/fsehaye/week2/sample.txt 245 bytes, replication=1, 1 block(s): OK
0. BP-369798605-...:blk_1073741826_1002 len=245 Live_repl=1
   [DatanodeInfoWithStorage[127.0.0.1:9866,...,DISK]]
```

| Field | Meaning |
|-------|---------|
| 245 bytes | File size |
| replication=1 | Desired copies |
| 1 block(s) | Number of blocks (small file = 1 block) |
| blk_1073741826 | Unique block ID |
| len=245 | Block size in bytes |
| Live_repl=1 | Actual copies that exist |
| 127.0.0.1:9866 | DataNode address |
| DISK | Storage type |

## 3.5 Replication

### Replication Factor Comparison

| Factor | Storage Cost | Fault Tolerance | Write Speed |
|--------|-------------|-----------------|-------------|
| 1x | 1 TB for 1 TB | None | Fastest |
| 2x | 2 TB for 1 TB | Survives 1 failure | Fast |
| 3x (default) | 3 TB for 1 TB | Survives 2 failures | Moderate |
| 4x | 4 TB for 1 TB | Survives 3 failures | Slower |

### Why 3x is the Default
- Survives 2 simultaneous node failures
- During repair after 1 failure, still 2 copies remain (safe)
- Rack-aware: 2 copies in one rack + 1 in another rack
- Cost-effective balance between safety and storage

### Under-Replication
When you set replication=2 on a single-machine setup:
- HDFS **wants** 2 copies (Target Replicas = 2)
- HDFS **has** 1 copy (only 1 DataNode exists)
- File is "under-replicated" - waiting for another DataNode

**Rule:** Each replica MUST be on a DIFFERENT DataNode.

### Automatic Re-Replication
```
Normal:          Failure:           Auto-Repair:
Node A: Copy 1   Node A: Copy 1    Node A: Copy 1
Node B: Copy 2   Node B: X DEAD    Node D: Copy 2 (NEW!)
Node C: Copy 3   Node C: Copy 3    Node C: Copy 3
```

## 3.6 Data Locality

**Definition:** Moving computation to data, not data to computation.

```
BAD: Move Data to Computation       GOOD: Move Computation to Data
Node A         Node B                Node A
+------+       +------+              +------------------+
| Data | ====> | CPU  |              | Data --> CPU     |
| 1 TB | Network| busy |              | Process locally! |
+------+       +------+              +------------------+
Slow! (hours)                        Fast! (seconds)
```

### Three Levels of Data Locality

| Priority | Level | Network Cost | Speed |
|----------|-------|-------------|-------|
| 1st | Node-local (same machine) | Zero | Fastest |
| 2nd | Rack-local (same rack) | Low | Fast |
| 3rd | Off-rack (different rack) | High | Slowest |

### Why Critical at Scale
- Moving 1 PB over network: ~9.3 DAYS
- Sending code to 1000 nodes: ~0.04 seconds
- Network becomes bottleneck without locality

## 3.7 Data Persistence

When HDFS services are stopped:
- HDFS commands fail ("Connection refused")
- But block files STILL EXIST on disk
- When services restart, HDFS rebuilds state from disk

## 3.8 Node Failure Scenarios (3-node cluster, replication=3)

| Scenario | Access Files? | Why? |
|----------|--------------|------|
| 1 DataNode stops | YES | 2 copies remain |
| 2 DataNodes stop | YES | 1 copy remains |
| All DataNodes stop | NO | No copies running |
| NameNode stops | NO | Metadata lost (can't find blocks) |

**Key Insight:** NameNode is the single point of failure. Data blocks exist on disk but can't be located without NameNode's metadata.

---

# 4. MapReduce Concepts

## 4.1 The Three Phases

```
INPUT DATA:
  San Francisco, 58
  New York, 32
  San Francisco, 62
  Los Angeles, 72
  New York, 28
  Los Angeles, 75

PHASE 1: MAP (Extract key-value pairs)
  (San Francisco, 58)
  (New York, 32)
  (San Francisco, 62)
  (Los Angeles, 72)
  (New York, 28)
  (Los Angeles, 75)

PHASE 2: SHUFFLE (Group by key)
  San Francisco -> [58, 62]
  New York      -> [32, 28]
  Los Angeles   -> [72, 75]

PHASE 3: REDUCE (Aggregate per group)
  San Francisco -> max(58, 62) = 62
  New York      -> max(32, 28) = 35
  Los Angeles   -> max(72, 75) = 75
```

## 4.2 In Spark Code

```python
# MAP:     each row extracts (city, temperature)
# SHUFFLE: groupBy("city") groups by key
# REDUCE:  max("temperature") finds max per group

max_temps = df.groupBy("city").agg(
    F.max("temperature").alias("max_temperature")
)
```

---

# 5. Spark Execution & Optimization

## 5.1 Execution Plan

```python
max_temps.explain(True)  # Shows all 4 plan levels
```

**Physical Plan (read bottom to top):**
```
HashAggregate (final max)           <-- STAGE 2: Final aggregation
  Exchange hashpartitioning         <-- SHUFFLE (stage boundary)
    HashAggregate (partial_max)     <-- STAGE 1: Local pre-aggregation
      Project [city, temperature]   <-- Select needed columns
        FileScan csv                <-- Read from HDFS
```

**Key optimization:** Spark does TWO aggregations:
1. **partial_max** (before shuffle) - compute local max per partition
2. **final max** (after shuffle) - combine partial results

This dramatically reduces data sent over the network.

## 5.2 Shuffle Phase

### What Is Shuffle?
Data redistribution across the network so all values with same key end up on the same machine.

### Why Most Expensive?
1. **Serialization** - Convert objects to bytes
2. **Disk write** - Write sorted data before sending
3. **Network transfer** - Send data across cluster
4. **Disk read** - Receiving node reads data
5. **Sort/merge** - Combine data from multiple sources

### Operations That CAUSE Shuffle

| Operation | Why |
|-----------|-----|
| `groupBy()` | Must collect all values for same key |
| `join()` | Must match records by key |
| `orderBy()` / `sort()` | Must compare across all partitions |
| `distinct()` | Must check all partitions for duplicates |
| `repartition()` | Explicitly redistributes data |

### Operations That DON'T Cause Shuffle

| Operation | Why |
|-----------|-----|
| `filter()` / `where()` | Each partition filters independently |
| `select()` | Just picks columns |
| `withColumn()` | Each row processed independently |
| `map()` | Each element transformed independently |
| `union()` | Just concatenates |

### Spark's Shuffle Optimizations vs Hadoop

| Optimization | Hadoop | Spark |
|-------------|--------|-------|
| Partial aggregation | Optional combiner | Automatic |
| Intermediate storage | Always disk | RAM when possible |
| Pipelining | No (separate jobs) | Yes (one DAG) |
| Partition adjustment | Fixed | Adaptive (AQE) |
| Serialization | Java (slow/large) | Tungsten (fast/compact) |

---

# 6. Lab Exercises & Answers

## Week 1 Lab: Spark Intro

### Key Code
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("cs570-week1").getOrCreate()
spark.range(10).count()  # Returns 10
```

## Week 2 Lab: HDFS & PySpark

### Part 1: HDFS Commands

**Q1: Replication Factor**
- Replication factor is 1 (configured in hdfs-site.xml)
- Set to 1 because single machine can only have 1 DataNode
- Default is 3 in production for fault tolerance

**Q2: Block Storage**
- a) 1 block (file is 245 bytes, much smaller than 128MB block size)
- b) 127.0.0.1:9866 (localhost DataNode)
- c) 1 rack (/default-rack)
- d) Single machine = pseudo-distributed mode = only 1 DataNode

**Q3: Replication Change (setrep 2)**
- a) stat shows 2 (desired replication)
- b) Only 1 actual copy (can't replicate on single machine, file is "under-replicated")
- c) On 3-node cluster, HDFS would create 2 copies on 2 different DataNodes

**Q4: Data Locality**
- HDFS runs Map tasks on same machine as data to avoid network transfer
- Moving code (KB) is faster than moving data (TB)
- Reduces network congestion and enables parallel processing

**Q5: Block Abstraction (Optional)**
- Enables files larger than single disk
- Independent replication per block
- Parallel processing of blocks
- Efficient recovery (only re-replicate damaged blocks)

**Q6: Node Failures (Optional)**
- 1 DataNode stops: YES, 2 copies remain
- 2 DataNodes stop: YES, 1 copy remains
- NameNode stops: NO, metadata lost (can't locate blocks)

### Part 2: PySpark

**Q7: MapReduce Phases**
- a) Map: Extract (city, temperature) pairs from each row
- b) Shuffle: Redistribute so all temps for same city go to same partition
- c) Reduce: Find max temperature per city group

**Q8: Execution Plan**
- a) 2 stages (separated by shuffle)
- b) "Exchange hashpartitioning" = shuffle
- c) Shuffle needed because data for same city may be on different partitions

**Q9: Caching**
- Second execution faster because data already in RAM
- First run reads from HDFS (disk), second run reads from cache (memory)
- RAM is ~50x faster than disk

### Part 3: Reflection

**Q10: HDFS Replication**
- 3x: survives 2 failures, safe during repair (2 copies while fixing)
- 1x OK for lab (sample data, single machine), NOT for production (real data loss)
- Higher replication = more safety but more storage cost and slower writes

**Q11: Data Locality**
- NameNode tracks block locations, scheduler assigns tasks to nodes with data
- At petabyte scale, moving data takes days; moving code takes milliseconds
- Without locality: network bottleneck, 10-100x slower, can't scale

**Q12: MapReduce vs Spark**
- Choose Hadoop: massive datasets that don't fit in memory, simple batch jobs, budget constraints
- Choose Spark: iterative ML, interactive queries, real-time streaming, multi-step pipelines
- Fundamental difference: Hadoop writes to disk between steps; Spark keeps data in memory

**Q13: Shuffle Phase**
- Most expensive: involves disk I/O + network transfer + serialization + sorting
- Spark optimizes: partial aggregation, in-memory shuffle, pipelining, AQE, Tungsten serialization
- Caused by: groupBy, join, orderBy, distinct, repartition

---

# 7. Key Comparisons

## 7.1 Hadoop MapReduce vs Spark

| Feature | Hadoop MapReduce | Spark |
|---------|-----------------|-------|
| Processing | Disk-based | In-memory |
| Speed | Slower | 10-100x faster |
| Between steps | Write to disk | Keep in RAM |
| Best for | Large batch jobs | Iterative/interactive |
| Fault recovery | Disk checkpoints | Lineage recompute |
| Cost | Less RAM needed | More RAM needed |
| Real-time | No | Yes (Streaming) |
| Programming | Java (verbose) | Python, Scala, SQL |
| ML support | Limited | Built-in (MLlib) |

## 7.2 Transformations vs Actions

| Transformations (LAZY) | Actions (EAGER) |
|------------------------|-----------------|
| Create new DataFrame | Trigger execution |
| Nothing executes | Return results |
| Build execution plan | Run the plan |
| select, filter, groupBy, join | count, show, collect, write |

## 7.3 NameNode vs DataNode

| NameNode | DataNode |
|----------|----------|
| Stores metadata | Stores actual data |
| File-to-block mapping | Block files on disk |
| Single instance | Multiple instances |
| Single point of failure | Can lose some without data loss |
| Runs in memory | Stores on disk |

## 7.4 HDFS vs Local File System

| HDFS | Local File System |
|------|-------------------|
| Distributed across machines | Single machine |
| Files split into blocks | Files stored as-is |
| Replicated (default 3x) | Single copy |
| Fault tolerant | No fault tolerance |
| Supports petabytes | Limited by disk size |
| Optimized for large files | Any file size |

---

# Quick Reference Commands

## HDFS Management
```bash
start-dfs.sh          # Start HDFS
stop-dfs.sh           # Stop HDFS
jps                   # Check running processes
sudo service ssh start # Start SSH (required before HDFS)
```

## HDFS File Operations
```bash
hdfs dfs -ls /                              # List root
hdfs dfs -mkdir -p /user/name/folder        # Create directory
hdfs dfs -put -f local.txt /hdfs/path/      # Upload (overwrite)
hdfs dfs -get /hdfs/path/file.txt local.txt # Download
hdfs dfs -cat /hdfs/path/file.txt           # View content
hdfs dfs -rm -r /hdfs/path/folder           # Delete directory
hdfs dfs -stat "%b %o %r %n" /path/file     # File statistics
hdfs dfs -setrep 2 /path/file               # Change replication
hdfs fsck /path/file -files -blocks -locations # Block health
```

## Spark
```python
# Initialize
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("app").getOrCreate()

# Read data
df = spark.read.csv("file.csv", header=True, inferSchema=True)

# Common operations
df.show()                    # Display data
df.printSchema()             # Show column types
df.count()                   # Count rows
df.select("col1", "col2")   # Select columns
df.filter(df.col > 50)      # Filter rows
df.groupBy("col").agg(F.max("val"))  # Aggregate

# Cache
df_cached = df.cache()       # Cache in memory

# Execution plan
df.explain(True)             # Show full plan

# Stop
spark.stop()
```

## WSL2
```bash
source ~/hadoop_env/bin/activate   # Activate Python env
jupyter lab --no-browser           # Start Jupyter
```

---

**End of Notes**

*Generated: Spring 2026 - CS570 Big Data Processing*
