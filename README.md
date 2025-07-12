# ABFS Thread Leak Reproduction Test

**Critical Bug Reproduction for Apache Hadoop ABFS Driver**

This repository contains a complete reproduction test case and proof-of-concept fix for a critical thread leak in the Apache Hadoop ABFS (Azure Data Lake Storage Gen2) driver.

## Issue Summary

**Bug:** `AbfsClientThrottlingAnalyzer` creates Timer threads that are never cleaned up, causing memory exhaustion and OutOfMemoryError crashes in production environments.

**Impact:** Production Hive Metastore instances crash with 60,000+ leaked threads  
**Component:** `hadoop-azure` - `AbfsClientThrottlingAnalyzer` class  
**Affected Versions:** Hadoop 3.3.6, 3.4.1+, and all versions with ABFS autothrottling  

## Quick Start

### Prerequisites
- Java 11+ (tested with Java 23)
- Maven 3.6+
- Azure storage account with access key

### Running the Test

1. **Clone and configure:**
   ```bash
   git clone <this-repo>
   cd abfs-thread-leak-test
   
   # Update your Azure storage account key
   # Edit src/main/java/AbfsThreadLeakTest.java line 141:
   # conf.set("fs.azure.account.key.YOURACCOUNT.dfs.core.windows.net", "YOUR_KEY_HERE");
   # Edit src/main/java/AbfsThreadLeakTest.java line 26 and 27:
   #  String[] storageAccounts = {
   #         "abfs://<container1>@<account>.dfs.core.windows.net/",
   #         "abfs://<container2>@<account>.dfs.core.windows.net/"
   #   };
   ```

2. **Build:**
   ```bash
   ./mvnw clean package
   ```

3. **Run the reproduction test:**
   ```bash
   java --add-opens java.base/java.lang=ALL-UNNAMED \
        --add-opens java.base/java.util=ALL-UNNAMED \
        --add-opens java.base/java.util.concurrent=ALL-UNNAMED \
        --add-opens java.base/java.net=ALL-UNNAMED \
        --add-opens java.base/java.io=ALL-UNNAMED \
        -Djava.security.manager=allow \
        -Dhadoop.home.dir=/tmp \
        -Dhadoop.security.authentication=simple \
        -jar target/abfs-thread-leak-test-1.0.0.jar
   ```

## Test Results

### Standard Test (20 cycles, 2-second intervals)
```
Cycle 1: 6→22 total threads (+16), 0→2 ABFS timer threads (+2)
Cycle 2: 22→24 total threads (+2), 2→4 ABFS timer threads (+2)
...
Final: 55 total threads (+49), 36 ABFS timer threads (+36)

⚠️ THREAD LEAK DETECTED!
36 ABFS timer threads were not cleaned up
```

### Realistic Test (200 cycles, no delays)
```
Final: 324 total threads (+318), 304 ABFS timer threads (+304)

⚠️ MASSIVE THREAD LEAK DETECTED!
304 ABFS timer threads were not cleaned up
```

### Live Production Monitoring
```bash
$ jstack <PID> | grep 'abfs-timer-client-throttling-analyzer' | wc -l
282
290
294
298
304
308  # Continues growing even after test completion
```

## 🔍 Root Cause Analysis

### The Problem
`AbfsClientThrottlingAnalyzer` creates Timer objects in its constructor but has no cleanup mechanism:

```java
// In constructor - creates timer threads
this.timer = new Timer(
    String.format("abfs-timer-client-throttling-analyzer-%s", name), true);

// Missing: No close() method to call timer.cancel()
```

### Why It Happens
1. Each ABFS filesystem connection creates an `AbfsClient`
2. Each `AbfsClient` creates an `AbfsClientThrottlingAnalyzer` 
3. Each analyzer creates Timer threads for read/write throttling
4. When `AbfsClient.close()` is called, it doesn't clean up analyzer timers
5. Timer threads persist until JVM shutdown

### Thread Evidence
Leaked threads have consistent naming pattern:
```
abfs-timer-client-throttling-analyzer-read {storage-account}
abfs-timer-client-throttling-analyzer-write {storage-account}
```

## ✅ Proof-of-Concept Fix

This repository includes a working fix that completely eliminates the thread leak.

### Fix Implementation
Added to `AbfsClientThrottlingAnalyzer.java`:

```java
/**
 * Closes the throttling analyzer and cleans up resources.
 */
public void close() {
    if (timer != null) {
        timer.cancel();
        timer.purge();
        timer = null;
    }
    
    if (blobMetrics != null) {
        blobMetrics.set(null);
    }
    
    LOG.debug("AbfsClientThrottlingAnalyzer for {} has been closed and cleaned up", name);
}

@VisibleForTesting
public boolean isClosed() {
    return timer == null;
}
```

### Fix Validation Results
```
🔴 Without fix: +3 ABFS timer threads (LEAKED)
✅ With fix: +0 ABFS timer threads (NO LEAK)

✅ FIX WORKS! No additional threads leaked when close() is called!
```

## 🔧 Immediate Workaround

For production environments experiencing this issue:

```xml
<property>
  <name>fs.azure.enable.autothrottling</name>
  <value>false</value>
</property>
```

**Trade-offs:**
- Completely eliminates thread leak
- Disables client-side throttling (server-side throttling still active)
- May see increased 503 responses under heavy load

## Production Impact

### Scale Projection
- **Test environment:** 304 threads in minutes
- **Production environment:** 60,000+ threads over weeks
- **Memory impact:** Each thread consumes ~1MB stack space
- **Crash threshold:** Varies by JVM configuration and available memory

## Test Architecture

### Test Structure
```
src/main/java/
├── AbfsThreadLeakTest.java                    # Main test harness
└── org/apache/hadoop/fs/azurebfs/services/
    └── AbfsClientThrottlingAnalyzer.java     # Fixed version with close()
```

### Test Scenarios
1. **Bug Reproduction:** Normal ABFS operations with autothrottling enabled
2. **Workaround Validation:** Same operations with autothrottling disabled  
3. **Fix Verification:** Direct analyzer testing with proper cleanup
4. **Live Monitoring:** Real-time thread count tracking

### System Information Captured
- Java version and vendor
- Hadoop version and build details
- Thread states and IDs
- Memory usage analysis
- Configuration details

## Monitoring Commands

Monitor thread leaks in production:

```bash
# Count ABFS timer threads
jstack <PID> | grep 'abfs-timer-client-throttling-analyzer' | wc -l

# Monitor total thread count
jstack <PID> | grep "^\"" | wc -l

# Watch for thread growth
watch -n 5 "jstack <PID> | grep 'abfs-timer-client-throttling-analyzer' | wc -l"
```

## Recommendations

### Immediate Actions
1. Apply workaround: `fs.azure.enable.autothrottling=false`
2. Monitor thread counts in production environments
3. Restart affected services when thread count exceeds safe limits

### Long-term Fix
1. Add `close()` method to `AbfsClientThrottlingAnalyzer`
2. Ensure `AbfsClient.close()` calls analyzer cleanup
3. Add proper resource management to prevent future leaks

### Testing
This repository provides a complete test suite for:
- Reproducing the bug reliably
- Validating the workaround
- Verifying the fix effectiveness
- Monitoring production environments

## Technical Details

**Test Environment:**
- Java: 23.0.2 (also tested on Java 11)
- Hadoop: 3.3.6
- Maven: 3.9+
- OS: macOS 15.3 (cross-platform compatible)

**Thread Leak Pattern:**
- +2 threads per ABFS filesystem connection (read + write analyzer)
- Threads persist indefinitely until JVM shutdown
- Memory usage grows linearly with thread count
- No self-healing or cleanup mechanism
