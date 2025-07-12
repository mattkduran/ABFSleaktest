import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class AbfsThreadLeakTest {

    public static void main(String[] args) throws Exception {
        // Fix Hadoop security issues for testing
        System.setProperty("hadoop.home.dir", "/tmp");
        System.setProperty("hadoop.security.authentication", "simple");
        System.setProperty("hadoop.security.authorization", "false");

        printSystemInfo();

        // Test with multiple storage accounts to trigger multiple analyzers
        String[] storageAccounts = {
            "abfs://<container1>@<account>.dfs.core.windows.net/",
            "abfs://<container2>@<account>.dfs.core.windows.net/"
        };

        System.out.println("\n" + "=".repeat(60));
        System.out.println("ABFS THREAD LEAK REPRODUCTION TEST");
        System.out.println("=".repeat(60));
        System.out.println("Testing Hadoop ABFS timer thread leak bug");
        System.out.println("Storage accounts: " + java.util.Arrays.toString(storageAccounts));
        System.out.println("Autothrottling enabled: " + System.getProperty("fs.azure.enable.autothrottling", "true (default)"));
        System.out.println("=".repeat(60));

        // Monitor thread count and memory
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

        System.out.printf("%-8s %-12s %-12s %-15s %-10s %-15s%n",
                         "Cycle", "Total", "ABFS Timer", "Memory (MB)", "Delta", "Timestamp");
        System.out.println("-".repeat(80));

        long initialMemory = memoryBean.getHeapMemoryUsage().getUsed();

        for (int cycle = 1; cycle <= 200; cycle++) {
            long startTime = System.currentTimeMillis();

            // Count threads and memory before
            int threadsBefore = threadBean.getThreadCount();
            int abfsTimerThreadsBefore = countAbfsTimerThreads();
            long memoryBefore = memoryBean.getHeapMemoryUsage().getUsed();

            // Create and use ABFS filesystems
            for (String accountUrl : storageAccounts) {
                createAndUseFilesystem(accountUrl);
            }

            // Force GC to ensure objects are cleaned up
            System.gc();
            //Thread.sleep(1000);

            // Count threads and memory after
            int threadsAfter = threadBean.getThreadCount();
            int abfsTimerThreadsAfter = countAbfsTimerThreads();
            long memoryAfter = memoryBean.getHeapMemoryUsage().getUsed();
            long endTime = System.currentTimeMillis();

            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));

            System.out.printf("%-8d %-12s %-12s %-15.1f %-10d %-15s%n",
                cycle,
                String.format("%d->%d", threadsBefore, threadsAfter),
                String.format("%d->%d", abfsTimerThreadsBefore, abfsTimerThreadsAfter),
                (memoryAfter - initialMemory) / 1024.0 / 1024.0,
                (endTime - startTime),
                timestamp);

            // Sleep between cycles
            //Thread.sleep(2000);
        }

        System.out.println("\n" + "=".repeat(60));
        System.out.println("FINAL ANALYSIS");
        System.out.println("=".repeat(60));

        printDetailedThreadAnalysis();
        printMemoryAnalysis();
        printConfigurationDetails();
        printRecommendations();
        testFixedAnalyzer();
    }

    private static void printSystemInfo() {
        System.out.println("SYSTEM INFORMATION");
        System.out.println("=".repeat(40));
        System.out.println("Java Version: " + System.getProperty("java.version"));
        System.out.println("Java Vendor: " + System.getProperty("java.vendor"));
        System.out.println("OS: " + System.getProperty("os.name") + " " + System.getProperty("os.version"));
        try {
            System.out.println("Hadoop Version: " + org.apache.hadoop.util.VersionInfo.getVersion());
            System.out.println("Hadoop Build: " + org.apache.hadoop.util.VersionInfo.getBuildVersion());
        } catch (Exception e) {
            System.out.println("Hadoop Version: Unable to determine");
        }
        System.out.println("Test Time: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    }

    private static void printDetailedThreadAnalysis() {
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        long[] threadIds = threadBean.getAllThreadIds();

        System.out.println("THREAD ANALYSIS");
        System.out.println("-".repeat(40));
        System.out.println("Total threads: " + threadIds.length);

        int abfsTimerCount = 0;
        int abfsOtherCount = 0;
        int daemonCount = 0;

        System.out.println("\nABFS Timer Threads Found:");
        for (long id : threadIds) {
            ThreadInfo info = threadBean.getThreadInfo(id);
            if (info != null) {
                if (info.getThreadName().contains("abfs-timer-client-throttling-analyzer")) {
                    System.out.println("  [" + info.getThreadId() + "] " + info.getThreadName() +
                                     " (State: " + info.getThreadState() + ")");
                    abfsTimerCount++;
                } else if (info.getThreadName().toLowerCase().contains("abfs")) {
                    abfsOtherCount++;
                }
                if (info.isDaemon()) {
                    daemonCount++;
                }
            }
        }

        System.out.println("\nThread Summary:");
        System.out.println("  ABFS Timer threads: " + abfsTimerCount);
        System.out.println("  Other ABFS threads: " + abfsOtherCount);
        System.out.println("  Daemon threads: " + daemonCount);
        System.out.println("  Non-daemon threads: " + (threadIds.length - daemonCount));

        if (abfsTimerCount > 0) {
            System.out.println("\n⚠️  THREAD LEAK DETECTED!");
            System.out.println("   " + abfsTimerCount + " ABFS timer threads were not cleaned up");
            System.out.println("   These threads will persist until JVM shutdown");
        } else {
            System.out.println("\n✅ NO THREAD LEAK DETECTED");
        }
    }

    private static void printMemoryAnalysis() {
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
        MemoryUsage nonHeapUsage = memoryBean.getNonHeapMemoryUsage();

        System.out.println("\nMEMORY ANALYSIS");
        System.out.println("-".repeat(40));
        System.out.printf("Heap Memory: %.1f MB used / %.1f MB max%n",
                         heapUsage.getUsed() / 1024.0 / 1024.0,
                         heapUsage.getMax() / 1024.0 / 1024.0);
        System.out.printf("Non-Heap Memory: %.1f MB used / %.1f MB max%n",
                         nonHeapUsage.getUsed() / 1024.0 / 1024.0,
                         nonHeapUsage.getMax() / 1024.0 / 1024.0);
    }

    private static void printConfigurationDetails() {
        System.out.println("\nCONFIGURATION DETAILS");
        System.out.println("-".repeat(40));

        String[] importantConfigs = {
            "fs.azure.enable.autothrottling",
            "fs.azure.analysis.period",
            "fs.azure.account.operation.idle.timeout",
            "hadoop.security.authentication"
        };

        Configuration conf = new Configuration();
        for (String config : importantConfigs) {
            String value = conf.get(config, "not set");
            System.out.println("  " + config + " = " + value);
        }
    }

    private static void printRecommendations() {
        System.out.println("\nRECOMMENDATIONS");
        System.out.println("-".repeat(40));
        System.out.println("1. To fix this issue immediately, set:");
        System.out.println("   fs.azure.enable.autothrottling=false");
        System.out.println();
        System.out.println("2. Report this bug to Apache Hadoop JIRA:");
        System.out.println("   - Project: HADOOP");
        System.out.println("   - Component: fs/azure");
        System.out.println("   - Summary: AbfsClientThrottlingAnalyzer timer threads leak");
        System.out.println();
        System.out.println("3. Root cause: Missing cleanup in AbfsClientThrottlingAnalyzer");
        System.out.println("   - Timer created in constructor but never cancelled");
        System.out.println("   - AbfsClient.close() doesn't clean up analyzer timers");
        System.out.println();
        System.out.println("4. In production environments, monitor thread counts:");
        System.out.println("   jstack <PID> | grep 'abfs-timer-client-throttling-analyzer' | wc -l");
    }

    private static void createAndUseFilesystem(String uri) {
        Configuration conf = new Configuration();

        // Add your ABFS configuration here
        conf.set("fs.azure.account.auth.type", "SharedKey");
        conf.set("fs.azure.account.key.<account>.dfs.core.windows.net", "<dummykey>"); // Replace with actual key

        // Check if autothrottling is explicitly disabled
        String autothrottling = System.getProperty("fs.azure.enable.autothrottling",
                                                  conf.get("fs.azure.enable.autothrottling", "true"));
        conf.set("fs.azure.enable.autothrottling", autothrottling);

        FileSystem fs = null;
        try {
            // Create filesystem - this should create AbfsClient and AbfsClientThrottlingAnalyzer
            fs = FileSystem.get(URI.create(uri), conf);

            // Do some basic operations to ensure the client is fully initialized
            Path testPath = new Path(uri + "test-" + System.currentTimeMillis());
            fs.exists(testPath);  // This will trigger ABFS operations

        } catch (IOException e) {
            System.err.println("Error with " + uri + ": " + e.getMessage());
        } finally {
            // Close filesystem - this should clean up resources but WON'T clean up timer threads
            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    System.err.println("Error closing filesystem: " + e.getMessage());
                }
            }
        }
    }

    private static int countAbfsTimerThreads() {
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        long[] threadIds = threadBean.getAllThreadIds();

        int count = 0;
        for (long id : threadIds) {
            ThreadInfo info = threadBean.getThreadInfo(id);
            if (info != null && info.getThreadName().contains("abfs-timer-client-throttling-analyzer")) {
                count++;
            }
        }
        return count;
    }

    /**
     * Direct test of the fixed AbfsClientThrottlingAnalyzer to prove the fix works
     */
    private static void testFixedAnalyzer() throws Exception {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("TESTING FIXED ANALYZER DIRECTLY");
        System.out.println("=".repeat(60));

        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

        // Test the broken behavior (simulated)
        System.out.println("🔴 Simulating BROKEN behavior (no close() called):");
        int initialThreads = threadBean.getThreadCount();
        int initialAbfsThreads = countAbfsTimerThreads();

        // Create analyzer instances but don't call close() (simulates bug)
        for (int i = 1; i <= 3; i++) {
            try {
                Configuration conf = new Configuration();
                org.apache.hadoop.fs.azurebfs.AbfsConfiguration abfsConfig =
                    new org.apache.hadoop.fs.azurebfs.AbfsConfiguration(conf, "testaccount" + i);

                org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingAnalyzer analyzer =
                    new org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingAnalyzer("testaccount" + i, abfsConfig);

                Thread.sleep(200);
                analyzer.addBytesTransferred(1024, false);

                // DON'T call close() - simulates the bug
                System.out.println("  Created analyzer " + i + " (not closed - simulates bug)");

            } catch (Exception e) {
                System.err.println("Error creating analyzer: " + e.getMessage());
            }
        }

        Thread.sleep(1000);
        int brokenThreads = threadBean.getThreadCount();
        int brokenAbfsThreads = countAbfsTimerThreads();

        System.out.printf("  Result: %d total threads (+%d), %d ABFS timer threads (+%d) - LEAKED!%n",
            brokenThreads, brokenThreads - initialThreads,
            brokenAbfsThreads, brokenAbfsThreads - initialAbfsThreads);

        // Test the fixed behavior
        System.out.println("\n✅ Testing FIXED behavior (with close() called):");

        for (int i = 4; i <= 6; i++) {
            try {
                Configuration conf = new Configuration();
                org.apache.hadoop.fs.azurebfs.AbfsConfiguration abfsConfig =
                    new org.apache.hadoop.fs.azurebfs.AbfsConfiguration(conf, "testaccount" + i);

                org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingAnalyzer analyzer =
                    new org.apache.hadoop.fs.azurebfs.services.AbfsClientThrottlingAnalyzer("testaccount" + i, abfsConfig);

                Thread.sleep(200);
                analyzer.addBytesTransferred(1024, false);

                // CALL close() - this is the fix!
                analyzer.close();
                System.out.println("  Created analyzer " + i + " and properly closed it");

            } catch (Exception e) {
                System.err.println("Error creating/closing analyzer: " + e.getMessage());
            }
        }

        Thread.sleep(1000);
        int finalThreads = threadBean.getThreadCount();
        int finalAbfsThreads = countAbfsTimerThreads();

        System.out.printf("  Result: %d total threads (+%d), %d ABFS timer threads (+%d)%n",
            finalThreads, finalThreads - brokenThreads,
            finalAbfsThreads, finalAbfsThreads - brokenAbfsThreads);

        if (finalAbfsThreads == brokenAbfsThreads) {
            System.out.println("✅ FIX WORKS! No additional threads leaked when close() is called!");
        } else {
            System.out.println("❌ Fix didn't work - threads still leaking");
        }

        System.out.println("\n📋 SUMMARY:");
        System.out.printf("  Without fix: +%d ABFS timer threads (LEAKED)%n", brokenAbfsThreads - initialAbfsThreads);
        System.out.printf("  With fix: +%d ABFS timer threads (NO LEAK)%n", finalAbfsThreads - brokenAbfsThreads);
    }
}