package org.gautelis.repo;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;

public class Reporting {


    public static void dumpStatistics() {
        String osName = System.getProperty("os.name");
        String osVersion = System.getProperty("os.version");
        String osArch = System.getProperty("os.arch");

        System.out.println("Operating System: " + osName);
        System.out.println("Version: " + osVersion);
        System.out.println("Architecture: " + osArch);

        Runtime runtime = Runtime.getRuntime();

        long totalMemory = runtime.totalMemory(); // Total memory in JVM
        long freeMemory = runtime.freeMemory();   // Free memory in JVM
        long maxMemory = runtime.maxMemory();     // Maximum memory the JVM will attempt to use

        System.out.println("Total Memory (bytes): " + totalMemory);
        System.out.println("Free Memory (bytes): " + freeMemory);
        System.out.println("Max Memory (bytes): " + maxMemory);

        int availableProcessors = Runtime.getRuntime().availableProcessors();
        System.out.println("Available processors (cores): " + availableProcessors);

        OperatingSystemMXBean osBean =
                (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

        // CPU load
        double cpuLoad = osBean.getCpuLoad();
        System.out.println("CPU Load: " + cpuLoad);

        // Total physical memory
        long totalPhysicalMemory = osBean.getTotalMemorySize();
        long freePhysicalMemory = osBean.getFreeMemorySize();

        System.out.println("Total Physical Memory (bytes): " + totalPhysicalMemory);
        System.out.println("Free Physical Memory (bytes): " + freePhysicalMemory);


    }
}
