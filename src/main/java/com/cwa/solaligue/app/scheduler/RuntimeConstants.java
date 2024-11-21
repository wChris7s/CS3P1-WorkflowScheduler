package com.cwa.solaligue.app.scheduler;

public class RuntimeConstants {

    public static long OneHour_MS = 3600000;
    public static long OneSec_MS = 1000;

    /**
     * Quantum size (in seconds)
     * Default: 1 hour
     */
    public static long quantum_MS = OneHour_MS;

    /**
     * Network speed (in MB/sec)
     * Default: 100 MB/sec (~1GBit)
     */
    public static  double network_speed_B_MS = 104857.6;

    public static double precisionError = 0.0000000001;

    public static double distributed_storage_speed_B_MS = 104857.6;
}
