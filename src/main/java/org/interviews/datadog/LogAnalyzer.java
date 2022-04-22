package org.interviews.datadog;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.Instant;
import java.util.*;

/**
 * Utility for analyzing CSV-formatted access logs. Uses Apache Commons CSV to help us read CSV files.
 */
public final class LogAnalyzer {
    private static final Logger LOG = LogManager.getLogger(LogAnalyzer.class);

    private final AlertManager alertManager;
    private final Deque<CSVRecord> statsWindow;
    private final Deque<Long> rateLimitWindow;
    private final int rateLimitPerSecond;
    private final int statsWindowSizeSeconds;
    private final int rateLimitWindowSizeSeconds;
    private boolean alertTriggered;

    /**
     * Default constructor
     *
     * @param statsWindowSizeSeconds     Window size for statistics collection. Expressed in seconds.
     * @param rateLimitWindowSizeSeconds Window size for rate-limiting. Expressed in seconds.
     * @param rateLimitPerSecond         Maximum requests allowed per second.
     * @param alertManager               Alert Manager.
     */
    public LogAnalyzer(int statsWindowSizeSeconds, int rateLimitWindowSizeSeconds, int rateLimitPerSecond, AlertManager alertManager) {
        if(statsWindowSizeSeconds <= 0) {
            throw new IllegalArgumentException("Must provide a non-zero, non-negative value for the stats window parameter!");
        }
        if(rateLimitWindowSizeSeconds <= 0) {
            throw new IllegalArgumentException("Must provide a non-zero, non-negative value for the rate-limit window parameter!");
        }
        if(rateLimitPerSecond <= 0) {
            throw new IllegalArgumentException("Must provide a non-zero, non-negative value for the rate-limit parameter!");
        }

        this.statsWindowSizeSeconds = statsWindowSizeSeconds;
        this.rateLimitWindowSizeSeconds = rateLimitWindowSizeSeconds;
        this.rateLimitPerSecond = rateLimitPerSecond;
        this.alertManager = alertManager;
        this.statsWindow = new ArrayDeque<>();
        this.rateLimitWindow = new ArrayDeque<>();
    }

    /**
     * Given a CSV log file, reads it one line at a time, maintaining two windows of requests. Although similar,
     * the stats window contains a discrete interval of requests which are rolled up into a set of statistics once the
     * window's threshold has been reached. The window then starts over at the beginning of the next interval.
     * <p>
     * The rate-limit window is a sliding window of requests used purely for rate-limiting. For each log entry,
     * we check to see if the rate-limit has been exceeded for the window. If it has, an alert is
     * triggered. Once the request rate dips back under the threshold, the alert is cleared. And if we have more than
     * X minutes worth of requests in the window, we slide items out of the window until we're back under the
     * limit.
     *
     * @param filePath  Path to a CSV log file to be analyzed.
     *
     */
    public void analyze(String filePath) throws IOException {
        LOG.info("Running log analysis with the following configuration - statsWindowSizeSeconds: {}, rateLimitWindowSizeSeconds: {}, rateLimit: {}, file: {}",
                statsWindowSizeSeconds, rateLimitWindowSizeSeconds, rateLimitPerSecond, filePath);

        try (Reader in = new FileReader(filePath)) {
            Iterable<CSVRecord> logs = CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withIgnoreEmptyLines()
                    .parse(in);

            for (CSVRecord log : logs) {
                // Validate the current log entry before continuing...
                if (isLogValid(log)) {
                    long windowEnd = Long.parseLong(log.get("date"));

                    rateLimitWindow.offer(windowEnd);
                    checkRateLimit();

                    // If the current log entry gets us over the stats window threshold, then go ahead and compute the
                    // stats for the current window before we add the log entry to it.
                    // This will prevent us from having more logs in our window than we expect.
                    if (!statsWindow.isEmpty()) {
                        long windowStart = Long.parseLong(statsWindow.peekFirst().get("date"));
                        if (windowEnd - windowStart > statsWindowSizeSeconds) {
                            computeAndPrintStatistics();
                        }
                    }
                    statsWindow.offer(log);
                }
            }
        }
    }

    /**
     * Checks to see if we're within our pre-defined rate limit.
     * If we're over the limit, trigger the alert if we haven't already.
     * Otherwise, if we're within the limit, reset the alert if it's been triggered.
     * Finally, shrink the window until we're back under the rate-limit window's size.
     */
    private void checkRateLimit() {
        long windowStart = rateLimitWindow.peekFirst();
        long windowEnd = rateLimitWindow.peekLast();
        int maxAllowedRequestsPerWindow = rateLimitPerSecond * rateLimitWindowSizeSeconds;

        // Check to see if we've exceeded the rate limit in our window
        if (!rateLimitWindow.isEmpty() && windowEnd - windowStart <= rateLimitWindowSizeSeconds) {
            if (rateLimitWindow.size() > maxAllowedRequestsPerWindow && !alertTriggered) {
                alertTriggered = true;
                alertManager.triggerAlert();
                LOG.warn("High traffic generated an alert - hits = {}, triggered at {}. Maximum number of requests allowed in {} seconds: {}", rateLimitWindow.size(), Instant.ofEpochSecond(windowEnd), rateLimitWindowSizeSeconds, maxAllowedRequestsPerWindow);
            }
            else if (rateLimitWindow.size() <= maxAllowedRequestsPerWindow && alertTriggered) {
                alertTriggered = false;
                alertManager.clearAlert();
                LOG.warn("Traffic back within rate-limit threshold of {} requests per second at {}", rateLimitPerSecond, Instant.ofEpochSecond(windowEnd));
            }
        } else {
            while (!rateLimitWindow.isEmpty() && windowEnd - windowStart > rateLimitWindowSizeSeconds) {
                rateLimitWindow.pollFirst();
            }
        }
    }

    /**
     * For each log entry in the stats window:
     * 1) Remove the entry
     * 2) Count the status code, number of bytes transferred, the accessed section, and the host.
     * <p>
     * Once the window is empty, print out aggregated statistics for the aforementioned fields.
     */
    private void computeAndPrintStatistics() {
        long totalBytesTransferred = 0;
        int numberOf200s = 0, numberOf300s = 0, numberOf400s = 0, numberOf500s = 0;

        Instant startTime = Instant.ofEpochSecond((Long.parseLong(statsWindow.peekFirst().get("date"))));
        Instant endTime = Instant.ofEpochSecond((Long.parseLong(statsWindow.peekLast().get("date"))));

        Map<String, Integer> sectionCounts = new HashMap<>();
        Map<String, Integer> hostCounts = new HashMap<>();
        while (!statsWindow.isEmpty()) {
            CSVRecord entry = statsWindow.poll();
            try {
                int status = Integer.parseInt(entry.get("status"));
                if (status >= 200 && status < 300) {
                    numberOf200s++;
                } else if (status >= 300 && status < 400) {
                    numberOf300s++;
                } else if (status >= 400 && status < 500) {
                    numberOf400s++;
                } else if (status >= 500 && status < 600) {
                    numberOf500s++;
                }
            } catch (NumberFormatException ex) {
                LOG.error("Invalid status code value: " + entry.get("status"));
            }

            try {
                totalBytesTransferred += Long.parseLong(entry.get("bytes"));
            } catch (NumberFormatException ex) {
                LOG.error("Invalid bytes value: " + entry.get("bytes"));
            }

            // Count the number of times each website section was hit.
            String section = parseSectionValue(entry);
            if (section != null) {
                sectionCounts.put(section, sectionCounts.getOrDefault(section, 0) + 1);
            }

            // Count the number of times each host made a request.
            String host = entry.get("remotehost");
            hostCounts.put(host, hostCounts.getOrDefault(host, 0) + 1);
        }

        LOG.info("***** Statistics for the window from {} to {} *****", startTime, endTime);
        LOG.info("Total Bytes Transferred: {}", totalBytesTransferred);
        LOG.info("Number of 200s: {}", numberOf200s);
        LOG.info("Number of 300s: {}", numberOf300s);
        LOG.info("Number of 400s: {}", numberOf400s);
        LOG.info("Number of 500s: {}", numberOf500s);

        // Sort the section counts in reverse order so we can easily find and print the most-accessed section.
        sectionCounts.entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .limit(1)
                .forEach(x -> LOG.info("Most-requested section: " + x.getKey() + " Number of hits: " + x.getValue()));

        // Sort the section counts in reverse order so we can easily find and print the most-accessed section.
        hostCounts.entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .limit(1)
                .forEach(x -> LOG.info("Host with most requests: " + x.getKey() + " Number of hits: " + x.getValue()));

        LOG.info("***** End statistics *****");
    }

    /**
     * Checks for the validity of a log entry by seeing if the date field is valid.
     * We need a valid date at the very minimum to do any analysis on the entry.
     *
     * We'll consider negative or un-parseable date values to be invalid.
     *
     * @param entry Log entry.
     * @return True if the entry has a valid date. False otherwise.
     */
    private static boolean isLogValid(CSVRecord entry) {
        try {
            return Long.parseLong(entry.get("date")) >= 0;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Parses out the "request" value from the current log entry. Assumes that each
     * request is in the format "VERB PATH PROTOCOL" (e.g. "GET /api/test HTTP/1.0").
     *
     * @param entry Log entry.
     * @return The website section if valid. Null otherwise.
     */
    private static String parseSectionValue(CSVRecord entry) {
        try {
            // The section will be before the 2nd '/' in the string.
            String[] request = entry.get("request").split(" ");
            return "/" + request[1].split("/")[1];
        } catch (Exception e) {
            LOG.error("Error parsing section from log entry {}", entry.toString());
        }
        return null;
    }
}
