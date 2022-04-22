package org.interviews.datadog;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Uses Apache Commons CLI to help parse our command-line arguments.
 */
public class App {
    private static final Logger LOG = LogManager.getLogger(App.class);

    public static void main(String[] args) {
        final Options options = new Options();
        final CommandLineParser parser = new DefaultParser();
        final HelpFormatter formatter = new HelpFormatter();

        // Read our defaults from the properties file
        try (FileReader reader = new FileReader("src/main/resources/application.properties")) {
            Properties props = new Properties();
            props.load(reader);

            String defaultRateLimit = props.getProperty("rateLimit");
            String defaultStatsWindowSizeSeconds = props.getProperty("statsWindowSizeSeconds");
            String defaultRateLimitWindowSizeSeconds = props.getProperty("rateLimitWindowSizeSeconds");

            // Required parameter
            Option file = Option.builder()
                    .argName("file")
                    .option("f")
                    .longOpt("file")
                    .required(true)
                    .hasArg()
                    .desc("Path to log file.")
                    .build();
            options.addOption(file);

            // Optional parameter
            Option statsWindowSize = Option.builder()
                    .argName("statsWindowSize")
                    .option("s")
                    .longOpt("statsWindowSize")
                    .required(false)
                    .type(Number.class)
                    .hasArg()
                    .desc("Window size (in seconds) for statistics collection and reporting.")
                    .build();
            options.addOption(statsWindowSize);

            // Optional parameter
            Option rateLimitWindowSizeSeconds = Option.builder()
                    .argName("rateLimitWindowSize")
                    .option("w")
                    .longOpt("rateLimitWindowSize")
                    .required(false)
                    .type(Number.class)
                    .hasArg()
                    .desc("Window size (in seconds) for rate-limit evaluation.")
                    .build();
            options.addOption(rateLimitWindowSizeSeconds);

            // Optional parameter
            Option rateLimit = Option.builder()
                    .argName("rateLimit")
                    .option("r")
                    .longOpt("rateLimit")
                    .required(false)
                    .type(Number.class)
                    .hasArg()
                    .desc("Maximum number of requests per second.")
                    .build();
            options.addOption(rateLimit);

            CommandLine cmd = parser.parse(options, args);
            LogAnalyzer logAnalyzer = new LogAnalyzer(
                    Integer.parseInt(cmd.getOptionValue("statsWindowSize", defaultStatsWindowSizeSeconds)),
                    Integer.parseInt(cmd.getOptionValue("rateLimitWindowSize", defaultRateLimitWindowSizeSeconds)),
                    Integer.parseInt(cmd.getOptionValue("rateLimit", defaultRateLimit)),
                    new AlertManagerImpl());
            logAnalyzer.analyze(cmd.getOptionValue("file"));
        } catch (IOException e) {
            LOG.error(e.getMessage());
            System.exit(1);
        } catch (NumberFormatException e) {
            LOG.error("Parameter value must be expressed as a number! {}", e.getMessage());
            formatter.printHelp("Log Analyzer", options);
            System.exit(1);
        } catch (Exception e) {
            LOG.error("Runtime error, aborting! {}", e.getMessage());
            System.exit(1);
        }
    }
}
