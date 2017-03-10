/*
 * Copyright 2017 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.hekate.runner;

import io.hekate.core.Hekate;
import io.hekate.core.HekateVersion;
import java.io.File;
import java.io.IOException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.FileSystemXmlApplicationContext;

/**
 * Command line runner for Spring-based {@link Hekate} application.
 */
public final class HekateRunner {
    private static final String CONFIG_DIR = "/config";

    private static final String CONFIG_DIR_PATH = System.getProperty("hekate.home", ".") + CONFIG_DIR;

    private static final String DEFAULT_CONFIG_FILE = "hekate.xml";

    private static final String CONFIG_FILE_OPTION = "c";

    private static final String VERSION_OPTION = "version";

    private static final String HELP_OPTION = "help";

    private static final Options OPTIONS = new Options();

    static {
        OPTIONS.addOption(Option.builder(CONFIG_FILE_OPTION)
            .longOpt("config")
            .hasArg().argName("CONFIG_FILE")
            .desc("Configuration file (Optional)")
            .build());
        OPTIONS.addOption(new Option(VERSION_OPTION, "Prints version"));
        OPTIONS.addOption(new Option(HELP_OPTION, "Prints help"));
    }

    private HekateRunner() {
        // No-op.
    }

    /**
     * Runs this application.
     *
     * @param args Optional parameter {@code -c <path-to-xml-file>}.
     *
     * @throws IOException Configuration file loading error.
     */
    public static void main(String[] args) throws IOException {
        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine commandLine = parser.parse(OPTIONS, args);

            if (commandLine.hasOption(HELP_OPTION)) {
                printHelpAndExit(0);
            } else if (commandLine.hasOption(VERSION_OPTION)) {
                print(HekateVersion.getInfo());
            } else {
                String cfgPath = DEFAULT_CONFIG_FILE;

                if (commandLine.hasOption("c")) {
                    cfgPath = commandLine.getOptionValue("c");
                }

                File cfgFile = new File(cfgPath);

                if (!cfgFile.isAbsolute()) {
                    cfgFile = new File(new File(CONFIG_DIR_PATH), cfgPath);
                }

                if (!cfgFile.exists()) {
                    printErrorExit("Configuration file doesn't exist - " + cfgFile.getCanonicalPath());
                } else if (!cfgFile.isFile()) {
                    printErrorExit("Not a file - " + cfgFile.getCanonicalPath());
                } else {
                    runApp(cfgFile);
                }
            }
        } catch (ParseException e) {
            printError(e.getMessage());

            printHelpAndExit(-1);
        }
    }

    private static void runApp(File cfgFile) throws IOException {
        String cfgPath = "file:" + cfgFile.getCanonicalPath();

        LoggerFactory.getLogger(HekateRunner.class).info("Starting {} [config={}]", HekateVersion.getFullVersion(), cfgPath);

        FileSystemXmlApplicationContext ctx = new FileSystemXmlApplicationContext(cfgPath);

        ctx.registerShutdownHook();
    }

    private static void printHelpAndExit(int status) {
        HelpFormatter formatter = new HelpFormatter();

        formatter.setWidth(400);
        // Preserve options definition order.
        formatter.setOptionComparator(null);

        formatter.printHelp("hekate [-" + CONFIG_FILE_OPTION + " <CONFIG_FILE>]", OPTIONS);

        System.exit(status);
    }

    private static void printErrorExit(String msg) {
        printError(msg);

        System.exit(-1);
    }

    private static void printError(String msg) {
        print("ERROR: " + msg);
    }

    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    private static void print(String msg) {
        System.out.println(msg);
    }
}
