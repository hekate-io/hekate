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

package io.hekate.core.internal.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Collection;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;

public final class Utils {
    public static final int MAGIC_BYTES = 19800124;

    /** Prefix for file names constructed via {@link #addressToFileName(InetSocketAddress)}. */
    public static final String ADDRESS_FILE_PREFIX = "node_";

    /** Separator for address components in a file name constructed via {@link #addressToFileName(InetSocketAddress)}. */
    public static final String ADDRESS_FILE_SEPARATOR = "_";

    /** {@link Charset} for UTF-8. */
    public static final Charset UTF_8 = Charset.forName("UTF-8");

    private static final DecimalFormatSymbols NUMBER_FORMAT_SYMBOLS;

    private static final ThreadPoolExecutor FALLBACK_EXECUTOR;

    private static String pid;

    static {
        // Formatting options.
        NUMBER_FORMAT_SYMBOLS = new DecimalFormatSymbols(Locale.getDefault());

        NUMBER_FORMAT_SYMBOLS.setDecimalSeparator('.');

        // Global fallback executor.
        HekateThreadFactory factory = new HekateThreadFactory("AsyncFallback") {
            @Override
            protected String resolveNodeName(String nodeName) {
                return null;
            }
        };

        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();

        FALLBACK_EXECUTOR = new ThreadPoolExecutor(0, 1, 1, TimeUnit.SECONDS, queue, r -> {
            Thread t = factory.newThread(r);

            t.setDaemon(true);

            return t;
        });

        FALLBACK_EXECUTOR.allowCoreThreadTimeOut(true);
    }

    private Utils() {
        // No-op.
    }

    public static String pid() {
        // No synchronization here:
        // 1) Pid never changes
        // 1) Extraction is cheap and it is ok if several threads will do it in parallel
        if (pid == null) {
            String jvmName = ManagementFactory.getRuntimeMXBean().getName();

            int index = jvmName.indexOf('@');

            if (index < 0) {
                pid = "";
            } else {
                pid = jvmName.substring(0, index);
            }
        }

        return pid;
    }

    public static String numberFormat(String pattern, Number number) {
        return new DecimalFormat(pattern, NUMBER_FORMAT_SYMBOLS).format(number);
    }

    public static String byteSizeFormat(long bytes) {
        long mb = bytes / 1024 / 1024;

        double gb = mb / 1024D;

        if (gb >= 1) {
            return numberFormat("###.##Gb", gb);
        } else {
            return mb + "Mb";
        }
    }

    public static int mod(int hash, int size) {
        if (hash < 0) {
            return -hash % size;
        } else {
            return hash % size;
        }
    }

    public static <T> Stream<T> nullSafe(Collection<T> collection) {
        if (collection == null || collection.isEmpty()) {
            return Stream.empty();
        }

        return collection.stream().filter(Objects::nonNull);
    }

    public static String stackTrace(Throwable error) {
        StringWriter out = new StringWriter();

        error.printStackTrace(new PrintWriter(out));

        return out.toString();
    }

    public static Executor fallbackExecutor() {
        return FALLBACK_EXECUTOR;
    }

    public static Waiting shutdown(ExecutorService executor) {
        executor.shutdown();

        return () -> executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public static <T> T getUninterruptedly(Future<T> future) throws ExecutionException {
        boolean interrupted = false;

        try {
            while (true) {
                try {
                    return future.get();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static boolean isCausedBy(Throwable error, Class<? extends Throwable> causeType) {
        return findCause(error, causeType) != null;
    }

    public static <T extends Throwable> T findCause(Throwable error, Class<T> causeType) {
        Throwable cause = error;

        while (cause != null) {
            if (causeType.isAssignableFrom(cause.getClass())) {
                return causeType.cast(cause);
            }

            cause = cause.getCause();
        }

        return null;
    }

    /**
     * Returns {@code true} if the specified value is a power of two.
     *
     * @param n Number to check.
     *
     * @return {@code true} if the specified value is a power of two.
     */
    public static boolean isPowerOfTwo(int n) {
        return (n & n - 1) == 0 && n > 0;
    }

    /**
     * Parses the specified string into a {@link InetSocketAddress}.
     *
     * <p>
     * String must be formatted as {@code <host>:<port>} (f.e. {@code 192.168.39.41:10012}). IPv6 addresses should wrap {@code <host>} path
     * with square brackets.
     * </p>
     *
     * @param address Address string.
     * @param check Checker that should be used to report parsing errors.
     *
     * @return Address or {@code null} if the provided address string is empty or {@code null}.
     *
     * @throws UnknownHostException Signals that host address couldn't be resolved (see {@link InetAddress#getByName(String)}).
     */
    public static InetSocketAddress parseAddress(String address, ConfigCheck check) throws UnknownHostException {
        return doParseSocketAddress(address, check, true);
    }

    /**
     * Parses the specified string into a {@link InetSocketAddress} with an unresolved host name.
     *
     * <p>
     * String must be formatted as {@code <host>:<port>} (f.e. {@code 192.168.39.41:10012}). IPv6 addresses should wrap {@code <host>} path
     * with square brackets.
     * </p>
     *
     * @param address Address string.
     * @param check Checker that should be used to report parsing errors.
     *
     * @return Address or {@code null} if the provided address string is empty or {@code null}.
     */
    public static InetSocketAddress parseUnresolvedAddress(String address, ConfigCheck check) {
        try {
            return doParseSocketAddress(address, check, false);
        } catch (UnknownHostException e) {
            // Never happens.
            throw new AssertionError("Unexpected error while parsing unresolved socket address.", e);
        }
    }

    /**
     * Converts the specified address into a string that is suitable for naming files.
     *
     * @param address Address.
     *
     * @return File name.
     */
    public static String addressToFileName(InetSocketAddress address) {
        return ADDRESS_FILE_PREFIX + address.getAddress().getHostAddress() + ADDRESS_FILE_SEPARATOR + address.getPort();
    }

    /**
     * Parses an address from the specified file name which was constructed via {@link #addressToFileName(InetSocketAddress)}. Returns
     * {@code null} if address could not be parsed due to invalid file format or if host address can't be {@link UnknownHostException
     * resolved}.
     *
     * @param name File name.
     * @param log Optional logger for errors logging.
     *
     * @return Address or {@code null} if parsing failed or parsed host is {@link UnknownHostException unknown}.
     */
    public static InetSocketAddress addressFromFileName(String name, Logger log) {
        if (name.startsWith(ADDRESS_FILE_PREFIX) && name.length() > ADDRESS_FILE_PREFIX.length()) {
            String[] tokens = name.substring(ADDRESS_FILE_PREFIX.length()).split(ADDRESS_FILE_SEPARATOR);

            if (tokens.length == 2) {
                InetAddress host = null;
                Integer port = null;

                try {
                    host = InetAddress.getByName(tokens[0]);
                } catch (UnknownHostException e) {
                    if (log != null) {
                        log.warn("Failed to parse address from file name [name={}, cause={}]", name, e.toString());
                    }
                }

                try {
                    port = Integer.parseInt(tokens[1]);
                } catch (NumberFormatException e) {
                    if (log != null) {
                        log.warn("Failed to parse address from file name [name={}, cause={}]", name, e.toString());
                    }
                }

                if (host != null && port != null) {
                    return new InetSocketAddress(host, port);
                }
            }
        }

        return null;
    }

    public static <T> String toString(Collection<T> collection, Function<? super T, String> mapper) {
        return collection.stream().map(mapper).sorted().collect(Collectors.joining(", ", "{", "}"));
    }

    /**
     * Returns {@code null} if the specified string is {@code null} or is an empty string after {@link String#trim() trimming}; returns a
     * {@link String#trim() trimmed} string otherwise.
     *
     * @param str String.
     *
     * @return {@code null} or {@link String#trim() trimmed} string.
     */
    public static String nullOrTrim(String str) {
        if (str == null) {
            return null;
        }

        str = str.trim();

        return str.isEmpty() ? null : str;
    }

    private static InetSocketAddress doParseSocketAddress(String address, ConfigCheck check, boolean resolve) throws UnknownHostException {
        if (address == null) {
            return null;
        }

        address = address.trim();

        if (address.isEmpty()) {
            return null;
        }

        int portIdx = address.lastIndexOf(':');

        check.that(portIdx > 0 && portIdx < address.length() - 1, "port not specified in the address string '" + address + "' "
            + "(must be <host>:<port>).");

        String hostStr = address.substring(0, portIdx);
        String portStr = address.substring(portIdx + 1);

        int port = 0;

        try {
            port = Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            // No-op.
        }

        check.that(port > 0, "invalid port value '" + portStr + "' in address '" + address + "'.");

        String host;

        if (hostStr.startsWith("[")) {
            check.that(hostStr.endsWith("]"), "invalid IPv6 host address '" + hostStr + "'.");
            check.that(hostStr.length() > 2, "invalid IPv6 host address '" + hostStr + "'.");

            host = hostStr.substring(1, hostStr.length() - 1);
        } else {
            host = hostStr;
        }

        if (resolve) {
            InetAddress hostAddr = InetAddress.getByName(host);

            return new InetSocketAddress(hostAddr, port);
        } else {
            return InetSocketAddress.createUnresolved(host, port);
        }
    }
}
