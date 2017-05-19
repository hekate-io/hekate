package io.hekate.core.internal.util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import org.slf4j.Logger;

/**
 * Network-related utilities.
 */
public final class AddressUtils {
    /** Prefix for the file name of {@link #toFileName(InetSocketAddress)}. */
    public static final String FILE_PREFIX = "node_";

    /** Separator for the address's components of {@link #toFileName(InetSocketAddress)}. */
    public static final String FILE_SEPARATOR = "_";

    private AddressUtils() {
        // No-op.
    }

    /**
     * Parses the specified string into an {@link InetSocketAddress}.
     *
     * <p>
     * String must be formatted as {@code <host>:<port>} (f.e. {@code 192.168.39.41:10012}). IPv6 addresses must wrap {@code <host>} path
     * with square brackets.
     * </p>
     *
     * @param address Address string.
     * @param check Checker that should be used to report the address parsing errors.
     *
     * @return Address or {@code null} if the provided address string is empty or is {@code null}.
     *
     * @throws UnknownHostException Signals that the host address couldn't be resolved (see {@link InetAddress#getByName(String)}).
     */
    public static InetSocketAddress parse(String address, ConfigCheck check) throws UnknownHostException {
        return doParse(address, check, true);
    }

    /**
     * Parses the specified string as an {@link InetSocketAddress} with an unresolved host name.
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
    public static InetSocketAddress parseUnresolved(String address, ConfigCheck check) {
        try {
            return doParse(address, check, false);
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
    public static String toFileName(InetSocketAddress address) {
        return FILE_PREFIX + address.getAddress().getHostAddress() + FILE_SEPARATOR + address.getPort();
    }

    /**
     * Parses an address from the specified file name which was constructed via {@link #toFileName(InetSocketAddress)}. Returns
     * {@code null} if address could not be parsed due to invalid file format or if host address can't be {@link UnknownHostException
     * resolved}.
     *
     * @param name File name.
     * @param log Optional logger for errors logging.
     *
     * @return Address or {@code null} if parsing failed or parsed host is {@link UnknownHostException unknown}.
     */
    public static InetSocketAddress fromFileName(String name, Logger log) {
        if (name.startsWith(FILE_PREFIX) && name.length() > FILE_PREFIX.length()) {
            String[] tokens = name.substring(FILE_PREFIX.length()).split(FILE_SEPARATOR);

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

    /**
     * Returns the {@link InetAddress#getHostAddress() host address} of a {@link InetSocketAddress#getAddress() socket address}.
     *
     * @param addr Address.
     *
     * @return Host address.
     */
    public static String host(InetSocketAddress addr) {
        return addr != null ? addr.getAddress().getHostAddress() : null;
    }

    private static InetSocketAddress doParse(String address, ConfigCheck check, boolean resolve) throws UnknownHostException {
        address = Utils.nullOrTrim(address);

        if (address == null) {
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
