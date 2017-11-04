package io.hekate.network.netty;

import io.netty.channel.Channel;
import java.util.Optional;

/**
 * Support interface for objects that has an internal Netty {@link Channel}.
 */
public interface NettyChannelSupport {
    /**
     * Returns an internal Netty channel.
     *
     * @return Netty channel.
     */
    Optional<Channel> nettyChannel();

    /**
     * Shortcut method to check if the specified object implements the {@link NettyChannelSupport} interface and returns its {@link
     * #nettyChannel() Netty channel}.
     *
     * @param obj Object to unwrap.
     *
     * @return Netty channel (if object is of {@link NettyChannelSupport} type and has a {@link #nettyChannel() channel}).
     */
    static Optional<Channel> unwrap(Object obj) {
        if (obj instanceof NettyChannelSupport) {
            return ((NettyChannelSupport)obj).nettyChannel();
        }

        return Optional.empty();
    }
}
