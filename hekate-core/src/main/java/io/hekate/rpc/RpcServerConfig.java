package io.hekate.rpc;

import io.hekate.util.format.ToString;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * RPC server configuration.
 *
 * @see RpcService
 * @see RpcServiceFactory#setServers(List)
 */
public class RpcServerConfig {
    private Object handler;

    private Set<String> tags;

    /**
     * Returns the RPC handler (see {@link #setHandler(Object)}).
     *
     * @return Java object that implements one or more @{@link Rpc}-annotated interfaces.
     */
    public Object getHandler() {
        return handler;
    }

    /**
     * Sets the RPC handler.
     *
     * <p>
     * RPC handler is a Java object that implements one or more @{@link Rpc}-annotated interfaces. Methods of those
     * interfaces will be exposed to the cluster and will be proxied to this instance.
     * </p>
     *
     * @param handler Java object that implements one or more @{@link Rpc}-annotated interfaces.
     */
    public void setHandler(Object handler) {
        this.handler = handler;
    }

    /**
     * Fluent-style version of {@link #setHandler(Object)}.
     *
     * @param handler Java object that implements one or more @{@link Rpc}-annotated interfaces.
     *
     * @return This instance.
     */
    public RpcServerConfig withHandler(Object handler) {
        setHandler(handler);

        return this;
    }

    /**
     * Returns the set of tags of this server (see {@link #setTags(Set)}).
     *
     * @return Tags of this server.
     */
    public Set<String> getTags() {
        return tags;
    }

    /**
     * Sets tags that should be attached to this RPC server.
     *
     * <p>
     * Tags make it possible to expose multiple implementations (or different configuration) of the same RPC interface as RPC
     * servers. In such case each such server must be registered with a distinct tag.
     * </p>
     *
     * <p>
     * Such tag can be specified on the RPC client side in order to disambiguate selection of the target server
     * (see {@link RpcService#clientFor(Class, String)} method) .
     * </p>
     *
     * <p>
     * Note that tags are optional and if not specified then RPC clients should also be constructed without any tags in order to
     * be able discover this server (see {@link RpcService#clientFor(Class)} method).
     * </p>
     *
     * @param tags Tags of this server.
     */
    public void setTags(Set<String> tags) {
        this.tags = tags;
    }

    /**
     * Fluent-style version of {@link #setTags(Set)}.
     *
     * @param tag Tag of this server.
     *
     * @return This instance.
     */
    public RpcServerConfig withTag(String tag) {
        if (this.tags == null) {
            this.tags = new HashSet<>();
        }

        this.tags.add(tag);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
