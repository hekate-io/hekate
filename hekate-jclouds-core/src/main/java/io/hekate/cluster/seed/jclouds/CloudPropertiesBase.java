package io.hekate.cluster.seed.jclouds;

import java.net.URLConnection;
import java.util.Properties;
import org.jclouds.Constants;

/**
 * Abstract base class for {@code jClouds} configuration properties.
 *
 * @param <T> Sub-class that supports {@code jClouds} properties.
 */
public abstract class CloudPropertiesBase<T extends CloudPropertiesBase<T>> {
    /** See {@link #setConnectTimeout(Integer)}. */
    private Integer connectTimeout;

    /** See {@link #setSoTimeout(Integer)}. */
    private Integer soTimeout;

    /**
     * Returns the connect timeout in milliseconds (see {@link #setConnectTimeout(Integer)}).
     *
     * @return Connect timeout in milliseconds.
     */
    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Sets the connect timeout in milliseconds.
     *
     * @param connectTimeout Connect timeout in milliseconds.
     *
     * @see Constants#PROPERTY_CONNECTION_TIMEOUT
     * @see URLConnection#setConnectTimeout(int)
     */
    public void setConnectTimeout(Integer connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Fluent-style version of {@link #setConnectTimeout(Integer)}.
     *
     * @param connectTimeout Connect timeout in milliseconds.
     *
     * @return This instance.
     */
    public T withConnectTimeout(Integer connectTimeout) {
        setConnectTimeout(connectTimeout);

        return self();
    }

    /**
     * Returns the response read timeout in milliseconds (see {@link #setSoTimeout(Integer)}).
     *
     * @return Response read timeout in milliseconds.
     */
    public Integer getSoTimeout() {
        return soTimeout;
    }

    /**
     * Sets the response read timeout in milliseconds.
     *
     * @param soTimeout Response read in milliseconds.
     *
     * @see Constants#PROPERTY_SO_TIMEOUT
     * @see URLConnection#setReadTimeout(int)
     */
    public void setSoTimeout(Integer soTimeout) {
        this.soTimeout = soTimeout;
    }

    /**
     * Fluent-style version of {@link #setSoTimeout(Integer)}.
     *
     * @param soTimeout Response read timeout.
     *
     * @return This instance.
     */
    public T withSoTimeout(Integer soTimeout) {
        setSoTimeout(soTimeout);

        return self();
    }

    /**
     * Converts this configuration to {@code jClouds}-compliant properties.
     *
     * <p>
     * This method is intentionally {@code final} in order to control how properties of {@link CloudPropertiesBase} class get converted to
     * {@code jClouds}'s properties. Subclasses are free to override properties that are returned by this method.
     * </p>
     *
     * @return A new instance of {@link Properties} with {@code jClouds}-related settings bases on properties of
     * {@link CloudPropertiesBase} class.
     */
    protected final Properties buildBaseProperties() {
        Properties props = new Properties();

        if (connectTimeout != null) {
            props.setProperty(Constants.PROPERTY_CONNECTION_TIMEOUT, String.valueOf(connectTimeout));
        }

        if (soTimeout != null) {
            props.setProperty(Constants.PROPERTY_SO_TIMEOUT, String.valueOf(soTimeout));
        }

        return props;
    }

    @SuppressWarnings("unchecked")
    private T self() {
        return (T)this;
    }
}
