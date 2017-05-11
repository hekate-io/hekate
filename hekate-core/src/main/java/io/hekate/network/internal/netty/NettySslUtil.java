package io.hekate.network.internal.netty;

import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.resource.ResourceLoadingException;
import io.hekate.core.resource.ResourceService;
import io.hekate.network.NetworkSslConfig;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

final class NettySslUtil {
    private NettySslUtil() {
        // No-op.
    }

    public static Throwable unwrap(Throwable err) {
        if (err instanceof DecoderException && err.getCause() != null && err.getCause() instanceof SSLException) {
            return err.getCause();
        }

        return err;
    }

    public static SslContext clientContext(NetworkSslConfig cfg, ResourceService res) {
        ConfigCheck check = checkConfig(cfg);

        try {
            return SslContextBuilder.forClient()
                .sslProvider(provider(cfg))
                .trustManager(trustManager(cfg, res))
                .sessionCacheSize(cfg.getSslSessionCacheSize())
                .sessionTimeout(cfg.getSslSessionCacheTimeout())
                .build();
        } catch (ResourceLoadingException | GeneralSecurityException | IOException e) {
            throw check.fail(e);
        }
    }

    public static SslContext serverContext(NetworkSslConfig cfg, ResourceService res) {
        ConfigCheck check = checkConfig(cfg);

        try {
            return SslContextBuilder.forServer(keyManager(cfg, res))
                .sslProvider(provider(cfg))
                .trustManager(trustManager(cfg, res))
                .sessionCacheSize(cfg.getSslSessionCacheSize())
                .sessionTimeout(cfg.getSslSessionCacheTimeout())
                .build();
        } catch (ResourceLoadingException | GeneralSecurityException | IOException e) {
            throw check.fail(e);
        }
    }

    private static KeyManagerFactory keyManager(NetworkSslConfig cfg, ResourceService res) throws GeneralSecurityException, IOException,
        ResourceLoadingException {
        KeyManagerFactory factory;

        if (cfg.getKeyStoreAlgorithm() == null || cfg.getKeyStoreAlgorithm().isEmpty()) {
            factory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        } else {
            factory = KeyManagerFactory.getInstance(cfg.getKeyStoreAlgorithm());
        }

        KeyStore store = keyStore(cfg.getKeyStorePath(), cfg.getKeyStorePassword(), cfg.getKeyStoreType(), res);

        factory.init(store, cfg.getKeyStorePassword().toCharArray());

        return factory;
    }

    private static TrustManagerFactory trustManager(NetworkSslConfig cfg, ResourceService resources) throws GeneralSecurityException,
        IOException, ResourceLoadingException {
        if (cfg.getTrustStorePath() == null || cfg.getTrustStorePath().isEmpty()) {
            return InsecureTrustManagerFactory.INSTANCE;
        } else {
            TrustManagerFactory factory;

            if (cfg.getTrustStoreAlgorithm() == null || cfg.getTrustStoreAlgorithm().isEmpty()) {
                factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            } else {
                factory = TrustManagerFactory.getInstance(cfg.getTrustStoreAlgorithm());
            }

            KeyStore store = keyStore(cfg.getTrustStorePath(), cfg.getTrustStorePassword(), cfg.getTrustStoreType(), resources);

            factory.init(store);

            return factory;
        }
    }

    private static KeyStore keyStore(String path, String password, String type, ResourceService resources) throws IOException,
        GeneralSecurityException, ResourceLoadingException {
        assert path != null : "Key store path null.";
        assert password != null : "Key store password is null.";
        assert resources != null : "Resource service is null.";

        KeyStore store;

        if (type == null || type.isEmpty()) {
            store = KeyStore.getInstance(KeyStore.getDefaultType());
        } else {
            store = KeyStore.getInstance(type);
        }

        try (InputStream bytes = resources.load(path)) {
            store.load(bytes, password.toCharArray());
        }

        return store;
    }

    private static ConfigCheck checkConfig(NetworkSslConfig cfg) {
        ConfigCheck check = ConfigCheck.get(NetworkSslConfig.class);

        check.notNull(cfg.getProvider(), "provider");
        check.notEmpty(cfg.getKeyStorePath(), "key store path");
        check.notEmpty(cfg.getKeyStorePassword(), "key store password");

        if (cfg.getTrustStorePath() != null && !cfg.getTrustStorePath().isEmpty()) {
            check.notEmpty(cfg.getTrustStorePassword(), "trust store password");
        }

        return check;
    }

    private static SslProvider provider(NetworkSslConfig cfg) {
        switch (cfg.getProvider()) {
            case AUTO: {
                return OpenSsl.isAvailable() ? SslProvider.OPENSSL : SslProvider.JDK;
            }
            case JDK: {
                return SslProvider.JDK;
            }
            case OPEN_SSL: {
                return SslProvider.OPENSSL;
            }
            default: {
                throw new IllegalArgumentException("Unexpected SSL provider: " + cfg.getProvider());
            }
        }
    }
}