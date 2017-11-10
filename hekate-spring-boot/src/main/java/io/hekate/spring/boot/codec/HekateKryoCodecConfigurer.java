package io.hekate.spring.boot.codec;

import com.esotericsoftware.kryo.Kryo;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.kryo.KryoCodecFactory;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import java.util.Map;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for {@link KryoCodecFactory}.
 *
 * <h2>Module dependency</h2>
 * <p>
 * Kryo integration requires
 * <a href="https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.esotericsoftware%22%20a%3A%22kryo-shaded%22" target="_blank">
 * 'com.esotericsoftware:kryo-shaded'
 * </a>
 * to be on the project's classpath.
 * </p>
 *
 * <h2>Configuration</h2>
 * <p>
 * This auto-configuration is enabled by default. If multiple implementations of the {@link CodecFactory} interface exist on the classpath,
 * then it is possible to enforce usage of {@link KryoCodecFactory} by setting the {@code 'hekate.codec'} property to {@code kryo} in the
 * application's configuration.
 * </p>
 *
 * <p>
 * The following properties can be used to customize the auto-configured {@link KryoCodecFactory} instance:
 * </p>
 * <ul>
 * <li>{@link KryoCodecFactory#setKnownTypes(Map) 'hekate.codec.kryo.known-types'}</li>
 * <li>{@link KryoCodecFactory#setUnsafeIo(boolean) 'hekate.codec.kryo.unsafe-io'}</li>
 * <li>{@link KryoCodecFactory#setReferences(Boolean) 'hekate.codec.kryo.references'}</li>
 * </ul>
 */
@Configuration
@ConditionalOnHekateEnabled
@ConditionalOnClass(Kryo.class)
@ConditionalOnMissingBean(CodecFactory.class)
@AutoConfigureBefore({HekateConfigurer.class, HekateJdkCodecConfigurer.class})
@ConditionalOnProperty(name = "hekate.codec", havingValue = "kryo")
public class HekateKryoCodecConfigurer {
    /**
     * Constructs a new instance of {@link KryoCodecFactory}.
     *
     * @return Codec factory.
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate.codec.kryo")
    public KryoCodecFactory<Object> kryoCodecFactory() {
        return new KryoCodecFactory<>();
    }
}
