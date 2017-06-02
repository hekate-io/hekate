package io.hekate.spring.boot.codec;

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
 * Kryo integration is provided by the 'hekate-codec-kryo' module and can be imported into the project dependency management system
 * as in the example below:
 * </p>
 * <div class="tabs">
 * <ul>
 * <li><a href="#maven">Maven</a></li>
 * <li><a href="#gradle">Gradle</a></li>
 * <li><a href="#ivy">Ivy</a></li>
 * </ul>
 * <div id="maven">
 * <pre>{@code
 * <dependency>
 *   <groupId>io.hekate</groupId>
 *   <artifactId>hekate-codec-kryo</artifactId>
 *   <version>REPLACE_VERSION</version>
 * </dependency>
 * }</pre>
 * </div>
 * <div id="gradle">
 * <pre>{@code
 * compile group: 'io.hekate', name: 'hekate-codec-kryo', version: 'REPLACE_VERSION'
 * }</pre>
 * </div>
 * <div id="ivy">
 * <pre>{@code
 * <dependency org="io.hekate" name="hekate-codec-kryo" rev="REPLACE_VERSION"/>
 * }</pre>
 * </div>
 * </div>
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
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnClass(KryoCodecFactory.class)
@ConditionalOnMissingBean(CodecFactory.class)
@ConditionalOnProperty(name = "hekate.codec", havingValue = "kryo", matchIfMissing = true)
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
