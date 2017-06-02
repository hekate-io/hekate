package io.hekate.spring.boot.codec;

import io.hekate.codec.CodecFactory;
import io.hekate.codec.fst.FstCodecFactory;
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
 * Auto-configuration for {@link FstCodecFactory}.
 *
 * <h2>Module dependency</h2>
 * <p>
 * FST integration is provided by the 'hekate-codec-fst' module and can be imported into the project dependency management system
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
 *   <artifactId>hekate-codec-fst</artifactId>
 *   <version>REPLACE_VERSION</version>
 * </dependency>
 * }</pre>
 * </div>
 * <div id="gradle">
 * <pre>{@code
 * compile group: 'io.hekate', name: 'hekate-codec-fst', version: 'REPLACE_VERSION'
 * }</pre>
 * </div>
 * <div id="ivy">
 * <pre>{@code
 * <dependency org="io.hekate" name="hekate-codec-fst" rev="REPLACE_VERSION"/>
 * }</pre>
 * </div>
 * </div>
 *
 * <h2>Configuration</h2>
 * <p>
 * This auto-configuration is enabled by default. If multiple implementations of the {@link CodecFactory} interface exist on the classpath,
 * then it is possible to enforce usage of {@link FstCodecFactory} by setting the {@code 'hekate.codec'} property to {@code fst} in the
 * application's configuration.
 * </p>
 *
 * <p>
 * The following properties can be used to customize the auto-configured {@link FstCodecFactory} instance:
 * </p>
 * <ul>
 * <li>{@link FstCodecFactory#setKnownTypes(Map) 'hekate.codec.fst.known-types'}</li>
 * <li>{@link FstCodecFactory#setUseUnsafe(boolean) 'hekate.codec.fst.use-unsafe'}</li>
 * <li>{@link FstCodecFactory#setSharedReferences(Boolean) 'hekate.codec.fst.shared-references'}</li>
 * </ul>
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore({HekateConfigurer.class, HekateKryoCodecConfigurer.class})
@ConditionalOnClass(FstCodecFactory.class)
@ConditionalOnMissingBean(CodecFactory.class)
@ConditionalOnProperty(name = "hekate.codec", havingValue = "fst", matchIfMissing = true)
public class HekateFstCodecConfigurer {
    /**
     * Constructs a new instance of {@link FstCodecFactory}.
     *
     * @return Codec factory.
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate.codec.fst")
    public FstCodecFactory<Object> fstCodecFactory() {
        return new FstCodecFactory<>();
    }
}
