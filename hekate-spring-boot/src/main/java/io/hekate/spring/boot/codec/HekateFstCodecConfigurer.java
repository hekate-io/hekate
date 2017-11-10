package io.hekate.spring.boot.codec;

import io.hekate.codec.fst.FstCodecFactory;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import java.util.Map;
import org.nustaq.serialization.FSTConfiguration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for {@link FstCodecFactory}.
 *
 * <h2>Module dependency</h2>
 * <p>
 * FST integration requires
 * <a href="https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22de.ruedigermoeller%22%20a%3A%22fst%22" target="_blank">
 * 'de.ruedigermoeller:fst'
 * </a>
 * to be on the project's classpath.
 * </p>
 *
 * <h2>Configuration</h2>
 * <p>
 * This auto-configuration can be enabled by setting the {@code 'hekate.codec'} property to {@code 'fst'} in the application's
 * configuration.
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
@ConditionalOnClass(FSTConfiguration.class)
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnProperty(name = "hekate.codec", havingValue = "fst")
public class HekateFstCodecConfigurer {
    /**
     * Constructs a new instance of {@link FstCodecFactory}.
     *
     * @return Codec factory.
     */
    @Bean
    @Qualifier("default")
    @ConfigurationProperties(prefix = "hekate.codec.fst")
    public FstCodecFactory<Object> fstCodecFactory() {
        return new FstCodecFactory<>();
    }
}
