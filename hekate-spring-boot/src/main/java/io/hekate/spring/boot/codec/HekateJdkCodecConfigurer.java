package io.hekate.spring.boot.codec;

import io.hekate.codec.JdkCodecFactory;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for {@link JdkCodecFactory}.
 *
 * <h2>Configuration</h2>
 * <p>
 * This auto-configuration can be enabled by setting the {@code 'hekate.codec'} property to {@code 'jdk'} in the application's
 * configuration.
 * </p>
 *
 * <p>
 * This auto-configuration doesn't provide any additional configuration properties.
 * </p>
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnProperty(name = "hekate.codec", havingValue = "jdk")
public class HekateJdkCodecConfigurer {
    /**
     * Constructs a new instance of {@link JdkCodecFactory}.
     *
     * @return Codec factory.
     */
    @Bean
    @Qualifier("default")
    @ConfigurationProperties(prefix = "hekate.codec.jdk")
    public JdkCodecFactory<Object> jdkCodecFactory() {
        return new JdkCodecFactory<>();
    }
}
