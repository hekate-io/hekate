package io.hekate.spring.boot.codec;

import io.hekate.codec.CodecFactory;
import io.hekate.codec.JdkCodecFactory;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for {@link JdkCodecFactory}.
 *
 * <p>
 * This auto-configuration is enabled by default. If multiple implementations of the {@link CodecFactory} interface exist on the classpath,
 * then it is possible to enforce usage of {@link JdkCodecFactory} by setting the {@code 'hekate.codec'} property to {@code jdk} in the
 * application's configuration.
 * </p>
 *
 * <p>
 * This auto-configuration doesn't provide any additional configuration properties.
 * </p>
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnMissingBean(CodecFactory.class)
@ConditionalOnProperty(name = "hekate.codec", havingValue = "jdk", matchIfMissing = true)
public class HekateJdkCodecConfigurer {
    /**
     * Constructs a new instance of {@link JdkCodecFactory}.
     *
     * @return Codec factory.
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate.codec.jdk")
    public JdkCodecFactory<Object> jdkCodecFactory() {
        return new JdkCodecFactory<>();
    }
}
