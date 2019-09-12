package io.hekate.javadoc.lock;

import io.hekate.lock.LockRegionConfig;
import io.hekate.spring.boot.EnableHekate;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

// Start:app
@EnableHekate
@SpringBootApplication
public class MyApp {
    @Bean
    public LockRegionConfig lockRegionConfig() {
        return new LockRegionConfig().withName("my-region");
    }

    // ... other beans and methods...
}
// End:app
