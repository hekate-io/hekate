/*
 * Copyright 2021 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

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
