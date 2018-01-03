/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.spring.boot;

import io.hekate.core.Hekate;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

/**
 * Health indicator for Spring Boot Actuator.
 *
 * <p>
 * This indicator can be enabled by setting {@code 'management.health.hekate.enabled'} configuration property to {@code true}.
 * </p>
 */
public class HekateHealthIndicator implements HealthIndicator {
    private final Hekate node;

    /**
     * Constructs new instance.
     *
     * @param node Node who's status should be exposed by this indicator.
     */
    public HekateHealthIndicator(Hekate node) {
        this.node = node;
    }

    /**
     * Returns current {@link Hekate#state()} as {@link Health#getStatus()}.
     *
     * @return Health status.
     */
    @Override
    public Health health() {
        Health.Builder health = new Health.Builder();

        switch (node.state()) {
            case DOWN: {
                health.status(Hekate.State.DOWN.name());

                break;
            }
            case INITIALIZING: {
                health.status(Hekate.State.INITIALIZING.name());

                break;
            }
            case INITIALIZED: {
                health.status(Hekate.State.INITIALIZED.name());

                break;
            }
            case JOINING: {
                health.status(Hekate.State.JOINING.name());

                break;
            }
            case UP: {
                health.status(Hekate.State.UP.name());

                break;
            }
            case LEAVING: {
                health.status(Hekate.State.LEAVING.name());

                break;
            }
            case TERMINATING: {
                health.status(Hekate.State.TERMINATING.name());

                break;
            }
            default: {
                health.unknown();
            }
        }

        return health.build();
    }
}
