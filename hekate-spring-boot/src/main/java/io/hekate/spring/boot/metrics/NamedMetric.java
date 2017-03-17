/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.spring.boot.metrics;

import io.hekate.metrics.Metric;
import io.hekate.metrics.MetricConfigBase;
import io.hekate.metrics.MetricsService;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Provides support for {@link Metric}s autowiring.
 *
 * <p>
 * This bean can be placed on any {@link Autowired autowire}-capable elements (fields, properties, parameters, etc) of application beans in
 * order to inject {@link Metric} by its {@link MetricConfigBase#setName(String) name}.
 * </p>
 *
 * <p>
 * Below is the example of how this annotation can be used.
 * </p>
 *
 * <p>
 * 1) Define a bean that will use {@link NamedMetric} annotation to inject {@link Metric} into its field.
 * ${source:metrics/MetricsInjectionJavadocTest.java#metric_bean}
 * 2) Define a Spring Boot application that will provide metric configuration.
 * ${source:metrics/MetricsInjectionJavadocTest.java#app}
 * </p>
 *
 * @see HekateMetricsServiceConfigurer
 */
@Autowired
@Qualifier
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER, ElementType.ANNOTATION_TYPE})
public @interface NamedMetric {
    /**
     * Specifies the {@link MetricConfigBase#setName(String) name} of a {@link Metric} that should be injected (see {@link
     * MetricsService#metric(String)}).
     *
     * @return Name of a {@link Metric}.
     */
    String value();
}
