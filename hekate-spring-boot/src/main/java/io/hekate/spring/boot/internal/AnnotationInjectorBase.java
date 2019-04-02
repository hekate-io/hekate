/*
 * Copyright 2019 The Hekate Project
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

package io.hekate.spring.boot.internal;

import io.hekate.core.internal.util.ArgAssert;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.core.ResolvableType;
import org.springframework.util.ReflectionUtils;

import static org.springframework.core.annotation.AnnotationUtils.findAnnotation;
import static org.springframework.core.annotation.AnnotationUtils.getAnnotation;

public abstract class AnnotationInjectorBase<A extends Annotation> implements BeanFactoryPostProcessor {
    private final Class<A> annotationType;

    private final Class<?> injectableType;

    public AnnotationInjectorBase(Class<A> annotationType, Class<?> injectableType) {
        ArgAssert.notNull(annotationType, "annotation type");
        ArgAssert.notNull(injectableType, "injectable type");

        this.annotationType = annotationType;
        this.injectableType = injectableType;
    }

    protected abstract void registerBeans(A annotation, ResolvableType targetType, BeanDefinitionRegistry registry);

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        BeanDefinitionRegistry beanRegistry = (BeanDefinitionRegistry)beanFactory;

        Set<Class<?>> uniqueTypes = new HashSet<>();

        for (String name : beanFactory.getBeanDefinitionNames()) {
            Class<?> beanClass = beanFactory.getType(name);

            if (beanClass != null) {
                if (uniqueTypes.add(beanClass)) {
                    // Collect annotations from all of the class members.
                    List<Map.Entry<ResolvableType, A>> annotations = new ArrayList<>();

                    // 1. Fields.
                    ReflectionUtils.doWithFields(beanClass, field -> {
                        if (isInjectableType(field.getType())) {
                            Optional.ofNullable(findAnnotation(field, annotationType)).ifPresent(a ->
                                annotations.add(new SimpleEntry<>(ResolvableType.forField(field), a))
                            );
                        }
                    });

                    // 2. Constructors.
                    for (Constructor<?> ctor : beanClass.getDeclaredConstructors()) {
                        Annotation[][] params = ctor.getParameterAnnotations();

                        for (int i = 0; i < params.length; i++) {
                            int idx = i;

                            for (Annotation param : params[i]) {
                                if (isInjectableType(ctor.getParameterTypes()[i])) {
                                    Optional.ofNullable(getAnnotation(param, annotationType)).ifPresent(a ->
                                        annotations.add(new SimpleEntry<>(ResolvableType.forConstructorParameter(ctor, idx), a))
                                    );
                                }
                            }
                        }
                    }

                    // 3. Methods.
                    ReflectionUtils.doWithMethods(beanClass, meth -> {
                        // Annotation that is declared at the method level.
                        Optional.ofNullable(findAnnotation(meth, annotationType)).ifPresent(a -> {
                            Class<?>[] params = meth.getParameterTypes();

                            for (int i = 0; i < params.length; i++) {
                                if (isInjectableType(params[i])) {
                                    annotations.add(new SimpleEntry<>(ResolvableType.forMethodParameter(meth, i), a));
                                }
                            }
                        });

                        // Annotations that are declared at the method's parameter level.
                        Annotation[][] params = meth.getParameterAnnotations();

                        for (int i = 0; i < params.length; i++) {
                            int idx = i;

                            for (Annotation param : params[i]) {
                                if (isInjectableType(meth.getParameterTypes()[i])) {
                                    Optional.ofNullable(getAnnotation(param, annotationType)).ifPresent(a ->
                                        annotations.add(new SimpleEntry<>(ResolvableType.forMethodParameter(meth, idx), a))
                                    );
                                }
                            }
                        }
                    });

                    // Register beans for collected annotations.
                    if (!annotations.isEmpty()) {
                        annotations.forEach(e ->
                            registerBeans(e.getValue(), e.getKey(), beanRegistry)
                        );
                    }
                }
            }
        }
    }

    private boolean isInjectableType(Class<?> type) {
        return injectableType.isAssignableFrom(type);
    }
}
