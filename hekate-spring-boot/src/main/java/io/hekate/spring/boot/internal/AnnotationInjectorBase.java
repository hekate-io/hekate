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

package io.hekate.spring.boot.internal;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import static org.springframework.core.annotation.AnnotationUtils.findAnnotation;

public abstract class AnnotationInjectorBase<A extends Annotation> implements BeanFactoryPostProcessor {
    private final Class<A> annotationType;

    private final Class<?> beanType;

    public AnnotationInjectorBase(Class<A> annotationType, Class<?> beanType) {
        this.annotationType = annotationType;
        this.beanType = beanType;
    }

    protected abstract String injectedBeanName(A annotation);

    protected abstract Object qualifierValue(A annotation);

    protected abstract void configure(BeanDefinitionBuilder builder, A annotation);

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        BeanDefinitionRegistry beanRegistry = (BeanDefinitionRegistry)beanFactory;

        Set<Class<?>> processedClasses = new HashSet<>();

        for (String defName : beanFactory.getBeanDefinitionNames()) {
            Class<?> beanClass = beanFactory.getType(defName);

            if (beanClass != null) {
                if (processedClasses.add(beanClass)) {
                    Set<A> annotations = new HashSet<>();

                    ReflectionUtils.doWithFields(beanClass, field ->
                        Optional.ofNullable(findAnnotation(field, annotationType)).ifPresent(annotations::add)
                    );

                    for (Constructor<?> constructor : beanClass.getDeclaredConstructors()) {
                        Optional.ofNullable(findAnnotation(constructor, annotationType)).ifPresent(annotations::add);

                        for (Annotation[] paramAnnotations : constructor.getParameterAnnotations()) {
                            for (Annotation paramAnnotation : paramAnnotations) {
                                Optional.ofNullable(AnnotationUtils.getAnnotation(paramAnnotation, annotationType))
                                    .ifPresent(annotations::add);
                            }
                        }
                    }

                    ReflectionUtils.doWithMethods(beanClass, method -> {
                        Optional.ofNullable(findAnnotation(method, annotationType)).ifPresent(annotations::add);

                        for (Annotation[] paramAnnotations : method.getParameterAnnotations()) {
                            for (Annotation paramAnnotation : paramAnnotations) {
                                Optional.ofNullable(AnnotationUtils.getAnnotation(paramAnnotation, annotationType))
                                    .ifPresent(annotations::add);
                            }
                        }
                    });

                    if (!annotations.isEmpty()) {
                        annotations.forEach(annotation -> {
                            String injectedBeanName = injectedBeanName(annotation);

                            if (!beanRegistry.isBeanNameInUse(injectedBeanName)) {
                                BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(beanType);

                                configure(builder, annotation);

                                AbstractBeanDefinition injectorDef = builder.getBeanDefinition();

                                Object qualifierVal = qualifierValue(annotation);

                                AutowireCandidateQualifier qualifier;

                                if (qualifierVal == null) {
                                    qualifier = new AutowireCandidateQualifier(annotationType);
                                } else {
                                    qualifier = new AutowireCandidateQualifier(annotationType, qualifierVal);
                                }

                                customize(qualifier, annotation);

                                injectorDef.addQualifier(qualifier);

                                beanRegistry.registerBeanDefinition(injectedBeanName, injectorDef);
                            }
                        });
                    }
                }
            }
        }
    }

    protected void customize(AutowireCandidateQualifier qualifier, A annotation) {
        // No-op.
    }
}
