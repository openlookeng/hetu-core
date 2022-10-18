/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.base.security;

import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Key;
import com.google.inject.MembersInjector;
import com.google.inject.Module;
import com.google.inject.PrivateBinder;
import com.google.inject.Provider;
import com.google.inject.Scope;
import com.google.inject.Stage;
import com.google.inject.TypeLiteral;
import com.google.inject.binder.AnnotatedBindingBuilder;
import com.google.inject.binder.AnnotatedConstantBindingBuilder;
import com.google.inject.binder.LinkedBindingBuilder;
import com.google.inject.matcher.Matcher;
import com.google.inject.spi.Dependency;
import com.google.inject.spi.Message;
import com.google.inject.spi.ModuleAnnotatedMethodScanner;
import com.google.inject.spi.ProvisionListener;
import com.google.inject.spi.TypeConverter;
import com.google.inject.spi.TypeListener;
import org.aopalliance.intercept.MethodInterceptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public class AllowAllAccessControlModuleTest
{
    private AllowAllAccessControlModule allowAllAccessControlModuleUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        AllowAllAccessControl allowAllAccessControl = new AllowAllAccessControl();
        allowAllAccessControlModuleUnderTest = new AllowAllAccessControlModule();
    }

    @Test
    public void testConfigure() throws Exception
    {
        // Setup
        final Binder binder = new Binder() {
            @Override
            public void bindInterceptor(Matcher<? super Class<?>> classMatcher, Matcher<? super Method> methodMatcher, MethodInterceptor... interceptors)
            {
            }

            @Override
            public void bindScope(Class<? extends Annotation> annotationType, Scope scope)
            {
            }

            @Override
            public <T> LinkedBindingBuilder<T> bind(Key<T> key)
            {
                return null;
            }

            @Override
            public <T> AnnotatedBindingBuilder<T> bind(TypeLiteral<T> typeLiteral)
            {
                return null;
            }

            @Override
            public <T> AnnotatedBindingBuilder<T> bind(Class<T> type)
            {
                return null;
            }

            @Override
            public AnnotatedConstantBindingBuilder bindConstant()
            {
                return null;
            }

            @Override
            public <T> void requestInjection(TypeLiteral<T> type, T instance)
            {
            }

            @Override
            public void requestInjection(Object instance)
            {
            }

            @Override
            public void requestStaticInjection(Class<?>... types)
            {
            }

            @Override
            public void install(Module module)
            {
            }

            @Override
            public Stage currentStage()
            {
                return null;
            }

            @Override
            public void addError(String message, Object... arguments)
            {
            }

            @Override
            public void addError(Throwable t)
            {
            }

            @Override
            public void addError(Message message)
            {
            }

            @Override
            public <T> Provider<T> getProvider(Key<T> key)
            {
                return null;
            }

            @Override
            public <T> Provider<T> getProvider(Dependency<T> dependency)
            {
                return null;
            }

            @Override
            public <T> Provider<T> getProvider(Class<T> type)
            {
                return null;
            }

            @Override
            public <T> MembersInjector<T> getMembersInjector(TypeLiteral<T> typeLiteral)
            {
                return null;
            }

            @Override
            public <T> MembersInjector<T> getMembersInjector(Class<T> type)
            {
                return null;
            }

            @Override
            public void convertToTypes(Matcher<? super TypeLiteral<?>> typeMatcher, TypeConverter converter)
            {
            }

            @Override
            public void bindListener(Matcher<? super TypeLiteral<?>> typeMatcher, TypeListener listener)
            {
            }

            @Override
            public void bindListener(Matcher<? super Binding<?>> bindingMatcher, ProvisionListener... listeners)
            {
            }

            @Override
            public Binder withSource(Object source)
            {
                return null;
            }

            @Override
            public Binder skipSources(Class... classesToSkip)
            {
                return null;
            }

            @Override
            public PrivateBinder newPrivateBinder()
            {
                return null;
            }

            @Override
            public void requireExplicitBindings()
            {
            }

            @Override
            public void disableCircularProxies()
            {
            }

            @Override
            public void requireAtInjectOnConstructors()
            {
            }

            @Override
            public void requireExactBindingAnnotations()
            {
            }

            @Override
            public void scanModulesForAnnotatedMethods(ModuleAnnotatedMethodScanner scanner)
            {
            }
        };

        // Run the test
        allowAllAccessControlModuleUnderTest.configure(binder);

        // Verify the results
    }
}
