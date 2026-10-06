/*
 * Copyright © 2024-2026 Lolaf.org
 *
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
package org.lolaf.betty.api.settings;

import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InstanceProviderTest {

    /**
     * An SPI of this test, registered in {@code META-INF/services} of the test resources.
     */
    public interface GreetingFactory extends Factory<String, String> {
    }

    public static class EnglishGreetingFactory implements GreetingFactory {

        @Override
        public String newInstance(String name) {
            return "Hello " + name;
        }
    }

    /**
     * An SPI nothing implements.
     */
    public interface UnimplementedFactory extends Factory<String, String> {
    }

    @Test
    void buildsThroughTheRegisteredFactory() {
        assertThat(InstanceProvider.getSpiInstance("betty", GreetingFactory.class)).isEqualTo("Hello betty");
    }

    /**
     * As on a thread of the JDK's HttpServer in a Spring Boot fat jar: its context class loader is the system one,
     * which holds neither the SPI nor its implementation.
     */
    @Test
    void findsTheFactoryWhenTheContextClassLoaderCannotSeeIt() throws Exception {
        String[] built = new String[1];
        Thread thread = new Thread(() -> built[0] = InstanceProvider.getSpiInstance("betty", GreetingFactory.class));
        thread.setContextClassLoader(new ClassLoader(null) {
        });
        thread.start();
        thread.join();

        assertThat(built[0]).isEqualTo("Hello betty");
    }

    @Test
    void failsWhenNoImplementationIsRegistered() {
        assertThatThrownBy(() -> InstanceProvider.getSpiInstance("betty", UnimplementedFactory.class))
                .isInstanceOf(NoSuchElementException.class);
    }
}
