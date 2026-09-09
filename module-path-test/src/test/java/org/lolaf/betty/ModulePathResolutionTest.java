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
package org.lolaf.betty;

import org.junit.jupiter.api.Test;

import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Every published betty jar must be usable on the module path, not only the classpath.
 *
 * <p>The rule this guards is that a package belongs to exactly one module. Betty satisfies it today —
 * {@code org.lolaf.betty.api} and its sub-packages come from {@code betty-api}, {@code org.lolaf.betty.impl}
 * from {@code betty-impl}, and nothing is shared. That is worth a test rather than a note, because nothing
 * else would catch it breaking: every other test runs on the classpath, where the rule does not exist, and
 * a package added to both jars would only fail for an application with its own {@code module-info} or for
 * anyone running {@code jlink}. Ringos carried exactly that defect undetected for a long time — one of its
 * packages was contributed by six jars — which is why the check is ported here before betty can regress
 * the same way.
 *
 * <p>The resolution happens in-process rather than by forking a JVM, which is the same check the launcher
 * makes: {@link Configuration#resolve} rejects a configuration in which two modules contain the same
 * package.
 *
 * <p>It reads {@code target/module-path}, filled by {@code maven-dependency-plugin} at
 * {@code process-test-classes}, rather than the test classpath — dependencies come through as exploded
 * {@code target/classes} directories on a bare {@code mvn test}, and {@link ModuleFinder} would name
 * every one of them {@code classes}.
 */
class ModulePathResolutionTest {

    private static final Path MODULE_PATH =
            Paths.get(System.getProperty("betty.module.path", "target/module-path"));

    /** The two published jars. Named rather than counted, so adding a third does not silently weaken this. */
    private static final Set<String> PUBLISHED_MODULES =
            Set.of("org.lolaf.betty.api", "org.lolaf.betty.impl");

    private static Set<ModuleReference> modules() {
        assertThat(MODULE_PATH)
                .as("the module path directory should have been filled at process-test-classes")
                .exists();
        Set<ModuleReference> found = ModuleFinder.of(MODULE_PATH).findAll();
        // an empty module path resolves perfectly and proves nothing, so make that a failure
        assertThat(found)
                .as("no betty jars found in %s, so this test would pass vacuously", MODULE_PATH)
                .hasSizeGreaterThanOrEqualTo(PUBLISHED_MODULES.size());
        return found;
    }

    /**
     * The readable half. {@link Configuration#resolve} names one offending pair and stops, so work the
     * whole set out first and report every clash at once.
     */
    @Test
    void everyPackageBelongsToExactlyOneModule() {
        Map<String, Set<String>> owners = new TreeMap<>();
        for (ModuleReference module : modules()) {
            for (String pkg : module.descriptor().packages()) {
                owners.computeIfAbsent(pkg, p -> new TreeSet<>()).add(module.descriptor().name());
            }
        }

        Map<String, Set<String>> split = owners.entrySet().stream()
                .filter(e -> e.getValue().size() > 1)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a, TreeMap::new));

        assertThat(split)
                .as("a package carried by more than one jar cannot be used on the module path")
                .isEmpty();
    }

    /**
     * The real half: the check the JVM itself makes when it builds the boot layer.
     */
    @Test
    void theWholeSetResolvesAsModules() {
        ModuleFinder finder = ModuleFinder.of(MODULE_PATH);
        Set<String> roots = modules().stream()
                .map(m -> m.descriptor().name())
                .collect(Collectors.toSet());

        assertThatCode(() -> Configuration.resolve(
                finder, List.of(ModuleLayer.boot().configuration()), ModuleFinder.of(), roots))
                .doesNotThrowAnyException();
    }

    /**
     * Every jar must carry an explicit {@code Automatic-Module-Name}. Without one the name is derived from
     * the file name, so renaming an artifact would silently rename the module consumers wrote
     * {@code requires} against — and the parent's sentinel default exists so that a module which forgets
     * to set the property fails here rather than shipping a plausible-looking name.
     */
    @Test
    void everyJarDeclaresItsOwnModuleName() {
        assertThat(modules()).extracting(m -> m.descriptor().name())
                .as("a module still named after its jar, or one that forgot to set the property")
                .allSatisfy(name -> assertThat(name).startsWith("org.lolaf.betty").doesNotContain("NOT.SET"));
    }

    /**
     * Guards the reading of the directory itself: a typo in the plugin configuration would leave it
     * empty, and every test above would then pass without checking anything.
     */
    @Test
    void theModulePathHoldsThePublishedBettyJars() throws Exception {
        try (var entries = Files.list(MODULE_PATH)) {
            assertThat(entries.map(p -> p.getFileName().toString()))
                    .allMatch(name -> name.startsWith("betty-") && name.endsWith(".jar"));
        }
        assertThat(modules()).extracting(m -> m.descriptor().name())
                .containsAll(PUBLISHED_MODULES);
    }
}
