package com.cascada.cache.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Turns the hexagon from a convention into a build failure.
 *
 * <p>This matters more here than in a typical project, because {@code lib_cache} keeps its application
 * core and its adapters (Lettuce, Arrow, DataSketches) in the <em>same Maven module</em>. Maven therefore
 * cannot prevent a domain class from importing Lettuce — the jar is right there on the compile classpath.
 * These rules are the only thing standing in for that missing module boundary, which is exactly why they
 * are asserted rather than merely documented.
 *
 * <p>The rules encode one sentence: <b>"all source code dependencies may only point from the outside
 * inwards."</b> Concretely, with {@code domain} innermost:
 *
 * <pre>
 *   adapter  ──▶  application  ──▶  domain
 *      │                              ▲
 *      └──────────────────────────────┘        (adapters may use the model directly)
 * </pre>
 *
 * and never an arrow in the other direction.
 */
class HexagonalLayeringArchitectureTest {

    private static JavaClasses moduleClasses;
    private static JavaClasses domainClasses;

    @BeforeAll
    static void importClasses() {
        ClassFileImporter importer = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS);
        moduleClasses = importer.importPackages("com.cascada.cache");
        domainClasses = importer.importPackages("com.cascada.cache.domain");
    }

    // --- the dependency rule ---------------------------------------------------------------------

    /**
     * The innermost ring may not know the ring outside it. If the domain could reach the application
     * layer, "the domain is testable on its own" would stop being true, and a use case could be dragged in
     * by a value object.
     */
    @Test
    void domainDoesNotDependOnApplicationOrAdapter() {
        noClasses().that().resideInAPackage("com.cascada.cache.domain..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "com.cascada.cache.application..",
                        "com.cascada.cache.adapter..")
                .because("the domain is the innermost ring; dependencies may only point inwards")
                .check(moduleClasses);
    }

    /**
     * The regression this suite exists for. {@code WarmingOrchestrator} used to import
     * {@code adapter.tracking.QueryPopularityTracker} — an application class reaching outward to a concrete
     * adapter. It compiled, it passed every functional test, and it silently welded a background service to
     * one storage choice. The fix was {@code QueryPopularityPort}; this rule is what stops it coming back.
     */
    @Test
    void applicationDoesNotDependOnAnyAdapter() {
        noClasses().that().resideInAPackage("com.cascada.cache.application..")
                .should().dependOnClassesThat().resideInAPackage("com.cascada.cache.adapter..")
                .because("the application layer must reach infrastructure only through its out-ports")
                .check(moduleClasses);
    }

    /**
     * The mirror rule to the one above. An outbound adapter implements an out-port; it must not bind
     * itself to an application service and thereby make the service implementation part of the adapter
     * contract. The composition root in {@code app} is the one intentional place that wires both sides.
     */
    @Test
    void adapterDoesNotDependOnApplicationService() {
        noClasses().that().resideInAPackage("com.cascada.cache.adapter..")
                .should().dependOnClassesThat().resideInAPackage("com.cascada.cache.application.service..")
                .because("adapters depend on ports, not on application service implementations")
                .check(moduleClasses);
    }

    /**
     * Every <em>top-level</em> type in a port package must be an interface: a port is a contract, and a
     * concrete class sitting there would be an implementation detail that services and adapters would both
     * bind to, defeating the substitution the port exists to allow.
     *
     * <p>Nested types are deliberately exempt, because they are part of the contract rather than an
     * implementation of it — a port's return record ({@code ExecuteCachedQueryUseCase.Result},
     * {@code WarmCacheUseCase.Report}) and its declared exception ({@code CacheSerializationException})
     * describe the vocabulary callers must speak. Requiring those to be interfaces would force pointless
     * indirection, and moving them elsewhere would split one contract across two packages.
     */
    @Test
    void everyTopLevelPortTypeIsAnInterface() {
        classes().that().resideInAnyPackage(
                        "com.cascada.cache.application.port.in..",
                        "com.cascada.cache.application.port.out..")
                .and().areTopLevelClasses()
                .should().beInterfaces()
                .because("a port declares a contract; anything concrete belongs in service/ or adapter/")
                .check(moduleClasses);
    }

    // --- framework purity of the core ------------------------------------------------------------

    /**
     * No framework may enter the domain. The denylist keeps naming libraries that were removed from the
     * build (JSQLParser, Tablesaw) as tripwires: if a future change re-adds one, this fails before it can
     * reach the core.
     */
    @Test
    void domainDoesNotDependOnAnyFramework() {
        noClasses()
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.springframework..",
                        "io.lettuce..",
                        "org.apache.spark..",
                        "org.apache.calcite..",
                        "org.apache.arrow..",
                        "io.fabric8..",
                        "net.sf.jsqlparser..",
                        "software.amazon.awssdk..",
                        "org.rocksdb..",
                        "tech.tablesaw..",
                        "com.fasterxml.jackson..",
                        "javax.persistence..",
                        "jakarta.persistence..",
                        "java.sql..")
                .check(domainClasses);
    }

    /**
     * The strongest statement of purity, and the one that makes the correctness gate possible: the domain
     * imports nothing but the JDK and Cascada's own value objects. That is why the bucket algebra, the merge
     * math and the safety rules run in milliseconds with no cluster.
     */
    @Test
    void domainOnlyDependsOnJdkAndPlatformValueObjects() {
        noClasses()
                .should().dependOnClassesThat().resideOutsideOfPackages(
                        "com.cascada..",
                        "java..")
                .check(domainClasses);
    }

    /**
     * The application layer is framework-free too. It coordinates domain objects through ports, so it has
     * no reason to see a driver, a client, or a parser — and keeping it clean is what lets a use case be
     * tested with plain fakes instead of containers.
     */
    @Test
    void applicationLayerIsFrameworkFree() {
        noClasses().that().resideInAPackage("com.cascada.cache.application..")
                .should().dependOnClassesThat().resideOutsideOfPackages("com.cascada..", "java..")
                .because("use cases coordinate through ports; frameworks belong in adapters")
                .check(moduleClasses);
    }
}
