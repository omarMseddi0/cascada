package com.cascada.cache.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Enforces dependency direction and framework boundaries inside the cache module. */
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

    /** Domain code must not depend on application or adapter code. */
    @Test
    void domainDoesNotDependOnApplicationOrAdapter() {
        noClasses().that().resideInAPackage("com.cascada.cache.domain..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "com.cascada.cache.application..",
                        "com.cascada.cache.adapter..")
                .because("the domain is the innermost ring; dependencies may only point inwards")
                .check(moduleClasses);
    }

    /** Application code reaches infrastructure only through outbound ports. */
    @Test
    void applicationDoesNotDependOnAnyAdapter() {
        noClasses().that().resideInAPackage("com.cascada.cache.application..")
                .should().dependOnClassesThat().resideInAPackage("com.cascada.cache.adapter..")
                .because("the application layer must reach infrastructure only through its out-ports")
                .check(moduleClasses);
    }

    /** Adapters depend on ports rather than application service implementations. */
    @Test
    void adapterDoesNotDependOnApplicationService() {
        noClasses().that().resideInAPackage("com.cascada.cache.adapter..")
                .should().dependOnClassesThat().resideInAPackage("com.cascada.cache.application.service..")
                .because("adapters depend on ports, not on application service implementations")
                .check(moduleClasses);
    }

    /** Top-level port types are interfaces; nested contract values remain allowed. */
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

    /** The domain must not depend on frameworks, SQL libraries, or database APIs. */
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

    /** Domain dependencies are limited to cache/identity values and JDK types. */
    @Test
    void domainOnlyDependsOnJdkAndPlatformValueObjects() {
        noClasses()
                .should().dependOnClassesThat().resideOutsideOfPackages(
                        "com.cascada.cache.domain..",
                        "com.cascada.identity.domain..",
                        "java..")
                .check(domainClasses);
    }

    @Test
    void domainTypesAreGroupedBySubdomainAndExceptionsStayWithTheirOwner() {
        boolean hasFlatDomainType = false;
        boolean hasSiblingExceptionType = false;
        for (var type : moduleClasses) {
            hasFlatDomainType |= type.getPackageName().equals("com.cascada.cache.domain");
            hasSiblingExceptionType |= type.getPackageName().equals("com.cascada.cache.exception");
        }
        assertFalse(hasFlatDomainType, "domain types must live in a cohesive domain subpackage");
        assertFalse(hasSiblingExceptionType, "domain exceptions must live beside their owning domain types");
    }

    /** Application dependencies are limited to cache ports/domain, identity values, and the JDK. */
    @Test
    void applicationLayerIsFrameworkFree() {
        noClasses().that().resideInAPackage("com.cascada.cache.application..")
                .should().dependOnClassesThat().resideOutsideOfPackages(
                        "com.cascada.cache.application..",
                        "com.cascada.cache.domain..",
                        "com.cascada.identity.domain..",
                        "java..")
                .because("application code may depend on its ports, domain, identity values, and the JDK")
                .check(moduleClasses);
    }
}
