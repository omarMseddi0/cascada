package com.cascada.cache.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Enforces the hexagon at build time (replacing the Python {@code import-linter}, ARCHITECTURE §11):
 * the framework-free domain core may import nothing outside the JDK and the platform's own
 * identity/cache value objects — no Spring, Spark, Lettuce, JSqlParser, Tablesaw, JPA, or AWS SDK.
 *
 * <p>If anyone later adds a framework import into {@code com.cascada.cache.domain}, this test fails
 * the build, which is exactly the guardrail that keeps the cache correctness logic unit-testable
 * without a cluster.
 */
class HexagonalLayeringArchitectureTest {

    private static JavaClasses domainClasses;

    @BeforeAll
    static void importDomainClasses() {
        domainClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("com.cascada.cache.domain");
    }

    @Test
    void domainDoesNotDependOnAnyFramework() {
        noClasses()
                .should().dependOnClassesThat().resideInAnyPackage(
                        "org.springframework..",
                        "io.lettuce..",
                        "org.apache.spark..",
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

    @Test
    void domainOnlyDependsOnJdkAndPlatformValueObjects() {
        noClasses()
                .should().dependOnClassesThat().resideOutsideOfPackages(
                        "com.cascada..",
                        "java..")
                .check(domainClasses);
    }
}
