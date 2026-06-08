package com.cascada.sparkconfig.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * The derivation is a pure domain function and must stay framework-free: no Spark on the classpath,
 * no Jackson (the golden test parses JSON, but the production derivation must not), no Spring. This
 * is what lets the golden test run in milliseconds without a cluster (ARCHITECTURE §8, §11).
 */
class SparkConfigDomainPurityTest {

    private static JavaClasses domainClasses;

    @BeforeAll
    static void importDomainClasses() {
        domainClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("com.cascada.sparkconfig.domain");
    }

    @Test
    void derivationDomainDependsOnlyOnTheJdk() {
        noClasses()
                .should().dependOnClassesThat().resideOutsideOfPackages("com.cascada..", "java..")
                .check(domainClasses);
    }
}
