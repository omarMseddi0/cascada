package com.cascada.sql.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Guards the one boundary that gives this module its shape: <b>Apache Calcite may exist only inside
 * {@code adapter/}.</b>
 *
 * <p>Calcite is this module's entire reason to exist and also its biggest risk. A {@code SqlNode} is
 * expressive and tempting, and the natural way to write a "helper" is to pass one around — at which point
 * the parser's type system has leaked into the domain, and the cache can no longer be canonicalised by any
 * other means. The rules below make that leak a failing build instead of a discovery made two years later.
 *
 * <p>Before the restructure this module had no such boundary at all: everything lived in technical packages
 * ({@code calcite/}, {@code canonical/}, {@code rewrite/}, {@code translate/}) that said nothing about which
 * side of the hexagon a class was on, so there was nothing to enforce.
 */
class SqlHexagonBoundaryTest {

    private static JavaClasses moduleClasses;

    @BeforeAll
    static void importClasses() {
        moduleClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("com.cascada.sql");
    }

    @Test
    void calciteAppearsOnlyInAdapters() {
        noClasses().that().resideInAPackage("com.cascada.sql.domain..")
                .should().dependOnClassesThat().resideInAPackage("org.apache.calcite..")
                .because("a SqlNode must never escape the adapter; the domain describes intent, not syntax")
                .check(moduleClasses);
    }

    @Test
    void theDomainDependsOnlyOnTheJdkAndCascadaValueObjects() {
        noClasses().that().resideInAPackage("com.cascada.sql.domain..")
                .should().dependOnClassesThat().resideOutsideOfPackages("com.cascada..", "java..")
                .check(moduleClasses);
    }

    /**
     * The direction check across module boundaries. {@code lib_sql} is an <em>outer</em> module: it may
     * depend on the cache's domain and on its ports (it implements two of them), but it must never reach
     * into the cache's application services or its adapters. Doing so would couple two adapter modules
     * together and route execution around the ports entirely.
     */
    @Test
    void doesNotReachIntoTheCachesServicesOrAdapters() {
        noClasses().that().resideInAPackage("com.cascada.sql..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "com.cascada.cache.application.service..",
                        "com.cascada.cache.adapter..")
                .because("this module implements the cache's ports; it must not know how the cache is built")
                .check(moduleClasses);
    }
}
