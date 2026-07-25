package com.cascada.spark.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Guards the claim this module's design rests on: <b>the Spark session configuration is a pure value, and
 * only the executor adapter touches Spark.</b>
 *
 * <p>That claim is what makes "local and cluster are the same code" verifiable. The whole configuration
 * contract — Delta always enabled, {@code spark.json} layering, the precedence env &gt; spark.json &gt;
 * defaults — is unit-tested in milliseconds with no Spark on the test path, and it can only stay that way if
 * no Spark type and no environment read creeps into {@code domain/}.
 *
 * <p>The second rule below is the one that would have caught a real defect: the config builder used to
 * default its lookup to {@code System::getenv}, so a class documented as pure read the operating system and
 * could produce different output on two machines.
 */
class SparkHexagonBoundaryTest {

    private static JavaClasses moduleClasses;

    @BeforeAll
    static void importClasses() {
        moduleClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("com.cascada.spark");
    }

    @Test
    void sparkTypesAppearOnlyInAdapters() {
        noClasses().that().resideInAnyPackage(
                        "com.cascada.spark.domain..",
                        "com.cascada.spark.application..")
                .should().dependOnClassesThat().resideInAnyPackage("org.apache.spark..", "io.delta..")
                .because("the configuration contract must be testable without a Spark runtime")
                .check(moduleClasses);
    }

    @Test
    void onlyTheAdapterLayerReadsTheOperatingSystem() {
        noClasses().that().resideOutsideOfPackages("com.cascada.spark.adapter..")
                .should().callMethod(System.class, "getenv", String.class)
                .because("a pure config builder that reads the OS is not pure; that is what EnvironmentPort is for")
                .check(moduleClasses);
    }

    @Test
    void domainDoesNotDependOnTheAdapterLayer() {
        noClasses().that().resideInAPackage("com.cascada.spark.domain..")
                .should().dependOnClassesThat().resideInAPackage("com.cascada.spark.adapter..")
                .check(moduleClasses);
    }
}
