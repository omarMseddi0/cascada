package com.cascada.fabric.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/**
 * Guards the boundaries this module gained when it was given an application layer.
 *
 * <p>Two leaks are worth failing a build over here, and both existed before:
 * <ul>
 *   <li><b>Fabric8 in the core.</b> The deployer rendered manifests and issued Kubernetes calls in one
 *       class, so there was no line to defend. Now the lifecycle rules live in {@code application/} and must
 *       stay expressible without a Kubernetes client on the classpath.</li>
 *   <li><b>Reading the OS from the innermost ring.</b> Process environment access belongs to the adapter;
 *       the settings reader receives it through a core-owned port.</li>
 * </ul>
 */
class FabricHexagonBoundaryTest {

    private static JavaClasses moduleClasses;

    @BeforeAll
    static void importClasses() {
        moduleClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("com.cascada.fabric");
    }

    @Test
    void kubernetesTypesAppearOnlyInAdapters() {
        noClasses().that().resideInAnyPackage(
                        "com.cascada.fabric.domain..",
                        "com.cascada.fabric.application..")
                .should().dependOnClassesThat().resideInAPackage("io.fabric8..")
                .because("the lifecycle rules must be provable without a Kubernetes client")
                .check(moduleClasses);
    }

    @Test
    void coreDependsOnlyOnItsOwnContractsModelsAndTheJdk() {
        noClasses().that().resideInAnyPackage("com.cascada.fabric.domain..", "com.cascada.fabric.application..")
                .should().dependOnClassesThat().resideOutsideOfPackages(
                        "com.cascada.fabric.domain..", "com.cascada.fabric.application..", "java..")
                .check(moduleClasses);
    }

    @Test
    void domainDoesNotDependOnApplicationOrAdapter() {
        noClasses().that().resideInAPackage("com.cascada.fabric.domain..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "com.cascada.fabric.application..",
                        "com.cascada.fabric.adapter..")
                .because("dependencies point inwards only")
                .check(moduleClasses);
    }

    @Test
    void domainDoesNotPerformResourceOrFilesystemIo() {
        noClasses().that().resideInAPackage("com.cascada.fabric.domain..")
                .should().dependOnClassesThat().resideInAnyPackage("java.io..", "java.nio.file..")
                .because("resource loading belongs in an adapter")
                .check(moduleClasses);
    }

    @Test
    void applicationDoesNotDependOnAnyAdapter() {
        noClasses().that().resideInAPackage("com.cascada.fabric.application..")
                .should().dependOnClassesThat().resideInAPackage("com.cascada.fabric.adapter..")
                .check(moduleClasses);
    }

    /**
     * Only the environment adapter may read the process environment. This is the rule that makes "is this
     * code deterministic?" answerable by grep rather than by reading every file.
     */
    @Test
    void onlyTheAdapterLayerReadsTheOperatingSystem() {
        noClasses().that().resideOutsideOfPackages("com.cascada.fabric.adapter..")
                .should().callMethod(System.class, "getenv", String.class)
                .because("reading the environment is I/O and belongs in an adapter")
                .check(moduleClasses);
    }
}
