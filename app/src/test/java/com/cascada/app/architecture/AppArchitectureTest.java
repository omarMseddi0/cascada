package com.cascada.app.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/** Concrete module implementations may be connected only by the composition root. */
class AppArchitectureTest {
    private static final JavaClasses CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("com.cascada.app");

    @Test
    void inboundAdaptersDependOnContractsAndModels() {
        noClasses().that().resideInAPackage("com.cascada.app.adapter.in..")
                .should().dependOnClassesThat().resideOutsideOfPackages(
                        "java..", "com.cascada.app.adapter.in..",
                        "com.cascada.cache.application.port.in..", "com.cascada.cache.domain..")
                .check(CLASSES);
    }

    @Test
    void outboundAdaptersDoNotDependOnCompositionOrServices() {
        noClasses().that().resideInAPackage("com.cascada.app.adapter.out..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "com.cascada.app.bootstrap..", "com.cascada.cache.application.service..")
                .check(CLASSES);
    }

    @Test
    void settingsDoNotDependOnInfrastructure() {
        noClasses().that().resideInAPackage("com.cascada.app.config..")
                .should().dependOnClassesThat().resideOutsideOfPackages(
                        "java..", "com.cascada.app.config..", "com.cascada.cache.application.config..")
                .check(CLASSES);
    }
}
