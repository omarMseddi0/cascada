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
            .withImportOption(location -> !location.asURI().toString().contains("/test-classes/"))
            .importPackages("com.cascada.app");

    @Test
    void productionRecordsLiveInModelPackagesAcrossLibraries() {
        var platform = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .withImportOption(location -> !location.asURI().toString().contains("/test-classes/"))
                .importPackages("com.cascada");
        for (var type : platform) {
            if (type.isRecord()) {
                org.junit.jupiter.api.Assertions.assertTrue(
                        java.util.Arrays.asList(type.getPackageName().split("\\.")).contains("model"),
                        () -> type.getName() + " must belong to its owning layer's model package");
            }
        }
    }

    @Test
    void inboundAdaptersDependOnContractsAndModels() {
        noClasses().that().resideInAPackage("com.cascada.app.adapter.in..")
                .should().dependOnClassesThat().resideOutsideOfPackages(
                        "java..", "com.sun.net.httpserver..", "io.grpc..", "com.google.protobuf..",
                        "com.fasterxml.jackson..", "org.apache.parquet..", "org.apache.hadoop..",
                        "javax.annotation..", "lombok..", "com.google.common.util.concurrent..", "com.cascada.app.adapter.in..", "com.cascada.app.config..", "com.cascada.app.model..", "com.cascada.app.exception..", "com.cascada.app.concurrent..",
                        "com.cascada.cache.application.execution..", "com.cascada.fabric.application.port.in..", "com.cascada.fabric.domain..",
                        "com.cascada.cache.application.port.in..", "com.cascada.cache.application.model..", "com.cascada.cache.domain..")
                .check(CLASSES);
    }

    @Test
    void outboundAdaptersDoNotDependOnCompositionOrServices() {
        noClasses().that().resideInAPackage("com.cascada.app.adapter.out..")
                .should().dependOnClassesThat().resideInAnyPackage(
                        "com.cascada.app.bootstrap..", "com.cascada.cache.application.service..",
                        "com.cascada.fabric.adapter..", "com.cascada.fabric.application.service..",
                        "com.cascada.spark.adapter..", "com.cascada.spark.application.service..")
                .check(CLASSES);
    }

    @Test
    void settingsDoNotDependOnInfrastructure() {
        noClasses().that().resideInAPackage("com.cascada.app.model.config..")
                .and().haveSimpleNameEndingWith("Settings")
                .should().dependOnClassesThat().resideOutsideOfPackages(
                        "java..", "com.cascada.app.model.config..", "com.cascada.cache.application.model.config..")
                .check(CLASSES);
    }
}
