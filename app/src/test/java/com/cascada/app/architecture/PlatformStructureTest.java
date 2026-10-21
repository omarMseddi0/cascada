package com.cascada.app.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;

class PlatformStructureTest {
    private static final JavaClasses PLATFORM = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .withImportOption(location -> !location.asURI().toString().contains("/test-classes/"))
            .importPackages("com.cascada");

    @Test
    void authoredTypesHaveTheirOwnFiles() {
        assertThat(PLATFORM.stream()
                .filter(type -> !type.getPackageName().startsWith("com.cascada.app.adapter.in.grpc.protocol"))
                .filter(type -> type.getName().contains("$") && !type.getName().matches(".*\\$\\d+"))
                .map(type -> type.getName())).isEmpty();
    }

    @Test
    void domainDoesNotContainApplicationPoliciesOrAlgorithms() {
        assertThat(PLATFORM.stream().filter(type -> type.getPackageName().contains(".domain"))
                .filter(type -> type.getSimpleName().matches(
                        ".*(Service|Rule|Registry|Planner|Calculator|Pyramid|Aggregator|Merger|Resampler|Deriver|Assembler|Generator|Evaluator|Verifier|Writer|Queue)"))
                .map(type -> type.getName())).isEmpty();
    }

    @Test
    void domainsDoNotDependOnAnyApplicationOrAdapter() {
        noClasses().that().resideInAPackage("..domain..")
                .should().dependOnClassesThat().resideInAnyPackage("..application..", "..adapter..")
                .check(PLATFORM);
    }

    @Test
    void applicationDoesNotDependOnAnyAdapter() {
        noClasses().that().resideInAPackage("..application..")
                .should().dependOnClassesThat().resideInAPackage("..adapter..")
                .check(PLATFORM);
    }

    @Test
    void fabricUsesItsOwnPortsForExternalConfigurationLogic() {
        noClasses().that().resideInAPackage("com.cascada.fabric..")
                .should().dependOnClassesThat().resideInAPackage("com.cascada.sparkconfig.application.service..")
                .check(PLATFORM);
    }
}
