package com.cascada.identity.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/** Protects the identity module's value objects from framework and adapter dependencies. */
class IdentityDomainPurityTest {

    private static JavaClasses domainClasses;

    @BeforeAll
    static void importDomainClasses() {
        domainClasses = new ClassFileImporter()
                .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
                .importPackages("com.cascada.identity.domain");
    }

    @Test
    void identityValueObjectsDependOnlyOnTheJdk() {
        noClasses()
                .should().dependOnClassesThat().resideOutsideOfPackages(
                        "com.cascada.identity.domain..", "java..")
                .because("identity value objects are the framework-free inner layer")
                .check(domainClasses);
    }
}
