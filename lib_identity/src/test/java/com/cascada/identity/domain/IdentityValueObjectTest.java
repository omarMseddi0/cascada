package com.cascada.identity.domain;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdentityValueObjectTest {

    @Nested
    class TenantIdentifierTest {

        @Test
        void acceptsLowercaseAlphanumericWithHyphenAndUnderscore() {
            assertThat(TenantIdentifier.of("acme-corp_01").value()).isEqualTo("acme-corp_01");
            assertThat(TenantIdentifier.of("a").asKeyPrefixSegment()).isEqualTo("a");
        }

        @Test
        void rejectsNullValue() {
            assertThatThrownBy(() -> TenantIdentifier.of(null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must not be null");
        }

        @Test
        void rejectsUppercaseLeadingHyphenAndOverlongValues() {
            assertThatThrownBy(() -> TenantIdentifier.of("Acme")).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> TenantIdentifier.of("-acme")).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> TenantIdentifier.of("a".repeat(64))).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> TenantIdentifier.of("")).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> TenantIdentifier.of("has space")).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void valueEqualityHoldsForSameValue() {
            assertThat(TenantIdentifier.of("acme")).isEqualTo(TenantIdentifier.of("acme"));
            assertThat(TenantIdentifier.of("acme")).isNotEqualTo(TenantIdentifier.of("globex"));
        }
    }

    @Nested
    class SchemaVersionTest {

        @Test
        void initialIsZeroAndNextIncrements() {
            assertThat(SchemaVersion.initial().value()).isZero();
            assertThat(SchemaVersion.initial().next().value()).isEqualTo(1L);
        }

        @Test
        void rejectsNegativeVersion() {
            assertThatThrownBy(() -> SchemaVersion.of(-1)).isInstanceOf(IllegalArgumentException.class);
        }

        @Property
        void nextAlwaysIncreasesByExactlyOne(@ForAll @LongRange(min = 0, max = 1_000_000) long version) {
            assertThat(SchemaVersion.of(version).next().value()).isEqualTo(version + 1);
        }
    }

    @Nested
    class LineageHashTest {

        @Test
        void emptyIsADistinctSentinel() {
            assertThat(LineageHash.empty().value()).isEqualTo("no-lineage");
        }

        @Test
        void rejectsBlankValue() {
            assertThatThrownBy(() -> LineageHash.of("  ")).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> LineageHash.of(null)).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void canonicalSourceVersionStringIsOrderIndependent() {
            String ordered = LineageHash.canonicalSourceVersionString(List.of("table_a:5", "table_b:2"));
            String reversed = LineageHash.canonicalSourceVersionString(List.of("table_b:2", "table_a:5"));
            assertThat(ordered).isEqualTo(reversed).isEqualTo("table_a:5|table_b:2");
        }

        @Test
        void canonicalSourceVersionStringHandlesSingleAndEmpty() {
            assertThat(LineageHash.canonicalSourceVersionString(List.of("only:1"))).isEqualTo("only:1");
            assertThat(LineageHash.canonicalSourceVersionString(List.of())).isEmpty();
        }
    }

    @Nested
    class PolicyVersionTest {

        @Test
        void currentIsVersionFour() {
            assertThat(PolicyVersion.current().value()).isEqualTo(4);
        }

        @Test
        void rejectsVersionsBelowOne() {
            assertThatThrownBy(() -> PolicyVersion.of(0)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> PolicyVersion.of(-3)).isInstanceOf(IllegalArgumentException.class);
        }

        @Property
        void acceptsAnyPositiveVersion(@ForAll @IntRange(min = 1, max = 10_000) int version) {
            assertThat(PolicyVersion.of(version).value()).isEqualTo(version);
        }
    }

    @Nested
    class QueryHashTest {

        @Test
        void acceptsValidMd5Digest() {
            String digest = "0123456789abcdef0123456789abcdef";
            assertThat(QueryHash.of(digest).value()).isEqualTo(digest);
        }

        @Test
        void rejectsNonHexOrWrongLength() {
            assertThatThrownBy(() -> QueryHash.of("XYZ")).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> QueryHash.of("0123456789ABCDEF0123456789abcdef"))
                    .isInstanceOf(IllegalArgumentException.class); // uppercase rejected
            assertThatThrownBy(() -> QueryHash.of("abc")).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> QueryHash.of(null)).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
