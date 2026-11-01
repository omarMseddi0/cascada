package com.cascada.identity.domain;

import com.cascada.identity.domain.model.QueryHash;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdentityValueObjectTest {

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
