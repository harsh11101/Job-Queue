package com.github.pants.jobqueue.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.springframework.stereotype.Component;

/**
 * SHA-256 hashing helper used for request and side-effect idempotency checks.
 */
@Component
public class HashSupport {

    /**
     * Calculates a lowercase SHA-256 hexadecimal digest.
     *
     * @param value input value.
     * @return SHA-256 hex digest.
     */
    public String sha256(final String value) {
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-256");
            final byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 algorithm is not available", exception);
        }
    }
}
