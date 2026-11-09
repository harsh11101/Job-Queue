package com.github.pants.jobqueue.service;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;

/**
 * Redis-backed distributed lock helper used to prevent concurrent processing of
 * the same job by multiple worker instances.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DistributedLockService {

    /**
     * Deletes the lock only while it still holds this owner's token. Running the
     * compare and the delete as one script closes the window in which a lock that
     * expired and was taken by another worker could be deleted by the old owner.
     */
    private static final RedisScript<Long> RELEASE_SCRIPT = RedisScript.of("""
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            end
            return 0
            """, Long.class);

    private final StringRedisTemplate redisTemplate;

    /**
     * Runs work only when a Redis lock is acquired.
     *
     * @param lockKey lock key.
     * @param ttl lock time to live.
     * @param work protected work.
     * @return true when work ran, false when another worker already owned the lock.
     */
    public boolean withLock(final String lockKey, final Duration ttl, final Runnable work) {
        final String token = UUID.randomUUID().toString();
        if (!Boolean.TRUE.equals(redisTemplate.opsForValue().setIfAbsent(lockKey, token, ttl))) {
            return false;
        }
        try {
            work.run();
            return true;
        } finally {
            release(lockKey, token);
        }
    }

    private void release(final String lockKey, final String token) {
        try {
            redisTemplate.execute(RELEASE_SCRIPT, List.of(lockKey), token);
        } catch (RuntimeException exception) {
            // The lock still expires after its TTL; never let a release failure hide the work's own outcome.
            log.warn("Failed to release lock {}; it will expire after its TTL", lockKey, exception);
        }
    }
}
