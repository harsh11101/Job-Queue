package com.github.pants.jobqueue.domain;

/**
 * Result produced by a job executor.
 *
 * @param resultJson serialized result written to the jobs table.
 * @param sideEffectCreated true when this attempt created the guarded side effect.
 */
public record JobExecutionResult(String resultJson, boolean sideEffectCreated) {
}
