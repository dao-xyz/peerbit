import type { SupabaseClient } from "@supabase/supabase-js";

export type RenewalAvailabilityRecovery = {
  recovered: boolean;
  error?: string;
};

/**
 * Reopen renewal challenge issuance only while the exact post-consumption
 * reservation still owns the active row. A release, cleanup transition, fresh
 * challenge, or competing recovery changes one of these predicates and makes
 * this a no-op.
 */
export async function recoverConsumedRenewalAvailability(
  supabase: SupabaseClient,
  lease: { id: string; reservedUpdatedAt: string },
  now = new Date(),
): Promise<RenewalAvailabilityRecovery> {
  const availableAt = now.toISOString();
  try {
    const { data, error } = await supabase
      .from("dns_leases")
      .update({ verify_available_at: availableAt })
      .eq("id", lease.id)
      .eq("status", "active")
      .eq("updated_at", lease.reservedUpdatedAt)
      .is("challenge_id", null)
      .is("challenge_token_hash", null)
      .is("challenge_expires_at", null)
      .gt("lease_expires_at", availableAt)
      .select("id")
      .maybeSingle<{ id: string }>();
    if (error) return { recovered: false, error: error.message };
    return { recovered: Boolean(data) };
  } catch (error) {
    return {
      recovered: false,
      error: error instanceof Error
        ? error.message
        : "Renewal availability recovery failed",
    };
  }
}
