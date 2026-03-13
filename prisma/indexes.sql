-- Raw SQL performance indexes
CREATE INDEX IF NOT EXISTS idx_campaigns_merchant_status ON "Campaign"("merchant_id", "status");
CREATE INDEX IF NOT EXISTS idx_campaigns_time ON "Campaign"("start_at", "end_at");
CREATE INDEX IF NOT EXISTS idx_redemptions_token ON "Redemption"("token");
CREATE INDEX IF NOT EXISTS idx_redemptions_expires ON "Redemption"("expires_at");
CREATE INDEX IF NOT EXISTS idx_redemptions_redeemed ON "Redemption"("redeemed");
CREATE INDEX IF NOT EXISTS idx_events_merchant_time ON "Event"("merchant_id", "created_at");
CREATE INDEX IF NOT EXISTS idx_events_name_time ON "Event"("event_name", "created_at");
CREATE INDEX IF NOT EXISTS idx_audit_action_time ON "AuditLog"("action", "created_at");
CREATE INDEX IF NOT EXISTS idx_activations_user_campaign ON "Activation"("user_id", "campaign_id");
CREATE INDEX IF NOT EXISTS idx_qr_public_code ON "QrCode"("public_code");
