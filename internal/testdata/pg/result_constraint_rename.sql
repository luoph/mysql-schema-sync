-- Table : user_audio
-- Type : alter
BEGIN;
ALTER TABLE "user_audio" DROP CONSTRAINT IF EXISTS "user_audio_uid_uniq";
DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint c JOIN pg_class t ON c.conrelid = t.oid JOIN pg_namespace n ON t.relnamespace = n.oid WHERE n.nspname = 'public' AND t.relname = 'user_audio' AND c.conname = 'user_audio_user_id_key') THEN ALTER TABLE "user_audio" ADD CONSTRAINT "user_audio_user_id_key" UNIQUE ("user_id"); END IF; END $$;
COMMIT;
