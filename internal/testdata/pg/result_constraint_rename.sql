-- Table : user_audio
-- Type : alter
BEGIN;
ALTER TABLE "user_audio" ADD CONSTRAINT "user_audio_user_id_key" UNIQUE ("user_id");
ALTER TABLE "user_audio" DROP CONSTRAINT "user_audio_uid_uniq";
COMMIT;
