-- Table : user
-- Type : alter
BEGIN;
ALTER TABLE "user" ADD COLUMN IF NOT EXISTS "register_time" timestamp NOT NULL;
ALTER TABLE "user" ADD COLUMN IF NOT EXISTS "password" varchar(1000) NOT NULL DEFAULT '';
ALTER TABLE "user" ADD COLUMN IF NOT EXISTS "status" smallint NOT NULL DEFAULT 0;
COMMIT;