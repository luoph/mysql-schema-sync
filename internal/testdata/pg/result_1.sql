-- Table : user
-- Type : alter
-- RelationTables :
-- Comment :
-- SQL :
ALTER TABLE "user" ADD COLUMN "register_time" timestamp NOT NULL;
ALTER TABLE "user" ADD COLUMN "password" varchar(1000) NOT NULL DEFAULT '';
ALTER TABLE "user" ADD COLUMN "status" smallint NOT NULL DEFAULT 0;