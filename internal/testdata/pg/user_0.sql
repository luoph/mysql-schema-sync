CREATE TABLE "user" (
  "id" bigint NOT NULL DEFAULT nextval('user_id_seq'::regclass),
  "email" varchar(1000) NOT NULL DEFAULT '',
  "register_time" timestamp NOT NULL,
  "password" varchar(1000) NOT NULL DEFAULT '',
  "status" smallint NOT NULL DEFAULT 0,
  CONSTRAINT "user_pkey" PRIMARY KEY ("id")
)