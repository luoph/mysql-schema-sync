CREATE TABLE "user" (
  "id" bigint NOT NULL DEFAULT nextval('user_id_seq'::regclass),
  "email" varchar(100) NOT NULL DEFAULT '',
  "register_time" timestamp NOT NULL,
  "password" varchar(255) NOT NULL DEFAULT '',
  "status" integer NOT NULL DEFAULT 1,
  CONSTRAINT "user_pkey" PRIMARY KEY ("id")
)