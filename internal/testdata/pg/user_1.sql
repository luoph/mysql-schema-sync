CREATE TABLE "user" (
  "id" bigint NOT NULL DEFAULT nextval('user_id_seq'::regclass),
  "email" varchar(1000) NOT NULL DEFAULT '',
  CONSTRAINT "user_pkey" PRIMARY KEY ("id")
)