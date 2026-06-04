CREATE TABLE "user_audio" (
  "id" bigint NOT NULL,
  "user_id" bigint NOT NULL,
  "audio_url" text NOT NULL,
  CONSTRAINT "user_audio_pkey" PRIMARY KEY ("id"),
  CONSTRAINT "user_audio_uid_uniq" UNIQUE ("user_id")
)
