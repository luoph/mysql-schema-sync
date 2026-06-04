CREATE TABLE "user_audio" (
  "id" bigint NOT NULL,
  "user_id" bigint NOT NULL,
  "audio_url" text NOT NULL,
  CONSTRAINT "user_audio_pkey" PRIMARY KEY ("id"),
  CONSTRAINT "user_audio_user_id_key" UNIQUE ("user_id")
)
