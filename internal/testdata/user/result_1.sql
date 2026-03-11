-- Table : user
-- Type : alter
ALTER TABLE `user`
ADD `register_time` timestamp NOT NULL AFTER `email`,
ADD `password` varchar(1000) NOT NULL DEFAULT '' AFTER `register_time`,
ADD `status` tinyint unsigned NOT NULL DEFAULT '0' AFTER `password`;