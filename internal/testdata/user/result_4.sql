-- Table : user
-- Type : alter
ALTER TABLE `user`
CHANGE `id` `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
CHANGE `email` `email` varchar(100) NOT NULL DEFAULT '',
CHANGE `password` `password` varchar(255) NOT NULL DEFAULT '',
CHANGE `status` `status` int(10) unsigned NOT NULL DEFAULT 1;