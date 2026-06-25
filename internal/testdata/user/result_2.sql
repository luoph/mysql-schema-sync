-- Table : user
-- Type : alter
BEGIN;
SET @__mss_sql = (SELECT IF(COUNT(*)=0, 'ALTER TABLE `user` ADD `register_time` timestamp NOT NULL AFTER `email`', 'SELECT 1') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='user' AND COLUMN_NAME='register_time');
PREPARE __mss_stmt FROM @__mss_sql;
EXECUTE __mss_stmt;
DEALLOCATE PREPARE __mss_stmt;
SET @__mss_sql = (SELECT IF(COUNT(*)=0, 'ALTER TABLE `user` ADD `password` varchar(1000) NOT NULL DEFAULT '''' AFTER `register_time`', 'SELECT 1') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='user' AND COLUMN_NAME='password');
PREPARE __mss_stmt FROM @__mss_sql;
EXECUTE __mss_stmt;
DEALLOCATE PREPARE __mss_stmt;
SET @__mss_sql = (SELECT IF(COUNT(*)=0, 'ALTER TABLE `user` ADD `status` tinyint unsigned NOT NULL DEFAULT ''0'' AFTER `password`', 'SELECT 1') FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='user' AND COLUMN_NAME='status');
PREPARE __mss_stmt FROM @__mss_sql;
EXECUTE __mss_stmt;
DEALLOCATE PREPARE __mss_stmt;
COMMIT;