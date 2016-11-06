CREATE TABLE IF NOT EXISTS `group_stats` (
  `datetime` TEXT NOT NULL,
  `group` TEXT NOT NULL,
  `usage` REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS `user_stats` (
  `datetime` TEXT NOT NULL,
  `user` TEXT NOT NULL,
  `group` TEXT NOT NULL,
  `usage` REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS `group_datetime` ON `group_stats` (`datetime`);
CREATE INDEX IF NOT EXISTS `group` ON `group_stats` (`group`);
CREATE INDEX IF NOT EXISTS `user_datetime` ON `user_stats` (`datetime`);
