USE mythconverg;

CREATE TABLE `SDlineupCache` (
  `row` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `headend` varchar(14) NOT NULL DEFAULT '',
  `md5` char(22) NOT NULL,
  `modified` char(20) DEFAULT '1970-01-01T00:00:00Z',
  `json` mediumtext,
  PRIMARY KEY (`row`),
  UNIQUE KEY `headend` (`headend`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

 CREATE TABLE `SDprogramCache` (
  `row` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `programID` char(14) NOT NULL DEFAULT '',
  `md5` char(22) NOT NULL,
  `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `json` varchar(4096) DEFAULT NULL,
  PRIMARY KEY (`row`),
  UNIQUE KEY `pid-MD5` (`programID`,`md5`),
  KEY `programID` (`programID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE videosource add `modified` char(20) DEFAULT '1970-01-01T00:00:00Z';
