USE mythconverg;

DROP TABLE IF EXISTS SDprogramCache,SDCredits,SDheadendCache,SDpeople,SDprogramgenres,SDprogramrating,SDschedule;

 CREATE TABLE `SDprogramCache` (
  `row` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `programID` varchar(64) NOT NULL,
  `md5` char(22) NOT NULL,
  `modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `json` varchar(4096) NOT NULL,
  PRIMARY KEY (`row`),
  UNIQUE KEY `pid` (`programID`),
  KEY `programID` (`programID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

 CREATE TABLE `SDcredits` (
  `personID` mediumint(8) unsigned NOT NULL DEFAULT '0',
  `programID` varchar(64) NOT NULL,
  `role` varchar(100) DEFAULT NULL,
  KEY `personID` (`personID`),
  KEY `programID` (`programID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

 CREATE TABLE `SDheadendCache` (
  `row` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `headend` varchar(14) NOT NULL DEFAULT '',
  `md5` char(22) NOT NULL,
  `modified` char(20) DEFAULT '1970-01-01T00:00:00Z',
  `json` mediumtext,
  PRIMARY KEY (`row`),
  UNIQUE KEY `headend` (`headend`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `SDpeople` (
  `personID` mediumint(8) unsigned NOT NULL,
  `name` varchar(128) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL DEFAULT '',
  PRIMARY KEY (`personID`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `SDprogramgenres` (
  `programID` varchar(64) NOT NULL,
  `relevance` char(1) NOT NULL DEFAULT '0',
  `genre` varchar(30) NOT NULL,
  PRIMARY KEY (`programID`),
  UNIQUE KEY `pid_relevance` (`programID`,`relevance`),
  KEY `genre` (`genre`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `SDprogramrating` (
  `programID` varchar(64) NOT NULL,
  `system` varchar(30) NOT NULL,
  `rating` varchar(16) DEFAULT NULL,
  PRIMARY KEY (`programID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

CREATE TABLE `SDschedule` (
  `stationID` varchar(12) NOT NULL,
  `programID` varchar(64) NOT NULL,
  `md5` char(22) NOT NULL,
  `air_datetime` char(20) NOT NULL,
  `duration` mediumint(8) unsigned DEFAULT '0' COMMENT 'Duration (in seconds) of the program.',
  `airdate` year(4) NOT NULL DEFAULT '0000',
  `previouslyshown` tinyint(1) DEFAULT '0',
  `closecaptioned` tinyint(1) NOT NULL DEFAULT '0',
  `partnumber` tinyint(3) unsigned DEFAULT '0',
  `parttotal` tinyint(3) unsigned DEFAULT '0',
  `listingsource` int(11) NOT NULL DEFAULT '0',
  `first` tinyint(1) NOT NULL DEFAULT '0',
  `last` tinyint(1) NOT NULL DEFAULT '0',
  `dvs` tinyint(1) DEFAULT '0' COMMENT 'Descriptive Video Service',
  `new` tinyint(1) DEFAULT '0' COMMENT 'New',
  `educational` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'Identifies broadcaster-designated Educational/Instructional programming.',
  `hdtv` tinyint(1) NOT NULL DEFAULT '0',
  `3d` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'Indicates program is in 3-D.',
  `letterbox` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'Indicates program is a letterbox version.',
  `stereo` tinyint(1) DEFAULT '0',
  `dolby` varchar(5) DEFAULT NULL,
  `dubbed` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'Indicates the program is dubbed.',
  `dubLanguage` varchar(40) DEFAULT NULL,
  `subtitled` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'Indicates if the audio is in a foreign language, the English translation appears on-screen.',
  `subtitleLanguage` varchar(40) DEFAULT NULL,
  `sap` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'Indicates the availability of Secondary Audio Programming.',
  `sapLanguage` varchar(40) DEFAULT NULL,
  `programLanguage` varchar(40) DEFAULT NULL,
  `tvRatingSystem` varchar(128) DEFAULT NULL,
  `tvRating` varchar(7) DEFAULT NULL,
  `dialogRating` tinyint(1) DEFAULT '0' COMMENT 'FCC content descriptor "D" rating',
  `languageRating` tinyint(1) DEFAULT '0' COMMENT 'FCC content descriptor "L" rating',
  `sexualContentRating` tinyint(1) DEFAULT '0' COMMENT 'FCC content descriptor "S" rating',
  `violenceRating` tinyint(1) DEFAULT '0' COMMENT 'FCC content descriptor "V" rating',
  `fvRating` tinyint(1) DEFAULT '0' COMMENT 'Indicates fantasy violence.',
  UNIQUE KEY `stationid_airdatetime` (`stationID`,`air_datetime`),
  KEY `previouslyshown` (`previouslyshown`),
  KEY `programid` (`programID`),
  KEY `md5` (`md5`),
  KEY `sid` (`stationID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

ALTER TABLE credits CHANGE role role SET('actor','director','producer','executive_producer','writer',
'guest_star','host','adapter','presenter','commentator','guest','musical_guest','judge','correspondent','contestant');
