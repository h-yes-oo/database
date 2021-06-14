#store table schemas for building, performance, audience, reservation
TABLES = {}
TABLES['building'] = (
    "CREATE TABLE `building` ("
    "  `id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `name` varchar(200) NOT NULL,"
    "  `location` varchar(200) NOT NULL,"
    "  `capacity` int(11) NOT NULL,"
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB")

TABLES['performance'] = (
    "CREATE TABLE `performance` ("
    "  `id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `name` varchar(200) NOT NULL,"
    "  `type` varchar(200) NOT NULL,"
    "  `price` int(11) NOT NULL,"
    "  `building` int(11),"
    "  PRIMARY KEY (`id`),"
    "  CONSTRAINT `performance_ibfk_1` FOREIGN KEY (`building`) "
    "     REFERENCES `building` (`id`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")

TABLES['audience'] = (
    "CREATE TABLE `audience` ("
    "  `id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `name` varchar(200) NOT NULL,"
    "  `gender` char(1) NOT NULL,"
    "  `age` int(11) NOT NULL,"
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB")

TABLES['reservation'] = (
    "  CREATE TABLE `reservation` ("
    "  `id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `audience` int(11) NOT NULL,"
    "  `performance` int(11) NOT NULL,"
    "  `seat` int(11) NOT NULL,"
    "  PRIMARY KEY (`id`),"
    "  CONSTRAINT `reservation_ibfk_1` FOREIGN KEY (`audience`) "
    "     REFERENCES `audience` (`id`) ON DELETE CASCADE,"
    "  CONSTRAINT `reservation_ibfk_2` FOREIGN KEY (`performance`) "
    "     REFERENCES `performance` (`id`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")