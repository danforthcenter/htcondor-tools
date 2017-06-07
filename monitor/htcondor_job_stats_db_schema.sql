-- jobs table
-- Stores job and machine Classad information about each HTCondor job
CREATE TABLE IF NOT EXISTS `jobs` (
    `id` INT(9) UNSIGNED AUTO_INCREMENT NOT NULL,
    `cluster` INT(9) NOT NULL,
    `process` INT(9) NOT NULL,
    `username` VARCHAR(30) NOT NULL,
    `groupname` VARCHAR(30) NOT NULL,
    `start_date` TIMESTAMP NOT NULL,
    `cpu` INT(4) NOT NULL,
    `cpu_unit` CHAR(5) DEFAULT "count",
    `memory` INT(6) NOT NULL,
    `memory_unit` CHAR(3) DEFAULT "MiB",
    `disk` INT(6) NOT NULL,
    `disk_unit` CHAR(3) DEFAULT "KiB",
    `host` VARCHAR(30) NOT NULL,
    `universe` VARCHAR(30) NOT NULL,
    `exe` VARCHAR(100) NOT NULL,
    `transfer` TINYINT(1) NOT NULL,
    PRIMARY KEY (id)
);
-- Create indexes
CREATE INDEX index_jobs ON jobs(cluster, process);
CREATE INDEX index_users ON jobs(username);
CREATE INDEX index_groups ON jobs(groupname);

-- units table
-- Stores information about the computing resource units of jobs and machines
CREATE TABLE IF NOT EXISTS `units` (
    `unit_id` VARCHAR(5) NOT NULL,
    `unit_name` VARCHAR(30) NOT NULL,
    `url` VARCHAR(512) NOT NULL
);
-- Insert default unit data into units
INSERT INTO `units` (`unit_id`, `unit_name`, `url`) VALUES ("count", "count", "http://semanticscience.org/resource/SIO_000794.rdf");
INSERT INTO `units` (`unit_id`, `unit_name`, `url`) VALUES ("MiB", "mebibyte", "http://purl.obolibrary.org/obo/UO_0000246");
INSERT INTO `units` (`unit_id`, `unit_name`, `url`) VALUES ("KiB", "kibibyte", "http://purl.obolibrary.org/obo/UO_0000245");

-- job_stats table
-- Stores a jobs current usage statistics
CREATE TABLE IF NOT EXISTS `job_stats` (
    `id` INT(9) UNSIGNED NOT NULL,
    `datetime` TIMESTAMP NOT NULL,
    `cpu_load` FLOAT(5, 2) NOT NULL,
    `memory_usage` INT(6) NOT NULL,
    `disk_usage` INT(6) NOT NULL,
    FOREIGN KEY (id) REFERENCES jobs(id)
);
