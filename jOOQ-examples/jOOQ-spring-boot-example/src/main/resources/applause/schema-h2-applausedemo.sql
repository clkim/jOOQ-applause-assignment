DROP TABLE IF EXISTS testers;
DROP TABLE IF EXISTS devices;
DROP TABLE IF EXISTS tester_device;
DROP TABLE IF EXISTS bugs;


CREATE TABLE testers (
  tester_id INT NOT NULL,
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  country VARCHAR(50) NOT NULL,
  last_login TIMESTAMP,

  CONSTRAINT pk_t_testers PRIMARY KEY (tester_id)
);

CREATE TABLE devices (
  device_id INT NOT NULL,
  description VARCHAR(50) NOT NULL,

  CONSTRAINT pk_t_devices PRIMARY KEY(device_id)
);

CREATE TABLE tester_device (
  tester_id INT NOT NULL,
  device_id INT NOT NULL,

  CONSTRAINT pk_t2d PRIMARY KEY(tester_id, device_id),
  CONSTRAINT fk_pk_t2d_tester_id FOREIGN KEY (tester_id)
                                 REFERENCES testers (tester_id)
                                 ON DELETE CASCADE,
  CONSTRAINT fk_t2d_device_id FOREIGN KEY (device_id)
                              REFERENCES devices (device_id)
                              ON DELETE CASCADE
);

CREATE TABLE bugs (
  bug_id INT NOT NULL,
  device_id INT NOT NULL,
  tester_id INT NOT NULL,

  CONSTRAINT pk_t_bugs PRIMARY KEY (bug_id),
  CONSTRAINT fk_t_bugs_device_id FOREIGN KEY (device_id) REFERENCES devices(device_id),
  CONSTRAINT fk_t_bugs_tester_id FOREIGN KEY (tester_id) REFERENCES testers(tester_id)
);