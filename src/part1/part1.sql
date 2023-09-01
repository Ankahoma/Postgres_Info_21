-- CREATE DATABASE "school_21" ENCODING 'UTF8';

-- Clean schema
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;
--

CREATE TYPE CheckStatus AS ENUM ('Start', 'Success', 'Failure');

CREATE TABLE IF NOT EXISTS peers
(
    nickname VARCHAR PRIMARY KEY,
    birthday DATE
);

CREATE TABLE IF NOT EXISTS timetracking
(
    id    SERIAL PRIMARY KEY,
    peer  VARCHAR,
    date  DATE   DEFAULT current_date NOT NULL,
    time  TIME   DEFAULT current_time NOT NULL,
    state BIGINT DEFAULT 1
    CONSTRAINT ch_state CHECK (state BETWEEN 1 AND 2),
    CONSTRAINT fk_timetracking_peer FOREIGN KEY (peer) REFERENCES peers(nickname)
);

CREATE TABLE IF NOT EXISTS recommendations
(
    id              SERIAL PRIMARY KEY,
    peer            VARCHAR,
    recommendedPeer VARCHAR,
    CONSTRAINT fk_recommendations_peer FOREIGN KEY (peer) REFERENCES peers(nickname),
    CONSTRAINT fk_recommendations_recommended_peer FOREIGN KEY (peer) REFERENCES peers(nickname)
);

CREATE TABLE IF NOT EXISTS friends
(
    id    SERIAL PRIMARY KEY,
    peer1 VARCHAR,
    peer2 VARCHAR,
    CONSTRAINT fk_friends_peer1 FOREIGN KEY (peer1) REFERENCES peers (Nickname),
    CONSTRAINT fk_friends_peer2 FOREIGN KEY (peer2) REFERENCES peers (Nickname)
);

CREATE TABLE IF NOT EXISTS transferredpoints
(
    id           SERIAL PRIMARY KEY,
    checkingpeer VARCHAR,
    checkedpeer  VARCHAR,
    pointsAmount BIGINT,
    CONSTRAINT fk_transferredpoints_checkingpeer FOREIGN KEY (checkingpeer) REFERENCES peers (nickname),
    CONSTRAINT fk_transferredpoints_checkedpeer FOREIGN KEY (checkedpeer) REFERENCES peers (nickname)
);

CREATE TABLE IF NOT EXISTS tasks
(
    title      VARCHAR PRIMARY KEY,
    parenttask VARCHAR DEFAULT NULL,
    maxxp      BIGINT,
    CONSTRAINT ch_max_xp CHECK (maxxp >= 0),
    CONSTRAINT fk_tasks_title_parent FOREIGN KEY (parenttask) REFERENCES tasks(title)
);


CREATE TABLE IF NOT EXISTS checks
(
    id   SERIAL PRIMARY KEY,
    peer VARCHAR,
    task VARCHAR,
    date DATE,
    CONSTRAINT fk_checks_peer FOREIGN KEY (peer) REFERENCES peers(nickname),
    CONSTRAINT fk_checks_task FOREIGN KEY (task) REFERENCES tasks(title)
);

CREATE TABLE IF NOT EXISTS p2p
(
    id           SERIAL PRIMARY KEY,
    "check"      BIGINT,
    checkingpeer VARCHAR,
    state        CheckStatus,
    time         TIME,
    CONSTRAINT fk_p2p_check FOREIGN KEY ("check") REFERENCES checks(id),
    CONSTRAINT fk_P2P_checkingpeer FOREIGN KEY (checkingpeer) REFERENCES peers(nickname)
);

CREATE TABLE IF NOT EXISTS verter
(
    id      SERIAL PRIMARY KEY,
    "check" SERIAL,
    state   CheckStatus,
    time    TIME,
    CONSTRAINT fk_verter_check FOREIGN KEY ("check") REFERENCES checks(id)
);

CREATE TABLE IF NOT EXISTS xp
(
    id       SERIAL PRIMARY KEY,
    "check"  BIGINT,
    xpamount BIGINT,
    CONSTRAINT fk_xp_check FOREIGN KEY ("check") REFERENCES checks(id)
);


-- ====================================================================================================================
-- ============================================= PROCEDURES ===========================================================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_import_from_CSV(target_table VARCHAR, filepath VARCHAR, delimiter VARCHAR)
AS
$prc_import_from_CSV$
DECLARE
    seq VARCHAR;
BEGIN
    delimiter := ',';
    EXECUTE 'COPY ' || target_table || ' FROM ''' || filepath || '''DELIMITER ''' || delimiter || ''' CSV HEADER';

    IF (target_table <> 'peers' AND  target_table <> 'tasks') THEN
     SELECT (target_table||'_id_seq') INTO seq;
    EXECUTE 'SELECT setval(''' || seq || ''', COALESCE((SELECT MAX(id) + 1 FROM ' || target_table || ' ), 1), false)';
    END IF;
END;
$prc_import_from_CSV$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE prc_export_to_CSV(source_table VARCHAR, filepath VARCHAR, delimiter VARCHAR)
AS
$prc_export_to_CSV$
BEGIN
    delimiter := ',';
    EXECUTE 'COPY ' || source_table || ' TO ''' || filepath || '''DELIMITER ''' || delimiter || ''' CSV HEADER';
END;
$prc_export_to_CSV$ LANGUAGE plpgsql;

-- ====================================================================================================================
-- ============================================= FILL TABLES ==========================================================
-- ====================================================================================================================
-- Clean tables

-- DELETE FROM xp;
-- DELETE FROM transferredpoints;
-- DELETE FROM p2p;
-- DELETE FROM verter;
-- DELETE FROM checks;
-- DELETE FROM tasks;
-- DELETE FROM friends;
-- DELETE FROM recommendations;
-- DELETE FROM timetracking;
-- DELETE FROM peers;

-- MacOS

CALL prc_import_from_CSV('peers', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/new_peers.csv', ',');
CALL prc_import_from_CSV('tasks', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/new_tasks.csv', ',' );
CALL prc_import_from_CSV('friends', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/new_friends.csv', ',' );
CALL prc_import_from_CSV('recommendations', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/new_recommendations.csv', ',' );
CALL prc_import_from_CSV('timetracking', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/new_timetracking.csv', ',' );
CALL prc_import_from_CSV('checks', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/new_checks.csv', ',' );
CALL prc_import_from_CSV('verter', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/new_verter.csv', ',');
CALL prc_import_from_CSV('p2p', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/new_p2p.csv', ',');
CALL prc_import_from_CSV('transferredpoints', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/new_transferred_points.csv', ',');
CALL prc_import_from_CSV('xp', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/new_xp.csv', ',');

-- Insert timetracking for part 3.24
SELECT setval('timetracking_id_seq', COALESCE((SELECT MAX(id) + 1 FROM timetracking), 1), false);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('tprobius', current_date - interval '1 day', '08:00:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('bdupp', current_date - interval '1 day', '10:00:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('tprobius', current_date - interval '1 day', '10:30:10'::time, 2);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('bromanyt', current_date - interval '1 day', '11:00:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('tprobius', current_date - interval '1 day', '11:45:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('bdupp', current_date - interval '1 day', '11:50:10'::time, 2);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('bdupp', current_date - interval '1 day', '12:20:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('bromanyt', current_date - interval '1 day', '17:00:10'::time, 2);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('tprobius', current_date - interval '1 day', '18:30:10'::time, 2);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('bdupp', current_date - interval '1 day', '19:20:10'::time, 2);

-- Insert timetracking for part 3.20
SELECT setval('timetracking_id_seq', COALESCE((SELECT MAX(id) + 1 FROM timetracking), 1), false);


INSERT INTO timetracking (peer, date, time, state)
VALUES ('tprobius', current_date, '08:00:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('bdupp', current_date, '10:00:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('tprobius', current_date, '10:30:10'::time, 2);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('bromanyt', current_date, '11:00:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('tprobius', current_date, '11:45:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('bdupp', current_date, '11:50:10'::time, 2);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('bdupp', current_date, '12:20:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('bromanyt', current_date, '17:00:10'::time, 2);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('tprobius', current_date, '18:30:10'::time, 2);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('bdupp', current_date, '19:20:10'::time, 2);

-- Insert timetracking for part 3.24
SELECT setval('timetracking_id_seq', COALESCE((SELECT MAX(id) + 1 FROM timetracking), 1), false);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('vluann', '2022-01-02'::date, '08:30:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('tprobius', '2022-01-02'::date, '10:30:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('vluann', '2022-01-02'::date, '18:30:10'::time, 2);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('tprobius', '2022-01-02'::date, '19:30:10'::time, 2);

-- ==================================================================

INSERT INTO timetracking (peer, date, time, state)
VALUES ('vluann', '2022-01-10'::date, '12:30:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('tprobius', '2022-01-10'::date, '11:30:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('vluann', '2022-01-02'::date, '18:30:10'::time, 2);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('tprobius', '2022-01-02'::date, '19:30:10'::time, 2);

-- =====================================================================

INSERT INTO timetracking (peer, date, time, state)
VALUES ('nstefan', '2022-12-02'::date, '08:30:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('cshara', '2022-12-02'::date, '10:30:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('nstefan', '2022-12-02'::date, '11:00:10'::time, 2);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('barlog', '2022-12-02'::date, '11:30:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('nstefan', '2022-12-02'::date, '12:15:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('cshara', '2022-12-02'::date, '12:20:10'::time, 2);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('cshara', '2022-12-02'::date, '12:50:10'::time, 1);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('barlog', '2022-12-02'::date, '17:30:10'::time, 2);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('nstefan', '2022-12-02'::date, '19:00:10'::time, 2);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('cshara', '2022-12-02'::date, '19:50:10'::time, 2);

-- ====================================================================
INSERT INTO timetracking (peer, date, time, state)
VALUES ('barlog', '2022-12-03'::date, '10:30:10'::time, 1);
INSERT INTO timetracking (peer, date, time, state)
VALUES ('barlog', '2022-12-03'::date, '17:30:10'::time, 2);


INSERT INTO timetracking (peer, date, time, state)
VALUES ('nstefan', '2022-12-03'::date, '12:30:10'::time, 1);
INSERT INTO timetracking (peer, date, time, state)
VALUES ('nstefan', '2022-12-03'::date, '13:00:10'::time, 2);
INSERT INTO timetracking (peer, date, time, state)
VALUES ('nstefan', '2022-12-03'::date, '13:15:10'::time, 1);
INSERT INTO timetracking (peer, date, time, state)
VALUES ('nstefan', '2022-12-03'::date, '19:00:10'::time, 2);

INSERT INTO timetracking (peer, date, time, state)
VALUES ('cshara', '2022-12-03'::date, '12:45:10'::time, 1);
INSERT INTO timetracking (peer, date, time, state)
VALUES ('cshara', '2022-12-03'::date, '12:55:10'::time, 2);
INSERT INTO timetracking (peer, date, time, state)
VALUES ('cshara', '2022-12-03'::date, '12:58:10'::time, 1);
INSERT INTO timetracking (peer, date, time, state)
VALUES ('cshara', '2022-12-03'::date, '19:50:10'::time, 2);

-- Ubuntu

-- CALL ImportFromCSV('peers', '/var/lib/postgresql/14/main/part1/new_peers.csv', ',');
-- CALL ImportFromCSV('tasks', '/var/lib/postgresql/14/main/part1/new_tasks.csv', ',');
-- CALL ImportFromCSV('friends', '/var/lib/postgresql/14/main/part1/new_friends.csv', ',');
-- CALL ImportFromCSV('recommendations', '/var/lib/postgresql/14/main/part1/new_recommendations.csv', ',');
-- CALL ImportFromCSV('timetracking', '/var/lib/postgresql/14/main/part1/new_timetracking.csv', ',');
-- CALL ImportFromCSV('checks', '/var/lib/postgresql/14/main/part1/new_checks.csv', ',');
-- CALL ImportFromCSV('verter', '/var/lib/postgresql/14/main/part1/new_verter.csv', ',');
-- CALL ImportFromCSV('p2p', '/var/lib/postgresql/14/main/part1/new_p2p.csv', ',');
-- CALL ImportFromCSV('transferredpoints', '/var/lib/postgresql/14/main/part1/new_transferred_points.csv', ',');
-- CALL ImportFromCSV('xp', '/var/lib/postgresql/14/main/part1/new_xp.csv', ',');
