
-- ======================================== ADDITIONAL CHECKS =========================================================
CREATE OR REPLACE FUNCTION fnc_check_task_is_passed(check_id BIGINT)
    RETURNS BOOLEAN AS
$fnc_check_task_is_passed$
DECLARE
    p2p_check_id_status_success    BIGINT;
    verter_check_id_status_start   BIGINT;
    verter_check_id_status_success BIGINT;

BEGIN
    SELECT p2p."check"
    INTO p2p_check_id_status_success
    FROM p2p
    WHERE p2p."check" = check_id
      AND p2p.state = 'Success'
    ORDER BY time DESC
    LIMIT 1;

    IF (p2p_check_id_status_success IS NOT NULL) THEN
        SELECT verter."check"
        INTO verter_check_id_status_start
        FROM verter
        WHERE verter."check" = p2p_check_id_status_success
          AND verter.state = 'Start'
        ORDER BY time DESC
        LIMIT 1;

        IF (verter_check_id_status_start IS NOT NULL) THEN
            SELECT verter."check"
            INTO verter_check_id_status_success
            FROM verter
            WHERE verter."check" = verter_check_id_status_start
              AND verter.state = 'Success'
            ORDER BY time DESC
            LIMIT 1;

            IF (verter_check_id_status_success IS NOT NULL) THEN
                RETURN TRUE;
            ELSE
                RETURN FALSE;
            END IF;
        ELSE
            RETURN TRUE;
        END IF;
    ELSE
        RETURN FALSE;
    END IF;
END;

$fnc_check_task_is_passed$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_check_task_verter_can_restart(check_id BIGINT)
    RETURNS BOOLEAN AS
$fnc_check_task_verter_can_restart$
DECLARE
    p2p_check_id_status_success    BIGINT;
    verter_check_id_status_start   BIGINT;

BEGIN
    SELECT p2p."check"
    INTO p2p_check_id_status_success
    FROM p2p
    WHERE p2p."check" = check_id
      AND p2p.state = 'Success'
    ORDER BY time DESC
    LIMIT 1;

    IF (p2p_check_id_status_success IS NOT NULL) THEN
        SELECT verter."check"
        INTO verter_check_id_status_start
        FROM verter
        WHERE verter."check" = p2p_check_id_status_success
          AND verter.state = 'Start'
        ORDER BY time DESC
        LIMIT 1;

        IF (verter_check_id_status_start IS NOT NULL) THEN
            RETURN FALSE;
        ELSE
            RETURN TRUE;
        END IF;
    ELSE
        RETURN FALSE;
    END IF;
END;

$fnc_check_task_verter_can_restart$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_check_previous_precheck(task_ch VARCHAR, peer_ch VARCHAR)
    RETURNS BOOLEAN AS
$fnc_check_previous_precheck$
DECLARE
    parenttask_ch   VARCHAR;
    parent_check_id BIGINT;

BEGIN
    SELECT parenttask
    INTO parenttask_ch
    FROM tasks
    WHERE title = task_ch;

    IF (parenttask_ch IS NOT NULL) THEN
        SELECT checks.id
        INTO parent_check_id
        FROM checks
        WHERE peer = peer_ch
          AND task = parenttask_ch
        ORDER BY checks.id DESC
        LIMIT 1;

        IF (parent_check_id IS NOT NULL) THEN
            RETURN fnc_check_task_is_passed(parent_check_id);
        ELSE
            RETURN FALSE;
        END IF;
    ELSE
        RETURN TRUE;
    END IF;
END;
$fnc_check_previous_precheck$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fnc_check_current_for_p2p_status(existing_check_id BIGINT)
    RETURNS BOOLEAN AS
$fnc_check_current_for_p2p_status$
DECLARE
    p2p_check_id_start  BIGINT;
    p2p_check_id_finish BIGINT;

BEGIN
    SELECT p2p."check"
    INTO p2p_check_id_start
    FROM p2p
    WHERE "check" = existing_check_id
      AND p2p.state = 'Start'
    ORDER BY time DESC
    LIMIT 1;

    IF (p2p_check_id_start IS NOT NULL) THEN
        SELECT p2p."check"
        INTO p2p_check_id_finish
        FROM p2p
        WHERE "check" = p2p_check_id_start
          AND (p2p.state = 'Success' OR p2p.state = 'Failure')
        LIMIT 1;

        IF (p2p_check_id_finish IS NOT NULL) THEN
            RETURN TRUE;
        ELSE
            RETURN FALSE;
        END IF;

    ELSE
        RETURN FALSE;
    END IF;
END;
$fnc_check_current_for_p2p_status$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fnc_check_current_for_verter_status(existing_check_id BIGINT)
    RETURNS BOOLEAN AS
$fnc_check_current_for_verter_status$
DECLARE
    current_check_id_start  BIGINT;
    current_check_id_finish BIGINT;

BEGIN
    SELECT "check"
    INTO current_check_id_start
    FROM verter
    WHERE "check" = existing_check_id
      AND verter.state = 'Start'
    ORDER BY time DESC
    LIMIT 1;

    IF (current_check_id_start IS NOT NULL)
    THEN
        SELECT "check"
        INTO current_check_id_finish
        FROM verter
        WHERE verter."check" = current_check_id_start
          AND (verter.state = 'Success' OR verter.state = 'Failure')
        ORDER BY time DESC
        LIMIT 1;

        IF (current_check_id_finish IS NOT NULL) THEN
            RETURN TRUE;
        ELSE
            RETURN FALSE;
        END IF;

    ELSE
        RETURN TRUE;
    END IF;

END;
$fnc_check_current_for_verter_status$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION fnc_check_current_for_p2p_and_verter_status(checked_peer VARCHAR,
                                                                       current_task VARCHAR)
    RETURNS BOOLEAN AS
$fnc_check_current_for_p2p_and_verter_status$
DECLARE
    existing_check_id BIGINT;

BEGIN
    SELECT id -- search if check already exists
    INTO existing_check_id
    FROM checks
    WHERE checks.task = current_task
      AND checks.peer = checked_peer
    ORDER BY checks.id DESC
    LIMIT 1;

    IF (existing_check_id IS NOT NULL) THEN -- if check exists, check for p2p and verter status

        RETURN (fnc_check_current_for_p2p_status(existing_check_id) IS TRUE
            AND fnc_check_current_for_verter_status(existing_check_id) IS TRUE);
    ELSE
        RETURN TRUE; -- if check doesn't exist we can add p2p check with start status
    END IF;
END;
$fnc_check_current_for_p2p_and_verter_status$ LANGUAGE plpgsql;

-- ====================================================================================================================
-- ======================================== 2.1 P2P INSERT PROCEDURE ==================================================
-- ====================================================================================================================

CREATE
    OR REPLACE PROCEDURE prc_insert_p2p_check(checked_peer VARCHAR, checking_peer VARCHAR,
                                              current_task VARCHAR, p2p_status checkstatus, check_time TIME)
AS
$InsertP2PCheck$
DECLARE
    current_check_id BIGINT;

BEGIN
    PERFORM setval('checks_id_seq', COALESCE((SELECT MAX(id) + 1 FROM checks), 1), false);

    IF (p2p_status = 'Start'
        AND fnc_check_current_for_p2p_and_verter_status(checked_peer, current_task) IS TRUE
        AND (fnc_check_previous_precheck(current_task, checked_peer) IS TRUE))
    THEN
        INSERT INTO checks (peer, task, date)
        VALUES (checked_peer, current_task, current_date);

        SELECT id
        INTO current_check_id
        FROM checks
        WHERE peer = checked_peer
          AND task = current_task
        ORDER BY id DESC
        LIMIT 1;

        INSERT INTO p2p("check", checkingpeer, state, time)
        VALUES (current_check_id, checking_peer, p2p_status,
                check_time);
    ELSE
        IF (p2p_status IN ('Success', 'Failure')
            AND fnc_check_current_for_p2p_and_verter_status(checked_peer, current_task) IS FALSE)
        THEN
            SELECT "check"
            INTO current_check_id
            FROM p2p
                     JOIN checks ON p2p."check" = checks.id AND checks.task = current_task
            WHERE p2p.checkingpeer = checking_peer
              AND p2p.state = 'Start'
              AND checks.peer = checked_peer
            ORDER BY checks.id DESC
            LIMIT 1;

            INSERT INTO p2p("check", checkingpeer, state, time)
            VALUES (current_check_id, checking_peer, p2p_status,
                    check_time::time);
        END IF;
    END IF;
        PERFORM setval('p2p_id_seq', COALESCE((SELECT MAX(id) + 1 FROM p2p), 1), false);
END;
$InsertP2PCheck$ LANGUAGE plpgsql;


-- ====================================================================================================================
-- ======================================== 2.2 VERTER INSERT PROCEDURE ===============================================
-- ====================================================================================================================

CREATE
    OR REPLACE PROCEDURE prc_insert_verter_check(checked_peer VARCHAR,
                                                 current_task VARCHAR, verter_status checkstatus,
                                                 check_time TIME)
AS
$InsertVerterCheck$
DECLARE
    current_check_id   BIGINT;
    current_check_time TIME;

BEGIN
    SELECT checks.id
    INTO current_check_id
    FROM checks
             JOIN p2p ON checks.id = p2p."check"
    WHERE peer = checked_peer
      AND task = current_task
      AND p2p.state = 'Success'
    ORDER BY checks.id DESC
    LIMIT 1;


    IF (current_check_id IS NOT NULL) THEN
        SELECT time
        INTO current_check_time
        FROM p2p
        WHERE p2p."check" = current_check_id
          AND p2p.state = 'Success'
        ORDER BY time DESC
        LIMIT 1;

        IF (verter_status = 'Start'
            AND fnc_check_current_for_verter_status(current_check_id) IS TRUE
            AND fnc_check_task_verter_can_restart(current_check_id) IS TRUE) THEN

            INSERT INTO verter("check", state, time)
            VALUES (current_check_id, verter_status, check_time::time);
        ELSE
            IF (verter_status IN ('Success', 'Failure')
                AND fnc_check_current_for_verter_status(current_check_id) IS FALSE)
            THEN
                INSERT INTO verter("check", state, time)
                VALUES (current_check_id, verter_status, (current_check_time + interval '1 minute')::time);
            END IF;
        END IF;
    END IF;
            PERFORM setval('verter_id_seq', COALESCE((SELECT MAX(id) + 1 FROM verter), 1), false);
END;
$InsertVerterCheck$ LANGUAGE plpgsql;

-- ====================================================================================================================
-- ======================================= 2.3 TRANSFER POINTS TRIGGER  ===============================================
-- ====================================================================================================================

CREATE OR REPLACE FUNCTION fnc_trg_p2p_insert_transferred_points() RETURNS TRIGGER AS
$fnc_trg_p2p_insert_transferred_points$
DECLARE
    checked_peer_checks VARCHAR;
    checked_peer_transf VARCHAR;

BEGIN
    IF (NEW.state = 'Start') THEN
        SELECT peer
        INTO checked_peer_checks
        FROM checks
                 JOIN p2p ON checks.id = p2p."check"
        WHERE checkingpeer = NEW.checkingpeer;

        SELECT checkedpeer
        INTO checked_peer_transf
        FROM transferredpoints
        WHERE checkedpeer = checked_peer_checks
          AND checkingpeer = NEW.checkingpeer;

        IF (checked_peer_transf IS NULL) THEN
            INSERT INTO transferredpoints(checkingpeer, checkedpeer, pointsamount)
            VALUES (NEW.checkingpeer, checked_peer_checks, 1);
        ELSE
            UPDATE transferredpoints
            SET pointsamount = pointsamount + 1
            WHERE checkingpeer = NEW.checkingpeer
              AND checkedpeer = checked_peer_transf;
        END IF;
    END IF;
    RETURN NULL;
END;
$fnc_trg_p2p_insert_transferred_points$ LANGUAGE plpgsql;


CREATE TRIGGER trg_p2p_insert_transferred_points
    AFTER INSERT
    ON p2p
    FOR EACH ROW
EXECUTE PROCEDURE fnc_trg_p2p_insert_transferred_points();


-- ====================================================================================================================
-- =========================================== 2.4 XP CHECK TRIGGER  ==================================================
-- ====================================================================================================================

CREATE OR REPLACE FUNCTION fnc_trg_XP_insert_check() RETURNS TRIGGER AS
$fnc_trg_XP_insert_check$
DECLARE
    max_xp    BIGINT;
    if_unique BIGINT;

BEGIN
    SELECT maxxp
    INTO max_xp
    FROM tasks
             JOIN checks ON tasks.title = checks.task
    WHERE checks.id = NEW."check";

    SELECT xp."check"
    INTO if_unique
    FROM xp
    WHERE xp."check" = NEW."check";

    IF ((NEW.xpamount <= max_xp) AND (fnc_check_task_is_passed(NEW."check") IS TRUE) AND if_unique IS NULL) THEN
        RETURN NEW;
    ELSE
        PERFORM setval('xp_id_seq', COALESCE((SELECT MAX(id) + 1 FROM xp), 1), false);
        RETURN NULL;
    END IF;

END;
$fnc_trg_XP_insert_check$ LANGUAGE plpgsql;


CREATE TRIGGER trg_XP_insert_check
    BEFORE INSERT
    ON xp
    FOR EACH ROW
EXECUTE PROCEDURE fnc_trg_XP_insert_check();


