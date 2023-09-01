-- ====================================================================================================================
-- ============ 3.1 Write a function that returns the TransferredPoints table in a more human-readable form  ==========
-- ====================================================================================================================
CREATE OR REPLACE FUNCTION fnc_transferredpoints_human_readable()
    RETURNS TABLE
            (
                peer1        VARCHAR,
                peer2        VARCHAR,
                pointsamount BIGINT
            )
AS
$fnc_transferredpoints_human_readable$
BEGIN
    RETURN QUERY
        WITH select1 AS (SELECT table1.checkingpeer                       AS peer1,
                                table1.checkedpeer                        AS peer2,
                                table1.pointsamount - table2.pointsamount AS pointsamount
                         FROM transferredpoints AS table1
                                  LEFT JOIN transferredpoints AS table2
                                            ON table1.checkingpeer = table2.checkedpeer AND
                                               table1.checkedpeer = table2.checkingpeer),

             select2 AS
                 (SELECT select1.peer1                  AS peer1,
                         select1.peer2                  AS peer2,
                         transferredpoints.pointsamount AS pointsamount
                  FROM transferredpoints
                           JOIN select1
                                ON select1.peer1 = transferredpoints.checkingpeer
                                    AND select1.peer2 = transferredpoints.checkedpeer
                                    AND select1.pointsamount IS NULL)

        SELECT select1.peer1        AS peer1,
               select1.peer2        AS peer2,
               select1.pointsamount AS pointsamount
        FROM select1
        WHERE NOT EXISTS(SELECT 1
                         FROM select2
                         WHERE select1.peer1 = select2.peer1
                           AND select1.peer2 = select2.peer2)
        UNION ALL
        SELECT select2.Peer1        AS peer1,
               select2.peer2        AS peer2,
               select2.pointsamount AS pointsamount
        FROM select2
        ORDER BY 1;
END;
$fnc_transferredpoints_human_readable$ LANGUAGE plpgsql;

SELECT *
FROM fnc_transferredpoints_human_readable();

-- ====================================================================================================================
-- ============ 3.2 Write a function that returns a table of the following form: ======================================
-- ================ user name, name of the checked task, number of XP received  =======================================
-- ====================================================================================================================

CREATE OR REPLACE FUNCTION fnc_peer_task_xp()
    RETURNS TABLE
            (
                peer VARCHAR,
                task VARCHAR,
                xp   BIGINT
            )
AS
$fnc_peer_task_xp$
BEGIN
    RETURN QUERY
        SELECT checks.peer AS peer,
               checks.task AS task,
               xp.xpamount AS xp
        FROM checks
                 JOIN xp ON checks.id = xp."check"
        WHERE fnc_check_task_is_passed(checks.id) IS TRUE;
END;
$fnc_peer_task_xp$ LANGUAGE plpgsql;

SELECT *
FROM fnc_peer_task_xp();

-- ====================================================================================================================
-- ============ 3.3 Write a function that finds the peers who have not left campus for the whole day ==================
-- ====================================================================================================================

CREATE OR REPLACE FUNCTION fnc_peer_whole_day_in_campus(check_date DATE DEFAULT '2022-11-05')
    RETURNS TABLE
            (
                peer VARCHAR
            )
AS
$fnc_peer_whole_day_in_campus$
BEGIN
    RETURN QUERY
        WITH select_all as
                 (SELECT timetracking.peer, date, COUNT(timetracking."state") AS count_exit
                  FROM timetracking
                  WHERE date = check_date
                    AND "state" = 2
                  GROUP by 1, 2)

        SELECT select_all.peer
        FROM select_all
        WHERE select_all.count_exit = 1;

END;
$fnc_peer_whole_day_in_campus$ LANGUAGE plpgsql;

SELECT *
FROM fnc_peer_whole_day_in_campus('2022-11-05');


-- ====================================================================================================================
-- ============ 3.4 Find the percentage of successful and unsuccessful checks for all time ============================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_checks_percentage(OUT successful_checks_percent INT, OUT failed_checks_percent INT)
AS
$prc_checks_percentage$
BEGIN

    WITH count_s AS (SELECT COUNT(checks.id)
                                AS success
                     FROM checks
                     WHERE fnc_check_task_is_passed(checks.id) IS TRUE),

         count_f AS (SELECT COUNT(checks.id)
                                AS failure
                     FROM checks
                     WHERE fnc_check_task_is_passed(checks.id) IS FALSE)
    SELECT (success * 100 / (success + failure)),
           (failure * 100 / (success + failure))
    FROM count_s
             CROSS JOIN count_f
    INTO successful_checks_percent, failed_checks_percent;

END;
$prc_checks_percentage$ LANGUAGE plpgsql;

CALL prc_checks_percentage(0, 0);

-- ====================================================================================================================
-- ======= 3.5 Calculate the change in the number of peer points of each peer using the TransferredPoints table =======
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_points_change(INOUT points_change REFCURSOR)
AS
$prc_points_change$
BEGIN
    OPEN points_change FOR
        WITH select1 AS (SELECT table1.checkingpeer                       AS peer1,
                                table1.checkedpeer                        AS peer2,
                                table2.pointsamount - table1.pointsamount AS pointsamount
                         FROM transferredpoints AS table1
                                  LEFT JOIN transferredpoints AS table2
                                            ON table1.checkingpeer = table2.checkedpeer AND
                                               table1.checkedpeer = table2.checkingpeer),

             select2 AS
                 (SELECT select1.peer1                  AS peer1,
                         select1.peer2                  AS peer2,
                         transferredpoints.pointsamount AS pointsamount
                  FROM transferredpoints
                           JOIN select1
                                ON select1.peer1 = transferredpoints.checkingpeer
                                    AND select1.peer2 = transferredpoints.checkedpeer
                                    AND select1.pointsamount IS NULL)

        SELECT select1.peer2        AS peer,
               select1.pointsamount AS points_change
        FROM select1
        WHERE NOT EXISTS(SELECT 1
                         FROM select2
                         WHERE select1.peer1 = select2.peer1
                           AND select1.peer2 = select2.peer2)
        UNION ALL
        SELECT select2.peer2        AS peer,
               select2.pointsamount AS points_change
        FROM select2
        ORDER BY 1;
END;
$prc_points_change$ LANGUAGE plpgsql;

BEGIN;
CALL prc_points_change('points_change');
FETCH ALL FROM "points_change";
END;

-- ====================================================================================================================
-- =============== 3.6 Calculate the change in the number of peer points of each peer =================================
-- ==================== using the table returned by the first function from Part 3 ====================================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_points_change_from_3_1(INOUT points_change REFCURSOR)
AS
$prc_points_change_from_3_1$
BEGIN
    OPEN points_change FOR
        SELECT peer2 AS peer, pointsamount * -1 AS points_change
        FROM fnc_transferredpoints_human_readable();
END;
$prc_points_change_from_3_1$ LANGUAGE plpgsql;

BEGIN;
CALL prc_points_change_from_3_1('points_change');
FETCH ALL FROM "points_change";
END;

-- ====================================================================================================================
-- =========================== 3.7 Find the most frequently checked task for each day =================================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_most_checked(INOUT most_checked REFCURSOR)
AS
$prc_most_checked$
BEGIN
    OPEN most_checked FOR
        SELECT date, task
        FROM (SELECT date, task, count, ROW_NUMBER() OVER (PARTITION BY date ORDER BY count DESC) AS row_num
              FROM (SELECT date, task, COUNT(task) AS count
                    FROM (SELECT date, task
                          FROM checks) AS T1
                    GROUP BY date, task) AS T2) AS T3
        WHERE row_num = 1
        ORDER BY date;
END;
$prc_most_checked$ LANGUAGE plpgsql;

BEGIN;
CALL prc_most_checked('most_checked');
FETCH ALL FROM "most_checked";
END;

-- ====================================================================================================================
-- =========================== 3.8 Determine the duration of the last P2P check =======================================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_last_check_duration(INOUT check_duration REFCURSOR)
AS
$prc_last_check_duration$
BEGIN
    OPEN check_duration FOR
        WITH last_finished AS
                 (SELECT checks.id AS finished_id, p2p.time
                  FROM checks
                           JOIN p2p ON p2p."check" = checks.id
                  WHERE fnc_check_current_for_p2p_status(p2p."check") IS TRUE
                  ORDER BY checks.id DESC
                  LIMIT 1),
             start AS
                 (SELECT p2p.time AS time
                  FROM p2p
                           JOIN last_finished ON last_finished.finished_id = p2p."check"
                  WHERE p2p.state = 'Start'),
             finish AS
                 (SELECT p2p.time AS time
                  FROM p2p
                           JOIN last_finished ON last_finished.finished_id = p2p."check"
                  WHERE p2p.state = 'Success'
                     OR p2p.state = 'Failure')

        SELECT (finish.time - start.time)::time AS last_check_duration
        FROM finish
                 CROSS JOIN start;
END;
$prc_last_check_duration$ LANGUAGE plpgsql;

BEGIN;
CALL prc_last_check_duration('check_duration');
FETCH ALL FROM "check_duration";
END;

-- ====================================================================================================================
-- =================== 3.9 Find all peers who have completed the whole given block of tasks ===========================
-- =============================== and the completion date of the last task ===========================================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_peer_completed_task_block(block VARCHAR, INOUT peer REFCURSOR)
AS
$prc_peer_completed_task_block$
BEGIN
    OPEN peer FOR
        WITH select_block AS (SELECT title
                              FROM tasks
                              WHERE title LIKE block || '_\_%'
                              ORDER BY tasks.title DESC
                              LIMIT 1),

             select_all_peers AS (SELECT checks.peer, checks.id
                                  FROM checks
                                  WHERE task LIKE block || '_\_%'
                                    AND fnc_check_task_is_passed(checks.id))

        SELECT DISTINCT checks.peer, checks.date as day
        FROM checks
                 JOIN select_all_peers ON checks.peer = select_all_peers.peer
            AND select_all_peers.id = checks.id
                 JOIN select_block ON checks.task = select_block.title
        WHERE checks.task = select_block.title
        ORDER BY day DESC;
END;
$prc_peer_completed_task_block$ LANGUAGE plpgsql;

BEGIN;
CALL prc_peer_completed_task_block('C', 'peer');
FETCH ALL FROM "peer";
END;

-- ====================================================================================================================
-- =================== 3.10 Determine which peer each student should go to for a check ================================
-- ====================================================================================================================

-- You should determine it according to the recommendations of the peer's friends,
-- i.e. you need to find the peer with the greatest number of friends who recommend to be checked by him.
-- Output format: peer's nickname, nickname of the checker found

CREATE OR REPLACE PROCEDURE prc_most_recommended(INOUT most_recommended REFCURSOR)
AS
$prc_most_recommended$
BEGIN
    OPEN most_recommended FOR
        WITH select_friends AS
                 (SELECT peers.nickname, peer2 as friend
                  FROM friends
                           JOIN peers ON peer1 = peers.nickname),

             select_recommendations AS
                 (SELECT select_friends.nickname, select_friends.friend, recommendedpeer
                  FROM recommendations
                           JOIN select_friends ON friend = recommendations.peer
                  WHERE recommendedpeer <> select_friends.nickname
                  GROUP BY 1, 2, 3),

             select_top AS
                 (SELECT select_recommendations.nickname        as peer,
                         select_recommendations.recommendedpeer as r_peer,
                         COUNT(recommendedpeer)                 as count
                  FROM select_recommendations
                  GROUP BY 1, 2
                  ORDER by 3 desc)

        SELECT peer, r_peer
        FROM (SELECT peer, r_peer, count, ROW_NUMBER() OVER (PARTITION BY peer ORDER BY count DESC) AS row_num
              FROM select_top) AS t1
        WHERE row_num = 1;

END;
$prc_most_recommended$ LANGUAGE plpgsql;

BEGIN;
CALL prc_most_recommended('most_recommended');
FETCH ALL FROM "most_recommended";
END;

-- ====================================================================================================================
-- =================== 3.11 Determine the percentage of peers who: ====================================================
-- =========== Started block 1 == Started block 2 == Started both == Have not started any of them =====================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_blocks_started(block_1 VARCHAR, block_2 VARCHAR, INOUT blocks_started REFCURSOR)
AS
$prc_blocks_started$
BEGIN
    OPEN blocks_started FOR
        WITH preselect_1 AS (SELECT DISTINCT peer AS peer1_preselect
                             FROM checks
                             WHERE task LIKE block_1 || '_\_%'),
             preselect_2 AS (SELECT DISTINCT peer AS peer2_preselect
                             FROM checks
                             WHERE task LIKE block_2 || '_\_%'),
             select_1 AS (SELECT DISTINCT peer AS peer1
                          FROM checks
                                   LEFT JOIN preselect_2 ON peer2_preselect = peer
                          WHERE task LIKE block_1 || '_\_%'
                            AND peer2_preselect IS NULL),

             select_2 AS (SELECT DISTINCT peer AS peer2
                          FROM checks

                                   LEFT JOIN preselect_1 ON peer1_preselect = peer
                          WHERE task LIKE block_2 || '_\_%'
                            AND peer1_preselect IS NULL),
             select_3 AS (SELECT DISTINCT peer1_preselect AS peer3
                          FROM preselect_1
                                   JOIN preselect_2 ON peer1_preselect = peer2_preselect),
             select_4 AS
                 (SELECT nickname AS peer4
                  FROM peers
                  EXCEPT ALL
                  SELECT peer
                  FROM checks),
             count_1 AS (SELECT count(peer1) AS count_started_1
                         FROM select_1),
             count_2 AS (SELECT count(peer2) AS count_started_2
                         FROM select_2),
             count_3 AS (SELECT count(peer3) AS count_started_both
                         FROM select_3),
             count_4 AS (SELECT count(peer4) AS count_didnt_started_any
                         FROM select_4),
             count_all AS (SELECT count(all_peers) AS count_all_peers
                           FROM (SELECT nickname AS all_peers
                                 FROM peers) AS peers)

        SELECT ROUND(count_started_1 * 100 / count_all_peers::numeric, 2)         AS started_block_1_only,
               ROUND(count_started_2 * 100 / count_all_peers::numeric, 2)         AS started_block_2_only,
               ROUND(count_started_both * 100 / count_all_peers::numeric, 2)      AS started_both_blocks,
               ROUND(count_didnt_started_any * 100 / count_all_peers::numeric, 2) AS didnt_started_any_block

        FROM count_1
                 CROSS JOIN count_2
                 CROSS JOIN count_3
                 CROSS JOIN count_4
                 CROSS JOIN count_all;

END;
$prc_blocks_started$ LANGUAGE plpgsql;

BEGIN;
CALL prc_blocks_started('C', 'CPP', 'blocks_started');
FETCH ALL FROM "blocks_started";
END;



-- ====================================================================================================================
-- ======================= 3.12 Determine N peers with the greatest number of friends =================================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_peer_friends(number BIGINT, INOUT peer REFCURSOR)
AS
$prc_peer_friends$
BEGIN
    OPEN peer FOR
        SELECT select_all.peer, select_all.friends_count
        FROM (SELECT peer1 AS peer, count(peer2) AS friends_count
              FROM friends
              GROUP BY 1
              ORDER by 2 DESC
              LIMIT number) AS select_all
        ORDER BY 2 ASC;
END;
$prc_peer_friends$ LANGUAGE plpgsql;

BEGIN;
CALL prc_peer_friends(3, 'peer');
FETCH ALL FROM "peer";
END;

-- ====================================================================================================================
-- ======== 3.13 Determine the percentage of peers who have ever successfully passed a check on their birthday ========
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_birthday_check_success(INOUT peer_check REFCURSOR)
AS
$prc_birthaday_check_success$
BEGIN
    OPEN peer_check FOR
        WITH count_success AS
                 (SELECT count(peer) AS peer_succeeded
                  FROM (SELECT peer
                        FROM checks
                                 JOIN peers ON checks.peer = peers.nickname
                        WHERE fnc_check_task_is_passed(checks.id)
                          AND to_char(checks.date, 'MM-DD') = to_char(peers.birthday, 'MM-DD')) AS select_success),

             count_failure AS
                 (SELECT count(peer) AS peer_failed
                  FROM (SELECT peer
                        FROM checks
                                 JOIN peers ON checks.peer = peers.nickname
                        WHERE fnc_check_task_is_passed(checks.id) IS FALSE
                          AND to_char(checks.date, 'MM-DD') = to_char(peers.birthday, 'MM-DD')) AS select_failure)

        SELECT peer_succeeded * 100 / (peer_succeeded + peer_failed) AS successfull_checks,
               peer_failed * 100 / (peer_succeeded + peer_failed)    AS unsuccessful_checks
        FROM count_success
                 CROSS JOIN count_failure;
END;
$prc_birthaday_check_success$ LANGUAGE plpgsql;

BEGIN;
CALL prc_birthday_check_success('peer_check');
FETCH ALL FROM "peer_check";
END;

-- ====================================================================================================================
-- ============================= 3.14 Determine the total amount of XP gained by each peer ============================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_count_total_xp(INOUT total_xp REFCURSOR)
AS
$prc_count_total_xp$
BEGIN
    OPEN total_xp FOR
        WITH have_xp AS (SELECT peer, SUM(s_1.xp_amount) AS sum
                         FROM (SELECT peer, checks.task, MAX(xp.xpamount) AS xp_amount
                               FROM xp
                                        JOIN checks on xp."check" = checks.id
                               GROUP BY 1, 2) AS s_1
                         GROUP BY 1)

        SELECT peers.nickname           AS peer,
               COALESCE(have_xp.sum, 0) AS xp_amount
        FROM peers
                 LEFT JOIN have_xp ON peers.nickname = have_xp.peer
        ORDER BY 2 DESC;
END;
$prc_count_total_xp$ LANGUAGE plpgsql;

BEGIN;
CALL prc_count_total_xp('total_xp');
FETCH ALL FROM "total_xp";
END;

-- ====================================================================================================================
-- =========== 3.15 Determine all peers who did the given tasks 1 and 2, but did not do task 3 ========================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_tasks_out_of_3(task_1 VARCHAR,
                                               task_2 VARCHAR,
                                               task_3 VARCHAR,
                                               INOUT tasks_out_of_3 REFCURSOR)
AS
$prc_tasks_out_of_3$
BEGIN
    OPEN tasks_out_of_3 FOR
        WITH preselect_1 AS (SELECT DISTINCT peer AS peer1_preselect
                             FROM checks
                             WHERE task = task_1
                               AND fnc_check_task_is_passed((checks.id))),
             preselect_2 AS (SELECT DISTINCT peer AS peer2_preselect
                             FROM checks
                             WHERE task = task_2
                               AND fnc_check_task_is_passed((checks.id))),
             preselect_3 AS (SELECT DISTINCT peer AS peer3_preselect
                             FROM checks
                             WHERE task = task_3
                               AND fnc_check_task_is_passed((checks.id)) IS FALSE)

        SELECT DISTINCT peer1_preselect AS peer
        FROM preselect_1
                 JOIN preselect_2 ON peer1_preselect = peer2_preselect
                 LEFT JOIN preselect_3 ON peer1_preselect = peer3_preselect
        WHERE peer3_preselect IS NULL;


END;
$prc_tasks_out_of_3$ LANGUAGE plpgsql;

BEGIN;
CALL prc_tasks_out_of_3('C2_SimpleBashUtils',
                        'C3_s21_string+',
                        'C4_s21_math',
                        'tasks_out_of_3');
FETCH ALL FROM "tasks_out_of_3";
END;

-- ====================================================================================================================
-- ==== 3.16 Using recursive common table expression, output the number of preceding tasks for each task ==============
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_count_parent_tasks(
    INOUT count_parent_task REFCURSOR)
AS
$prc_count_parent_tasks$
BEGIN
    OPEN count_parent_task FOR
        WITH RECURSIVE recursion AS
                           (SELECT tasks.title AS task, 0 AS prevcount
                            FROM tasks
                            WHERE parenttask IS NULL
                            UNION ALL
                            SELECT tasks.title AS task, recursion.prevcount + 1 AS prevcount
                            FROM tasks
                                     JOIN recursion ON recursion.task = tasks.parenttask)

        SELECT *
        FROM recursion;
END;
$prc_count_parent_tasks$ LANGUAGE plpgsql;

BEGIN;
CALL prc_count_parent_tasks('count_parent_task');
FETCH ALL FROM "count_parent_task";
END;

-- ====================================================================================================================
-- ============================ 3.17 Find "lucky" days for checks. ====================================================
-- ==== A day is considered "lucky" if it has at least N consecutive successful checks ================================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_lucky_days(
    days_num INT, INOUT lucky_days REFCURSOR)
AS
$prc_lucky_days$
BEGIN
    OPEN lucky_days FOR
        WITH select_days AS (SELECT checks.date, checks.id
                             FROM checks
                                      JOIN tasks ON checks.task = tasks.title
                                      JOIN xp ON checks.id = xp."check"
                             WHERE (fnc_check_task_is_passed(checks.id))
                               AND (xp.XPAmount / tasks.MaxXP::numeric >= 0.8)),

             check_next AS (SELECT select_days.date,
                                   (LEAD(select_days.date) OVER
                                       (ORDER BY select_days.date) = select_days.date)
                            FROM select_days),

             count_selected AS (SELECT check_next.date, COUNT(check_next.date) AS real_count
                                FROM check_next
                                GROUP BY 1),

             final_select AS (SELECT count_selected.date, MAX(real_count) AS count_needed
                              FROM count_selected
                              GROUP BY 1)

        SELECT final_select.date AS lucky_days
        FROM final_select
        WHERE count_needed >= days_num;


END;
$prc_lucky_days$ LANGUAGE plpgsql;

BEGIN;
CALL prc_lucky_days(2, 'lucky_days');
FETCH ALL FROM "lucky_days";
END;

-- ====================================================================================================================
-- ============================ 3.18 Determine the peer with the greatest number of completed tasks ===================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_best_student_by_completed_tasks(
    INOUT best_student_by_completed_tasks REFCURSOR)
AS
$prc_best_student_by_completed_tasks$
BEGIN
    OPEN best_student_by_completed_tasks FOR
        SELECT peer, count(title) AS completed_tasks
        FROM (SELECT DISTINCT tasks.title, peer
              FROM checks
                       JOIN tasks on checks.task = tasks.title
                  AND fnc_check_task_is_passed(checks.id)) AS select_all
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 1;


END;
$prc_best_student_by_completed_tasks$ LANGUAGE plpgsql;

BEGIN;
CALL prc_best_student_by_completed_tasks('best_student_by_completed_tasks');
FETCH ALL FROM "best_student_by_completed_tasks";
END;

-- ====================================================================================================================
-- ============================ 3.19 Find the peer with the highest amount of XP =====================================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_best_student_by_xp(
    INOUT best_student_by_xp REFCURSOR)
AS
$prc_best_student_by_xp$
BEGIN
    OPEN best_student_by_xp FOR
        WITH have_xp AS (SELECT peer, SUM(s_1.xp_amount) AS sum
                         FROM (SELECT peer, checks.task, MAX(xp.xpamount) AS xp_amount
                               FROM xp
                                        JOIN checks on xp."check" = checks.id
                               GROUP BY 1, 2) AS s_1
                         GROUP BY 1)

        SELECT peers.nickname           AS peer,
               COALESCE(have_xp.sum, 0) AS xp
        FROM peers
                 LEFT JOIN have_xp ON peers.nickname = have_xp.peer
        ORDER BY 2 DESC
        LIMIT 1;

END;
$prc_best_student_by_xp$ LANGUAGE plpgsql;

BEGIN;
CALL prc_best_student_by_xp('best_student_by_xp');
FETCH ALL FROM "best_student_by_xp";
END;

-- ====================================================================================================================
-- =================== 3.20 Find the peer who spent the longest amount of time on campus today ========================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_peer_longest_time_in_campus(
    INOUT peer_longest_time_in_campus REFCURSOR)
AS
$prc_peer_longest_time_in_campus$
BEGIN
    OPEN peer_longest_time_in_campus FOR
        WITH entry AS (SELECT ROW_NUMBER() OVER (ORDER BY peer, time) AS id, peer, time
                       FROM timetracking
                       WHERE date = current_date
                         AND state = 1),

             exit AS (SELECT ROW_NUMBER() OVER (ORDER BY peer, time) AS id, peer, time
                      FROM timetracking
                      WHERE date = current_date
                        AND state = 2)

        SELECT entry.peer
        FROM entry
                 JOIN exit USING (peer, id)
        GROUP by 1
        ORDER by SUM(exit.time - entry.time) DESC
        LIMIT 1;
END;
$prc_peer_longest_time_in_campus$ LANGUAGE plpgsql;

BEGIN;
CALL prc_peer_longest_time_in_campus('peer_longest_time_in_campus');
FETCH ALL FROM "peer_longest_time_in_campus";
END;

-- ====================================================================================================================
-- ====== 3.21 Determine the peers that came before the given time at least N times during the whole time =============
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_peers_entered_before(
    given_time TIME, entries INT, INOUT peers_entered_before REFCURSOR)
AS
$prc_peers_entered_before$
BEGIN
    OPEN peers_entered_before FOR
        WITH select_entries AS (SELECT peer, COUNT(time) AS count
                               FROM timetracking
                               WHERE state = 1
                                 AND time < given_time
                               GROUP BY 1)
        SELECT peer
        FROM select_entries
        WHERE count >= entries;
END;
$prc_peers_entered_before$ LANGUAGE plpgsql;

BEGIN;
CALL prc_peers_entered_before('11:00:00'::time, 2, 'peers_entered_before');
FETCH ALL FROM "peers_entered_before";
END;

-- ====================================================================================================================
-- ====== 3.22 Determine the peers who left the campus more than M times during the last N days =======================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_peers_left_campus(
    number_of_days INT, number_of_times INT, INOUT peers_left_campus REFCURSOR)
AS
$prc_peers_left_campus$
BEGIN
    OPEN peers_left_campus FOR
        WITH select_exits AS (SELECT peer, timetracking.date, COUNT(date) AS count
                              FROM timetracking
                              WHERE state = 2
                              GROUP BY 1, 2),
             count_exits_and_days AS
                 (SELECT peer, COUNT(select_exits.date) AS count_days, SUM(select_exits.count) AS total_exits
                  FROM select_exits
                  GROUP BY 1)

        SELECT peer
        FROM count_exits_and_days
        WHERE count_days >= number_of_days
          AND total_exits >= number_of_times;
END;
$prc_peers_left_campus$ LANGUAGE plpgsql;

BEGIN;
CALL prc_peers_left_campus(2, 3, 'peers_left_campus');
FETCH ALL FROM "peers_left_campus";
END;

-- ====================================================================================================================
-- ========================= 3.23 Determine which peer was the last to come in today ==================================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_peer_came_last_today(INOUT peer_came_last_today REFCURSOR)
AS
$prc_peer_came_last_today$
BEGIN
    OPEN peer_came_last_today FOR
        WITH select_entries AS (SELECT DISTINCT ROW_NUMBER() OVER (PARTITION BY peer) AS id, peer, timetracking.time
                               FROM timetracking
                               WHERE state = 1
                                 AND date = current_date::date
                               ORDER BY 2 DESC)
        SELECT peer
        FROM (SELECT peer, time
              FROM select_entries
              WHERE id = 1
              ORDER BY time DESC
              LIMIT 1) AS first_entry;
END;
$prc_peer_came_last_today$ LANGUAGE plpgsql;

BEGIN;
CALL prc_peer_came_last_today('peer_came_last_today');
FETCH ALL FROM "peer_came_last_today";
END;

-- ====================================================================================================================
-- =================== 3.24 Determine the peer that left campus yesterday for more than N minutes =====================
-- ====================================================================================================================

CREATE OR REPLACE PROCEDURE prc_peers_left_campus_for_some_time(
    absence_time_in_minutes INT, INOUT peers_left_campus_for_some_time REFCURSOR)
AS
$prc_peers_left_campus_for_some_time$
BEGIN
    OPEN peers_left_campus_for_some_time FOR
        WITH entry AS (SELECT ROW_NUMBER() OVER (ORDER BY peer, time) AS id, peer, time
                       FROM timetracking
                       WHERE date = current_date - interval '1 day'
                         AND state = 1),

             exit AS (SELECT ROW_NUMBER() OVER (ORDER BY peer, time) AS id, peer, time
                      FROM timetracking
                      WHERE date = current_date - interval '1 day'
                        AND state = 2),

             exit_excluding_last AS
                 (SELECT exit.peer, exit.time AS exit_time
                  FROM exit
                  EXCEPT
                  (SELECT peer, MAX(time)
                   FROM exit
                   GROUP BY 1)),

             entry_excluding_first AS
                 (SELECT entry.peer, entry.time AS entry_time
                  FROM entry
                  EXCEPT
                  (SELECT peer, MIN(time)
                   FROM entry
                   GROUP BY 1))
        SELECT exit_excluding_last.peer
        FROM exit_excluding_last
                 JOIN entry_excluding_first USING (peer)
        WHERE (EXTRACT
                   (EPOCH
                    FROM entry_excluding_first.entry_time - exit_excluding_last.exit_time) / 60
                  ) >= absence_time_in_minutes;
END;
$prc_peers_left_campus_for_some_time$ LANGUAGE plpgsql;

BEGIN;
CALL prc_peers_left_campus_for_some_time
    (31, 'peers_left_campus_for_some_time');
FETCH ALL FROM "peers_left_campus_for_some_time";
END;

-- ====================================================================================================================
-- ======================== 3.25 Determine for each month the percentage of early entries =============================
-- ====================================================================================================================
-- For each month, count how many times people born in that month
-- came to campus during the whole time (we'll call this the total number of entries).

-- For each month, count the number of times people born in that month
-- have come to campus before 12:00 in all time (we'll call this the number of early entries).

-- For each month, count the percentage of early entries to campus relative to the total number of entries.
-- Output format: month, percentage of early entries

CREATE OR REPLACE PROCEDURE prc_birth_month_in_campus(INOUT birth_month_in_campus REFCURSOR)
AS
$prc_birth_month_in_campus$
BEGIN
    OPEN birth_month_in_campus FOR
        WITH select_all_entries AS (SELECT peer,
                                           time,
                                           date AS month
                                    FROM timetracking
                                             JOIN peers on peers.nickname = timetracking.peer
                                    WHERE to_char(timetracking.date, 'Month') = to_char(peers.birthday, 'Month')
                                      AND state = 1),

             select_first_entries AS (SELECT peer, month, MIN(time)::time AS first_entry
                                      FROM select_all_entries
                                      GROUP BY 1, 2),

             all_entries_by_month AS (SELECT to_char(month, 'Month') AS month, SUM(count_total) AS sum_total
                                      FROM (SELECT month, peer, COUNT(first_entry) AS count_total
                                            FROM select_first_entries
                                            GROUP BY 1, 2) AS count_first
                                      GROUP BY 1),

             early_entries_by_month AS (SELECT to_char(month, 'Month') AS month, SUM(count_early) AS sum_early
                                        FROM (SELECT month, COUNT(first_entry) AS count_early
                                              FROM select_first_entries
                                              WHERE select_first_entries.first_entry < '12:00:00'::time
                                              GROUP BY 1) AS e_1
                                        GROUP BY 1)


        SELECT early_entries_by_month.month,
               ROUND((early_entries_by_month.sum_early * 100 / all_entries_by_month.sum_total),
                     2) AS early_entries
        FROM early_entries_by_month
                 JOIN all_entries_by_month
                      ON early_entries_by_month.month = all_entries_by_month.month;

END;
$prc_birth_month_in_campus$ LANGUAGE plpgsql;

BEGIN;
CALL prc_birth_month_in_campus('birth_month_in_campus');
FETCH ALL FROM "birth_month_in_campus";
END;