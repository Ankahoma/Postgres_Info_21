-- ======================= TESTING P2P, TRANSFERRED POINTS AND CHECKS INSERTS ===========================================================


CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C5_s21_decimal'::varchar,
                          'Start'::checkstatus,
                          current_time::time); -- must not be inserted - previous not done - OK

CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C4_s21_math'::varchar,
                          'Success'::checkstatus,
                          (current_time + interval '15 minute')::time); -- must not be inserted - no start status - OK

CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C4_s21_math'::varchar,
                          'Start'::checkstatus,
                          current_time::time); -- must be inserted -  OK

CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C4_s21_math'::varchar,
                          'Failure'::checkstatus,
                          (current_time + interval '20 minute')::time); -- must be inserted -  OK

CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C5_s21_decimal'::varchar,
                          'Start'::checkstatus,
                          current_time::time); -- must not be inserted - previous task failure - OK

CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C4_s21_math'::varchar,
                          'Start'::checkstatus,
                          current_time::time); -- must be inserted - OK

CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C5_s21_decimal'::varchar,
                          'Start'::checkstatus,
                          current_time::time); -- must not be inserted - previous task start - OK


CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C4_s21_math'::varchar,
                          'Success'::checkstatus,
                          (current_time + interval '25 minute')::time); -- must be inserted -  OK

CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C5_s21_decimal'::varchar,
                          'Start'::checkstatus,
                          current_time::time); -- must be inserted - previous task success - OK

CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C5_s21_decimal'::varchar,
                          'Start'::checkstatus,
                          current_time::time); -- must not be inserted - OK


CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C5_s21_decimal'::varchar,
                          'Failure'::checkstatus,
                          (current_time + interval '35 minute')::time); -- must be inserted - OK

CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C5_s21_decimal'::varchar,
                          'Success'::checkstatus,
                          (current_time + interval '15 minute')::time); -- must not be inserted - OK

CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C5_s21_decimal'::varchar,
                          'Start'::checkstatus,
                          current_time::time); -- must be inserted - OK

CALL prc_insert_p2p_check('tprobius'::varchar,
                          'bdupp'::varchar,
                          'C3_s21_string+'::varchar,
                          'Start'::checkstatus,
                          current_time::time); -- must not be inserted - previous not done - OK

CALL prc_insert_p2p_check('tprobius'::varchar,
                          'bdupp'::varchar,
                          'C3_s21_string+'::varchar,
                          'Success'::checkstatus,
                          (current_time + interval '15 minute')::time); -- must not be inserted - no start status - OK


-- ================================= TESTING VERTER INSERT ===========================================================

CALL prc_insert_verter_check('bdupp'::varchar,
                             'CPP1_matrix'::varchar,
                             'Start'::checkstatus,
                             current_time::time); -- must not be inserted - OK

CALL prc_insert_verter_check('bdupp'::varchar,
                             'C4_s21_math'::varchar,
                             'Start'::checkstatus,
                             current_time::time); -- must be inserted - OK

CALL prc_insert_verter_check('bdupp'::varchar,
                             'C4_s21_math'::varchar,
                             'Success'::checkstatus,
                             current_time::time); -- must be inserted - OK

CALL prc_insert_verter_check('bdupp'::varchar,
                             'C5_s21_decimal'::varchar,
                             'Start'::checkstatus,
                             current_time::time); -- must not be inserted - OK

CALL prc_insert_verter_check('bdupp'::varchar,
                             'C4_s21_math'::varchar,
                             'Success'::checkstatus,
                             current_time::time); -- must not be inserted - OK

CALL prc_insert_verter_check('tprobius'::varchar,
                          'C3_s21_string+'::varchar,
                          'Start'::checkstatus,
                          current_time::time); -- must be inserted  - OK

CALL prc_insert_verter_check('tprobius'::varchar,
                          'C3_s21_string+'::varchar,
                          'Success'::checkstatus,
                          (current_time)::time); -- must be inserted OK


CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C5_s21_decimal'::varchar,
                          'Success'::checkstatus,
                          (current_time + interval '32 minute')::time);

CALL prc_insert_verter_check('bdupp'::varchar,
                             'C5_s21_decimal'::varchar,
                             'Start'::checkstatus,
                             current_time::time); -- must be inserted - OK

CALL prc_insert_verter_check('bdupp'::varchar,
                             'C5_s21_decimal'::varchar,
                             'Failure'::checkstatus,
                             current_time::time); -- must be inserted - OK

CALL prc_insert_verter_check('bdupp'::varchar,
                             'C5_s21_decimal'::varchar,
                             'Start'::checkstatus,
                             current_time::time); -- must NOT be inserted without p2p new start -  OK


CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C5_s21_decimal'::varchar,
                          'Success'::checkstatus,
                          (current_time + interval '15 minute')::time); -- must not be inserted - OK





-- =============================================== TESTING XP INSERT ===========================================================
-- DELETE
-- FROM xp;
-- ALTER SEQUENCE xp_id_seq RESTART WITH 1;

INSERT
INTO xp ("check", xpamount)
VALUES (14, 300); -- must be inserted - OK

INSERT
INTO xp ("check", xpamount)
VALUES (6, 251); -- must not be inserted - max xp exceeds - OK

INSERT
INTO xp ("check", xpamount)
VALUES (6, 250); -- must be inserted - OK

INSERT
INTO xp ("check", xpamount)
VALUES (6, 248); -- must not be inserted - check duplicates --------------------- OK

INSERT
INTO xp ("check", xpamount)
VALUES (7, 300); -- must not be inserted - p2p status = failure - OK

INSERT
INTO xp ("check", xpamount)
VALUES (8, 300); -- must be inserted - OK

INSERT
INTO xp ("check", xpamount)
VALUES (9, 400); -- must not be inserted - p2p status = failure - OK

INSERT
INTO xp ("check", xpamount)
VALUES (10, 400); -- must not be inserted - verter status = failure - OK

CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C5_s21_decimal'::varchar,
                          'Start'::checkstatus,
                          current_time::time);


CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C5_s21_decimal'::varchar,
                          'Success'::checkstatus,
                          (current_time + interval '34 minute')::time);

CALL prc_insert_verter_check('bdupp'::varchar,
                             'C5_s21_decimal'::varchar,
                             'Start'::checkstatus,
                             current_time::time);

CALL prc_insert_verter_check('bdupp'::varchar,
                             'C5_s21_decimal'::varchar,
                             'Success'::checkstatus,
                             (current_time + interval '28 minute')::time);



INSERT
INTO xp ("check", xpamount)
VALUES (11, 350);

CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C5_s21_decimal'::varchar,
                          'Start'::checkstatus,
                          current_time::time);


CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'C5_s21_decimal'::varchar,
                          'Success'::checkstatus,
                          (current_time + interval '19 minute')::time);

CALL prc_insert_verter_check('bdupp'::varchar,
                             'C5_s21_decimal'::varchar,
                             'Start'::checkstatus,
                             current_time::time);

CALL prc_insert_verter_check('bdupp'::varchar,
                             'C5_s21_decimal'::varchar,
                             'Success'::checkstatus,
                             current_time::time);

INSERT
INTO xp ("check", xpamount)
VALUES (12, 400);

CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'CPP1_matrix'::varchar,
                          'Start'::checkstatus,
                          current_time::time);


CALL prc_insert_p2p_check('bdupp'::varchar,
                          'tprobius'::varchar,
                          'CPP1_matrix'::varchar,
                          'Success'::checkstatus,
                          (current_time + interval '19 minute')::time);


INSERT
INTO xp ("check", xpamount)
VALUES (13, 400);

-- ====================================================================================================================
-- ===================================== TESTING EXPORT TO CSV PROCEDURE ==============================================
-- ====================================================================================================================

CALL prc_export_to_CSV('peers', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/Exported/save_peers.csv', ',');
CALL prc_export_to_CSV('tasks', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/Exported/save_tasks.csv', ',' );
CALL prc_export_to_CSV('friends', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/Exported/save_friends.csv', ',' );
CALL prc_export_to_CSV('recommendations', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/Exported/save_recommendations.csv', ',' );
CALL prc_export_to_CSV('timetracking', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/Exported/save_timetracking.csv', ',' );
CALL prc_export_to_CSV('checks', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/Exported/save_checks.csv', ',' );
CALL prc_export_to_CSV('verter', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/Exported/save_verter.csv', ',');
CALL prc_export_to_CSV('p2p', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/Exported/save_p2p.csv', ',');
CALL prc_export_to_CSV('transferredpoints', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/Exported/save_transferred_points.csv', ',');
CALL prc_export_to_CSV('xp', '/Users/annavertikova/PROJECTS/SQL2_Info21_v1.0-0/src/part1/Exported/save_xp.csv', ',');
