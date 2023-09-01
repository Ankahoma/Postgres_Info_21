DROP DATABASE IF EXISTS part4;
DROP SCHEMA IF EXISTS public CASCADE;

CREATE DATABASE part4;
CREATE SCHEMA public;

-- ====================================================================================================================
-- ======================================== 4.0 CREATE TABLES FOR PART 4 ==============================================
-- ====================================================================================================================

CREATE TABLE IF NOT EXISTS TableName_1
(
    id     SERIAL PRIMARY KEY,
    status BIGINT DEFAULT 1
);

CREATE TABLE IF NOT EXISTS TableName_2
(
    id     SERIAL PRIMARY KEY,
    status BIGINT DEFAULT 2
);

CREATE TABLE IF NOT EXISTS TableName_3
(
    id     SERIAL PRIMARY KEY,
    status BIGINT DEFAULT 3
);

CREATE TABLE IF NOT EXISTS TableName_4
(
    id     SERIAL PRIMARY KEY,
    status BIGINT DEFAULT 4
);

CREATE TABLE IF NOT EXISTS Table_____Name
(
    id     SERIAL PRIMARY KEY,
    status BIGINT DEFAULT 4
);

-- ====================================================================================================================
-- ====================== 4.1 Create a stored procedure that, without destroying the database, ========================
-- ============= destroys all those tables in the current database whose names begin with the phrase 'TableName'. =====
-- ====================================================================================================================

CREATE
    OR REPLACE PROCEDURE prc_drop_tables_by_name()
AS
$prc_drop_tables_by_name$
DECLARE
    statement text;
BEGIN
    FOR statement IN
        SELECT drop_query
        FROM (SELECT 'DROP TABLE IF EXISTS ' || TABLE_NAME || ';' AS drop_query
              FROM INFORMATION_SCHEMA.TABLES
              WHERE TABLE_NAME LIKE 'tablename%') AS select_tables
        LOOP
            EXECUTE statement;
        END LOOP;
END;

$prc_drop_tables_by_name$ LANGUAGE plpgsql;


CALL prc_drop_tables_by_name();


-- ====================================================================================================================
-- ================= 4.2 Create a stored procedure with an output parameter that outputs ==============================
-- ========= a list of names and parameters of all scalar user's SQL functions in the current database. ===============
-- ============================== Do not output function names without parameters. ====================================
-- ========================== The names and the list of parameters must be in one string. =============================
-- ========================== The output parameter returns the number of functions found. =============================
-- ====================================================================================================================
CREATE OR REPLACE FUNCTION fnc_4_2_with_parameters_1(number BIGINT, string VARCHAR)
RETURNS INTEGER AS $fnc_with_parameters_1$ BEGIN END $fnc_with_parameters_1$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_4_2_with_parameters_2(string VARCHAR, number BIGINT)
RETURNS INTEGER AS $fnc_with_parameters_2$ BEGIN END $fnc_with_parameters_2$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_4_2_with_parameters_3(string1 VARCHAR, string2 VARCHAR)
RETURNS INTEGER AS $fnc_with_parameters_3$ BEGIN END $fnc_with_parameters_3$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_4_2_with_parameters_4(number1 BIGINT, number2 BIGINT)
RETURNS INTEGER AS $fnc_with_parameters_4$ BEGIN END $fnc_with_parameters_4$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_4_2_without_parameters_1()
RETURNS INTEGER AS $fnc_without_parameters_1$ BEGIN END $fnc_without_parameters_1$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fnc_4_2_without_parameters_2()
RETURNS INTEGER AS $fnc_without_parameters_2$ BEGIN END $fnc_without_parameters_2$ LANGUAGE plpgsql;

-- ==================================================================================================================

CREATE
    OR REPLACE PROCEDURE prc_get_user_functions_list(OUT functions_counter BIGINT, IN user_functions_list REFCURSOR)
AS
$prc_get_user_functions_list$
BEGIN
    CREATE TEMPORARY VIEW show_user_functions AS
        SELECT function_name || '(' || function_parameters || ')' AS user_function
        FROM (SELECT routine_name                                                   AS function_name,
                     STRING_AGG(information_schema.parameters.parameter_name, ', ') AS function_parameters
              FROM information_schema.routines
                       JOIN information_schema.parameters ON routines.specific_name = parameters.specific_name
              WHERE routine_type = 'FUNCTION'
                AND routine_schema = 'public'
              GROUP BY 1) AS select_functions;

    OPEN user_functions_list FOR
        SELECT * FROM show_user_functions;

        SELECT COUNT(*)
        INTO functions_counter
        FROM show_user_functions;

    DROP VIEW show_user_functions;
END;

$prc_get_user_functions_list$ LANGUAGE plpgsql;

BEGIN;
CALL prc_get_user_functions_list(0,'user_functions_list');
FETCH ALL FROM "user_functions_list";
END;


-- ====================================================================================================================
-- ===================== 4.3 Create a stored procedure with output parameter, =========================================
-- ==================== which destroys all SQL DML triggers in the current database. ==================================
-- =================== The output parameter returns the number of destroyed triggers. =================================
-- ====================================================================================================================
CREATE TABLE IF NOT EXISTS TableName_1
(
    id     SERIAL PRIMARY KEY,
    status BIGINT DEFAULT 1
);

CREATE TABLE IF NOT EXISTS TableName_2
(
    id     SERIAL PRIMARY KEY,
    status BIGINT DEFAULT 2
);

CREATE TABLE IF NOT EXISTS TableName_3
(
    id     SERIAL PRIMARY KEY,
    status BIGINT DEFAULT 3
);

CREATE TABLE IF NOT EXISTS TableName_4
(
    id     SERIAL PRIMARY KEY,
    status BIGINT DEFAULT 4
);

CREATE OR REPLACE FUNCTION fnc_trg_4_3_DML_1() RETURNS TRIGGER AS $$ BEGIN END; $$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION fnc_trg_4_3_DML_2() RETURNS TRIGGER AS $$ BEGIN END; $$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION fnc_trg_4_3_DML_3() RETURNS TRIGGER AS $$ BEGIN END; $$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION fnc_trg_4_3_DML_4() RETURNS TRIGGER AS $$ BEGIN END; $$ LANGUAGE plpgsql;


CREATE TRIGGER trigger_4_3_DML_1 AFTER INSERT ON tablename_1 FOR EACH ROW EXECUTE PROCEDURE fnc_trg_4_3_DML_1();
CREATE TRIGGER trigger_4_3_DML_2 BEFORE UPDATE ON tablename_2 FOR EACH ROW EXECUTE PROCEDURE fnc_trg_4_3_DML_2();
CREATE TRIGGER trigger_4_3_DML_3 AFTER UPDATE ON tablename_3 FOR EACH ROW EXECUTE PROCEDURE fnc_trg_4_3_DML_3();
CREATE TRIGGER trigger_4_3_DML_4 AFTER DELETE ON tablename_4 FOR EACH ROW EXECUTE PROCEDURE fnc_trg_4_3_DML_4();

CREATE OR REPLACE FUNCTION fnc_trg_4_3_DDL_1() RETURNS EVENT_TRIGGER AS $$ BEGIN END; $$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION fnc_trg_4_3_DDL_2() RETURNS EVENT_TRIGGER AS $$ BEGIN END; $$ LANGUAGE plpgsql;

CREATE EVENT TRIGGER trigger_4_3_DDL_1 ON ddl_command_start EXECUTE PROCEDURE fnc_trg_4_3_DDL_1();
CREATE EVENT TRIGGER trigger_4_3_DDL_2 ON ddl_command_start EXECUTE PROCEDURE fnc_trg_4_3_DDL_2();



CREATE
    OR REPLACE PROCEDURE prc_drop_triggers(OUT trigger_counter INTEGER)
AS
$prc_drop_triggers$
DECLARE
    statement TEXT;

BEGIN
    trigger_counter := 0;
    FOR statement IN
        SELECT drop_query
        FROM (SELECT 'DROP TRIGGER IF EXISTS ' || TRIGGER_NAME || ' ON ' || event_object_table ||' CASCADE;' AS drop_query
              FROM INFORMATION_SCHEMA.TRIGGERS) AS select_triggers
        LOOP
            EXECUTE statement;
            trigger_counter = trigger_counter + 1;
        END LOOP;
END;

$prc_drop_triggers$ LANGUAGE plpgsql;

SELECT trigger_name FROM information_schema.triggers;
CALL prc_drop_triggers(NULL);


DROP FUNCTION IF EXISTS fnc_trg_4_3_DML_1();
DROP FUNCTION IF EXISTS fnc_trg_4_3_DML_2();
DROP FUNCTION IF EXISTS fnc_trg_4_3_DML_3();
DROP FUNCTION IF EXISTS fnc_trg_4_3_DML_4();

DROP FUNCTION IF EXISTS fnc_trg_4_3_DDL_1() CASCADE;
DROP FUNCTION IF EXISTS fnc_trg_4_3_DDL_2() CASCADE;

-- ====================================================================================================================
-- ===================== 4.4 Create a stored procedure with an input parameter that outputs names =====================
-- ================== and descriptions of object types (only stored procedures and scalar functions) ==================
-- ============================ that have a string specified by the procedure parameters. =============================
-- ====================================================================================================================

CREATE
    OR REPLACE PROCEDURE prc_get_user_objects_list_by_pattern(INOUT pattern VARCHAR, IN user_functions_list REFCURSOR)
AS
$prc_get_user_functions_list$
BEGIN
    OPEN user_functions_list FOR
      SELECT routine_name                                                   AS object_name,
                     routine_type AS object_type
              FROM information_schema.routines
              WHERE (routine_type = 'FUNCTION'
              OR routine_type = 'PROCEDURE')
                AND routine_schema = 'public'
              AND routines.routine_definition LIKE '%' || pattern ||'%'
              GROUP BY 1,2;

END;

$prc_get_user_functions_list$ LANGUAGE plpgsql;

BEGIN;
CALL prc_get_user_objects_list_by_pattern('DROP TRIGGER','user_functions_list');
FETCH ALL FROM "user_functions_list";
END;

BEGIN;
CALL prc_get_user_objects_list_by_pattern('DROP TABLE','user_functions_list');
FETCH ALL FROM "user_functions_list";
END;
