/**
 * Copyright (C) Zoomdata, Inc. 2012-2016. All rights reserved.
 */
package com.zoomdata.connector.example.provider.cratedb;

import com.google.common.collect.ImmutableSet;
import com.querydsl.core.types.Ops;
import com.querydsl.sql.MySQLTemplates;
import com.zoomdata.connector.example.framework.common.sql.ops.ExtendedDateTimeOps;

import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Set;

// CrateDB mostly closely mimics MySQL dialect, so use the existing QueryDSL templates and modify as needed
public class CrateDBSQLTemplates extends MySQLTemplates {

    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public CrateDBSQLTemplates() {
        this('\\', false);
    }

    public CrateDBSQLTemplates(char escape, boolean quote) {
        super(escape, quote);

        add(Ops.DateTimeOps.TRUNC_YEAR,   "DATE_TRUNC('year',{0})");
        add(ExtendedDateTimeOps.TRUNC_QUARTER, "DATE_TRUNC('quarter',{0})");
        add(Ops.DateTimeOps.TRUNC_MONTH,   "DATE_TRUNC('month',{0})");
        // The default date_trunc('week') operates on a different day of the week, so for day of week = 7 we have to
        // compensate. Not a ton of date/time function support, so use math.
        add(Ops.DateTimeOps.TRUNC_WEEK, "DATE_FORMAT('%Y-%m-%dT%H:%i:%s.000Z', ((DATE_TRUNC('week',{0}) - 86400000)) + (((EXTRACT(DOW from {0}) / 7) * (7 * 86400000))))");
        add(Ops.DateTimeOps.TRUNC_DAY,    "CAST(DATE_TRUNC('day',{0}) as timestamp)");
        add(Ops.DateTimeOps.TRUNC_HOUR,    "DATE_TRUNC('hour',{0})");
        add(Ops.DateTimeOps.TRUNC_MINUTE,    "DATE_TRUNC('minute',{0})");
        add(Ops.DateTimeOps.TRUNC_SECOND,    "DATE_TRUNC('second',{0})");
        add(ExtendedDateTimeOps.FROM_UNIXTIME, "DATE_FORMAT(CAST({0} * 1000 as timestamp))");
        add(ExtendedDateTimeOps.FROM_MILLIS, "DATE_FORMAT(CAST({0} as timestamp))");
        // Integration tests expect fully formatted dates with milliseconds
        add(ExtendedDateTimeOps.FROM_YEARPATTERN,"DATE_FORMAT('%Y-%m-%dT%H:%i:%s.000Z', format('%s-01-01',{0}))");
    }

    @Override
    public String serialize(String literal, int jdbcType) {
        switch (jdbcType) {
            case Types.TIMESTAMP:
                try {
                    // CrateDB expects time to cast in ISO format
                    return "cast('" + dateFormatter.parse(literal).toInstant() + "' as timestamp)";
                } catch (ParseException e) {
                    throw new IllegalArgumentException("Could not parse " + literal + " to valid time format", e);
                }
            default:
                return super.serialize(literal, jdbcType);
        }
    }

    // Using the MySQL templates so we have to override this way
    // Normally we would register these with QueryDSL but the package is protected
    // From https://crate.io/docs/reference/sql/reference/lexical_structure.html#key-words-and-identifiers
    protected final static Set<String> MYSQL_RESERVED_WORDS = ImmutableSet.of(
        "ABS","DEREF","MEMBER","SECOND","ABSOLUTE","DESC","MERGE","SECTION","ACTION","DESCRIBE","METHOD","SELECT","ADD",
        "DESCRIPTOR","MIN","SENSITIVE","AFTER","DETERMINISTIC","MINUTE","SESSION","ALL","DIAGNOSTICS","MOD","SESSION_USER",
        "STATE","STRING","DIRECTORY","SHORT","ALLOCATE","DISCONNECT","MODIFIES","SET","ALTER","DISTINCT","MODULE","SETS","AND",
        "DO","MONTH","SIGNAL","ANY","DOMAIN","MULTISET","SIMILAR","ARE","DOUBLE","NAMES","SIZE","ARRAY","DROP","NATIONAL",
        "SMALLINT","ARRAY_AGG","DYNAMIC","NATURAL","SOME","ARRAY_MAX_CARDINALITY","EACH","NCHAR","SPACE","AS","ELEMENT","NCLOB",
        "SPECIFIC","ASC","ELSE","NEW","SPECIFICTYPE","ASENSITIVE","ELSEIF","NEXT","SQL","ASSERTION","END","NO","SQLCODE",
        "ASYMMETRIC","END_FRAME","NONE","SQLERROR","AT","END_PARTITION","NORMALIZE","SQLEXCEPTION","ATOMIC","END_EXEC","NOT",
        "SQLSTATE","AUTHORIZATION","EQUALS","NTH_VALUE","SQLWARNING","AVG","ESCAPE","NTILE","SQRT","BEFORE","EVERY","NULL",
        "START","BEGIN","EXCEPT","NULLIF","NULLS","BEGIN_FRAME","EXCEPTION","NUMERIC","STATIC","BEGIN_PARTITION","EXEC","OBJECT",
        "STDDEV_POP","BETWEEN","EXECUTE","OCTET_LENGTH","STDDEV_SAMP","BIGINT","EXISTS","OF","SUBMULTISET","BINARY","EXIT",
        "OFFSET","SUBSTRING","BIT","EXTERNAL","OLD","SUBSTRING_REGEX","BIT_LENGTH","EXTRACT","ON","SUCCEEDSBLOB","FALSE","ONLY",
        "SUM","UNBOUNDED","BOOLEAN","FETCH","OPEN","SYMMETRIC","BOTH","FILTER","OPTION","SYSTEM","BREADTH","FIRST","OR",
        "SYSTEM_TIME","BY","FIRST_VALUE","ORDER","SYSTEM_USER","CALL","FLOAT","ORDINALITY","TABLE","CALLED","FOR","OUT",
        "TABLESAMPLE","CARDINALITY","FOREIGN","OUTER","TEMPORARY","CASCADE","FOUND","OUTPUT","THEN","CASCADED","FRAME_ROW",
        "OVER","TIME","CASE","FREE","OVERLAPS","TIMESTAMP","CAST","FROM","OVERLAY","TIMEZONE_HOUR","CATALOG","FULL","PAD",
        "TIMEZONE_MINUTE","CEIL","FUNCTION","PARAMETER","TO","CEILING","FUSION","PARTIAL","TRAILING","YEAR","PARTITION",
        "TRY_CAST","TRANSLATE","CHAR","GENERAL","PERSISTENT","TRANSACTION","CHAR_LENGTH","GET","PATH","TRANSIENT","CHARACTER",
        "GLOBAL","PERCENT","TRANSLATE_REGEX","CHARACTER_LENGTH","GO","PERCENT_RANK","TRANSLATION","CHECK","GOTO",
        "PERCENTILE_CONT","TREAT","CLOB","GRANT","PERCENTILE_DISC","TRIGGER","CLOSE","GROUP","PERIOD","TRIM","COALESCE",
        "GROUPING","PORTION","TRIM_ARRAY","COLLATE","GROUPS","POSITION","TRUE","COLLATION","HANDLER","POSITION_REGEX","TRUNCATE",
        "COLLECT","HAVING","POWER","UESCAPE","COLUMN","HOLD","PRECEDES","UNDER","COMMIT","HOUR","PRECISION","UNDO","CONDITION",
        "IDENTITY","PREPARE","UNION","CONNECT","IF","PRESERVE","UNIQUE","CONNECTION","IMMEDIATE","PRIMARY","UNKNOWN",
        "CONSTRAINT","IN","PRIOR","UNNEST","CONSTRAINTS","INDICATOR","PRIVILEGES","UNTIL","CONSTRUCTOR","INITIALLY","PROCEDURE",
        "UPDATE","CONTAINS","INNER","PUBLIC","UPPER","CONTINUE","INOUT","RANGE","USAGE","CONVERT","INPUT","RANK","USER","CORR",
        "INSENSITIVE","READ","USING","CORRESPONDING","INSERT","READS","VALUE","COUNT","INT","REAL","VALUES","COVAR_POP",
        "INTEGER","RECURSIVE","VALUE_OF","COVAR_SAMP","INTERSECT","REF","VAR_POP","CREATE","INTERSECTION","REFERENCES","VAR_SAMP",
        "CROSS","INTERVAL","REFERENCING","VARBINARY","CUBE","INTO","REGR_AVGX","VARCHAR","CUME_DIST","IS","REGR_AVGY","VARYING",
        "CURRENT","ISOLATION","REGR_COUNT","VERSIONING","CURRENT_CATALOG","ITERATE","REGR_INTERCEPT","VIEW","CURRENT_DATE",
        "JOIN","REGR_R2","WHEN","STRATIFY","KEY","REGR_SLOPE","WHENEVER","CURRENT_PATH","LANGUAGE","REGR_SXX","WHERE",
        "CURRENT_ROLE","LARGE","REGR_SXYREGR_SYY","WHILE","CURRENT_ROW","LAST","RELATIVE","WIDTH_BUCKET","CURRENT_SCHEMA",
        "LAST_VALUE","RELEASE","WINDOW","CURRENT_TIME","LATERAL","REPEAT","WITH","CURRENT_TIMESTAMP","LEAD","RESIGNAL","WITHIN",
        "ZONE","LEADING","RESTRICT","WITHOUT","CURRENT_USER","LEAVE","RESULT","WORK","CURSOR","LEFT","RETURN","WRITE","CYCLE",
        "LEVEL","RETURNS","BYTE","DATA","LIKE","REVOKE","RESET","DATE","LIKE_REGEX","RIGHT","INDEX","DAY","LIMIT","ROLE","IP",
        "DEALLOCATE","LN","ROLLBACK","SCROLL","DEC","LOCAL","ROLLUP","LONG","DECIMAL","LOCALTIME","ROUTINE","STRATIFY","DECLARE",
        "LOCALTIMESTAMP","ROW","SEARCH","DEFAULT","LOCATOR","ROW_NUMBER","MAX","DEFERRABLE","LOOP","ROWS","DEPTH","DEFERRED",
        "LOWER","SAVEPOINT","DELETE","MAP","SCHEMA","DENSE_RANK","MATCH","SCOPE"
    );
}
