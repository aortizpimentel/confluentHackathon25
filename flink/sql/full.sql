CREATE VIEW er_patients_enriched AS
SELECT
    userid    AS patientId,
    gender    AS gender,
    regionid  AS riskGroup,
    registertime
FROM `er.patients`;

CREATE OR REPLACE VIEW er_admission AS
SELECT
    episodeId,
    patientId,
    arrivalMode,
    arrivalTs
FROM `er.admission.requests`;

CREATE OR REPLACE VIEW er_triage AS
SELECT
    episodeId,
    triageLevel,
    reason,
    triageTs
FROM `er.triage.completed`;

CREATE OR REPLACE VIEW er_patients_enriched AS
SELECT
    userid     AS patientId,
    gender     AS gender,
    regionid   AS riskGroup,
    registertime
FROM `er.patients`;

CREATE TABLE `er.violations.enriched` (
                                          episodeId   STRING,
                                          patientId   STRING,
                                          triageLevel STRING,
                                          waitMinutes DOUBLE,
                                          slaMinutes  INT,
                                          severity    STRING,
                                          gender      STRING,
                                          riskGroup   STRING,
                                          violationTs TIMESTAMP(3) WITH LOCAL TIME ZONE
)
    WITH (
        'connector'      = 'confluent',
        'changelog.mode' = 'append',
        'value.format'   = 'avro-registry'
        );

CREATE TABLE `er.load.stats` (
                                 windowStart      TIMESTAMP(3) WITH LOCAL TIME ZONE,
                                 windowEnd        TIMESTAMP(3) WITH LOCAL TIME ZONE,
                                 triageLevel      STRING,
                                 waitingForTriage INT,
                                 waitingForDoctor INT
)
    WITH (
        'connector'      = 'confluent',
        'changelog.mode' = 'append',
        'value.format'   = 'avro-registry'
        );

CREATE TABLE `er.violations.enriched` (
                                          episodeId   STRING,
                                          triageLevel STRING,
                                          patientId   STRING,
                                          waitMinutes DOUBLE,
                                          slaMinutes  INT,
                                          severity    STRING,
                                          gender      STRING,
                                          riskGroup   STRING,
                                          violationTs TIMESTAMP(3) WITH LOCAL TIME ZONE,
                                          PRIMARY KEY (episodeId, triageLevel) NOT ENFORCED
)
    WITH (
        'connector'      = 'confluent',
        'changelog.mode' = 'upsert',
        'value.format'   = 'avro-registry'
        );

INSERT INTO `er.violations.enriched` (
    episodeId,
    triageLevel,
    patientId,
    waitMinutes,
    slaMinutes,
    severity,
    gender,
    riskGroup,
    violationTs
)
SELECT
    a.episodeId                                              AS episodeId,
    t.triageLevel                                            AS triageLevel,
    a.patientId                                              AS patientId,
    TIMESTAMPDIFF(MINUTE, a.arrivalTs, t.triageTs)           AS waitMinutes,
    CASE t.triageLevel
        WHEN 'I'  THEN 5
        WHEN 'II' THEN 10
        WHEN 'III' THEN 30
        ELSE 60
        END                                                      AS slaMinutes,
    'SLA_BREACH'                                             AS severity,
    p.gender                                                 AS gender,
    p.riskGroup                                              AS riskGroup,
    t.triageTs                                               AS violationTs
FROM er_admission a
         JOIN er_triage t
              ON a.episodeId = t.episodeId
         LEFT JOIN er_patients_enriched p
                   ON a.patientId = p.patientId
WHERE TIMESTAMPDIFF(MINUTE, a.arrivalTs, t.triageTs) >
      CASE t.triageLevel
          WHEN 'I'  THEN 5
          WHEN 'II' THEN 10
          WHEN 'III' THEN 30
          ELSE 60
          END;

CREATE TABLE `er.load.stats` (
                                 triageLevel      STRING,
                                 waitingForTriage INT,
                                 waitingForDoctor INT,
                                 PRIMARY KEY (triageLevel) NOT ENFORCED
)
    WITH (
        'connector'      = 'confluent',
        'changelog.mode' = 'upsert',
        'value.format'   = 'avro-registry'
        );

INSERT INTO `er.load.stats`
SELECT
    triageLevel,
    0                            AS waitingForTriage,
    CAST(COUNT(*) AS INT)        AS waitingForDoctor
FROM `er.triage.completed`
GROUP BY triageLevel;