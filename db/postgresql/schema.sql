---------------------------------------------------------------
-- Copyright (C) 2024 Frode Randers
-- All rights reserved
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
---------------------------------------------------------------

---------------------------------------------------------------
-- Database schema: PostgreSQL
--
-- Remember to grant privileges to the specific database user
---------------------------------------------------------------

---------------------------------------------------------------
-- Identifies tenants for information units
--
CREATE TABLE repo_tenant
(
    tenantid    INTEGER        NOT NULL, -- id of tenant

    name        VARCHAR(255)   NOT NULL, -- name of tenant
    description VARCHAR(1024),           -- description of type
    created     TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT repo_tenant_pk
        PRIMARY KEY (tenantid),
    CONSTRAINT repo_tenant_name_unique
        UNIQUE (name)
)
;


---------------------------------------------------------------
-- Unit entities
--
CREATE TABLE repo_unit
(
    tenantid INTEGER   NOT NULL,                      -- id of tenant
    unitid   BIGINT GENERATED BY DEFAULT AS IDENTITY, -- id of unit

    corrid   CHAR(36)  NOT NULL,
    name     VARCHAR(255) NOT NULL,                   -- name of unit
    status   INTEGER   NOT NULL DEFAULT 30,           -- See Unit.Status
    created  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT repo_unit_pk
        PRIMARY KEY (tenantid, unitid),
    CONSTRAINT repo_uk_corrid_unique
        UNIQUE (corrid),
    CONSTRAINT repo_unit_tenant_exists
        FOREIGN KEY (tenantid) REFERENCES repo_tenant (tenantid)
)
;

CREATE INDEX repo_ut_ind1 ON repo_unit
(
     tenantid, status, created, unitid
)
;


---------------------------------------------------------------
-- Collection of known attributes
--
CREATE TABLE repo_namespace
(
    alias     VARCHAR(20)  NOT NULL, -- alias of namespace
    namespace VARCHAR(255) NOT NULL, -- namespace of attribute

    CONSTRAINT repo_namespace_pk
        PRIMARY KEY (alias, namespace)
)
;

CREATE TABLE repo_attribute
(
    attrid      INTEGER      NOT NULL,           -- id of attribute (serial)

    qualname    VARCHAR(255) NOT NULL,           -- qualified name of attribute
    attrname    VARCHAR(255) NOT NULL,           -- name of attribute
    attrtype    INTEGER      NOT NULL,           -- defined in org.gautelis.repo.model.attributes.Type
    scalar      BOOLEAN      NOT NULL DEFAULT FALSE,
    created     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT repo_attributes_pk
        PRIMARY KEY (attrid),
    CONSTRAINT repo_attr_qualname_unique
        UNIQUE (qualname),
    CONSTRAINT repo_attr_name_unique
        UNIQUE (attrname)
)
;

CREATE TABLE repo_attribute_description
(
    attrid  INTEGER NOT NULL,               -- id of attribute (serial)
    lang    CHAR(2) NOT NULL DEFAULT 'SE',

    alias   VARCHAR(255) NOT NULL,          -- translated name of attribute
    description VARCHAR(1024),              -- description of attribute

    CONSTRAINT repo_attribute_description_pk
        PRIMARY KEY (attrid, lang),
    CONSTRAINT repo_attr_desc_attr_ex
        FOREIGN KEY (attrid) REFERENCES repo_attribute (attrid)
)
;

---------------------------------------------------------------------------
-- Values of attributes
--
CREATE TABLE repo_attribute_value
(
    tenantid INTEGER NOT NULL,                        -- id of tenant
    unitid   BIGINT  NOT NULL,                        -- id of unit
    attrid   INTEGER NOT NULL,                        -- id of attribute

    valueid  BIGINT GENERATED BY DEFAULT AS IDENTITY, -- id of value vector

    CONSTRAINT repo_attribute_value_pk
        PRIMARY KEY (tenantid, unitid, attrid),
    CONSTRAINT repo_attribute_value_id_unique
        UNIQUE (valueid),
    CONSTRAINT repo_attribute_value_attr_ex
        FOREIGN KEY (attrid) REFERENCES repo_attribute (attrid),
    CONSTRAINT repo_attribute_value_unit_ex
        FOREIGN KEY (tenantid, unitid) REFERENCES repo_unit (tenantid, unitid) ON DELETE CASCADE
)
;

CREATE INDEX repo_av_ind1 ON repo_attribute_value
(
     attrid ASC
)
;

CREATE INDEX repo_av_ind2 ON repo_attribute_value
(
     valueid ASC
)
;

CREATE INDEX repo_av_ind3 ON repo_attribute_value
(
     tenantid ASC, unitid ASC
)
;

CREATE INDEX repo_av_ind4 ON repo_attribute_value
(
     tenantid ASC, unitid ASC, attrid ASC, valueid ASC
)
;

---------------------------------------------------------------
-- Attribute value vectors
--
-- String values
--
CREATE TABLE repo_string_vector
(
    valueid  BIGINT  NOT NULL,           -- id of attribute value (serial)
    idx      INTEGER NOT NULL DEFAULT 0, -- index of value

    val      VARCHAR(255),

    CONSTRAINT repo_string_vector_pk
        PRIMARY KEY (valueid, idx),
    CONSTRAINT repo_string_v_value_ex
        FOREIGN KEY (valueid) REFERENCES repo_attribute_value(valueid) ON DELETE CASCADE
)
;

CREATE INDEX repo_sv_ind1 ON repo_string_vector
(
     valueid ASC, LOWER(val) ASC
)
;

-- relevant?
CREATE INDEX repo_sv_ind2 ON repo_string_vector
(
     LOWER(val) ASC
)
;

--
-- Date values
--
CREATE TABLE repo_time_vector
(
    valueid  BIGINT  NOT NULL,           -- id of attribute value
    idx      INTEGER NOT NULL DEFAULT 0, -- index of value

    val      TIMESTAMP,

    CONSTRAINT repo_time_vector_pk
        PRIMARY KEY (valueid, idx),
    CONSTRAINT repo_time_v_value_ex
        FOREIGN KEY (valueid) REFERENCES repo_attribute_value(valueid) ON DELETE CASCADE
)
;

CREATE INDEX repo_tiv_ind1 ON repo_time_vector
(
     valueid ASC, val ASC
)
;

-- relevant?
CREATE INDEX repo_tiv_ind2 ON repo_time_vector
(
     val ASC
)
;

--
-- Integer values
--
CREATE TABLE repo_integer_vector
(
    valueid  BIGINT  NOT NULL,           -- id of attribute value
    idx      INTEGER NOT NULL DEFAULT 0, -- index of value

    val      INTEGER,

    CONSTRAINT repo_integer_vector_pk
        PRIMARY KEY (valueid, idx),
    CONSTRAINT repo_integer_v_value_ex
        FOREIGN KEY (valueid) REFERENCES repo_attribute_value(valueid) ON DELETE CASCADE
)
;

CREATE INDEX repo_iv_ind1 ON repo_integer_vector
(
     valueid ASC, val ASC
)
;

--
-- Long values
--
CREATE TABLE repo_long_vector
(
    valueid  BIGINT  NOT NULL,           -- id of attribute value
    idx      INTEGER NOT NULL DEFAULT 0, -- index of value

    val      BIGINT,

    CONSTRAINT repo_long_vector_pk
        PRIMARY KEY (valueid, idx),
    CONSTRAINT repo_long_v_value_ex
        FOREIGN KEY (valueid) REFERENCES repo_attribute_value(valueid) ON DELETE CASCADE
)
;

CREATE INDEX repo_lv_ind1 ON repo_long_vector
(
     valueid ASC, val ASC
)
;

--
-- Double values
--
CREATE TABLE repo_double_vector
(
    valueid  BIGINT  NOT NULL,           -- id of attribute value
    idx      INTEGER NOT NULL DEFAULT 0, -- index of value

    val      DOUBLE PRECISION,

    CONSTRAINT repo_double_vector_pk
        PRIMARY KEY (valueid, idx),
    CONSTRAINT repo_double_v_value_ex
        FOREIGN KEY (valueid) REFERENCES repo_attribute_value(valueid) ON DELETE CASCADE
)
;

CREATE INDEX repo_dov_ind1 ON repo_double_vector
(
     valueid ASC, val ASC
)
;

--
-- Boolean values
--
CREATE TABLE repo_boolean_vector
(
    valueid  BIGINT  NOT NULL,           -- id of attribute value
    idx      INTEGER NOT NULL DEFAULT 0, -- index of value

    val      INTEGER,

    CONSTRAINT repo_boolean_vector_pk
        PRIMARY KEY (valueid, idx),
    CONSTRAINT repo_boolean_v_value_ex
        FOREIGN KEY (valueid) REFERENCES repo_attribute_value(valueid) ON DELETE CASCADE
)
;

CREATE INDEX repo_bv_ind1 ON repo_boolean_vector
(
     valueid ASC, val ASC
)
;

--
-- Binary values
--
CREATE TABLE repo_data_vector
(
    valueid  BIGINT  NOT NULL,           -- id of attribute value
    idx      INTEGER NOT NULL DEFAULT 0, -- index of value

    val      BYTEA,

    CONSTRAINT repo_data_vector_pk
        PRIMARY KEY (valueid, idx),
    CONSTRAINT repo_data_v_value_ex
        FOREIGN KEY (valueid) REFERENCES repo_attribute_value(valueid) ON DELETE CASCADE
)
;

CREATE INDEX repo_dv_ind1 ON repo_data_vector
(
     valueid ASC
)
;

---------------------------------------------------------------
-- The log
--
CREATE TABLE repo_log
(
    tenantid  INTEGER      NOT NULL,
    unitid    BIGINT       NOT NULL,
    event     INTEGER      NOT NULL, -- defined in org.gautelis.repo.model.ActionEvent
    logentry  VARCHAR(255) NOT NULL,
    logtime   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
)
;

-- When searching for all events for a certain unit
CREATE INDEX repo_log_ind1 ON repo_log
(
    tenantid, unitid
)
;





---------------------------------------------------------------
-- Unit locks
--
-- The 'type' may have one of these values
--   1 - read lock
--   2 - existence lock
--   3 - write lock
--
CREATE TABLE repo_lock
(
    tenantid INTEGER      NOT NULL,
    unitid   BIGINT       NOT NULL,
    lockid   BIGINT GENERATED ALWAYS AS IDENTITY,

    purpose  VARCHAR(255) NOT NULL,
    locktype INTEGER      NOT NULL,
    expire   TIMESTAMP,
    locktime TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT repo_lock_pk
        PRIMARY KEY (tenantid, unitid, lockid),
    CONSTRAINT repo_lock_unit_unique
        UNIQUE (lockid),
    CONSTRAINT repo_lock_unit_exists
        FOREIGN KEY (tenantid, unitid) REFERENCES repo_unit (tenantid, unitid) ON DELETE CASCADE
)
;

CREATE UNIQUE INDEX repo_lock_ind ON repo_lock
(
     lockid
)
;


----------------------------------------------------------------
-- Associations functionality
--
-- Internal associations describe associations between units
-- (of various types) in the archive.
--
-- External associations describe associations between a unit
-- and a resource in any external system where the resource is
-- uniquely identified by a string.
--
CREATE TABLE repo_internal_assoc
(
    tenantid      INTEGER NOT NULL,
    unitid        BIGINT  NOT NULL,
    assoctype     INTEGER NOT NULL,
    assoctenantid INTEGER NOT NULL,
    assocunitid   BIGINT  NOT NULL,

    CONSTRAINT repo_internal_assoc_pk
        PRIMARY KEY (tenantid, unitid, assoctype, assoctenantid, assocunitid),
    CONSTRAINT repo_ia_left_unit_exists
        FOREIGN KEY (tenantid, unitid) REFERENCES repo_unit (tenantid, unitid) ON DELETE CASCADE,
    CONSTRAINT repo_ia_right_unit_exists
        FOREIGN KEY (assoctenantid, assocunitid) REFERENCES repo_unit (tenantid, unitid) ON DELETE CASCADE
)
;

CREATE INDEX repo_iassoc_idx1 ON repo_internal_assoc
(
     assoctype, assoctenantid, assocunitid
)
;

CREATE INDEX repo_iassoc_idx2 ON repo_internal_assoc
(
     tenantid, unitid, assoctype
)
;

CREATE TABLE repo_external_assoc
(
    tenantid    INTEGER      NOT NULL,
    unitid      BIGINT       NOT NULL,
    assoctype   INTEGER      NOT NULL,
    assocstring VARCHAR(255) NOT NULL,
    associd     BIGINT GENERATED BY DEFAULT AS IDENTITY,

    CONSTRAINT repo_external_assoc_pk
        PRIMARY KEY (tenantid, unitid, assoctype, assocstring, associd),
    CONSTRAINT repo_ea_left_unit_exists
        FOREIGN KEY (tenantid, unitid) REFERENCES repo_unit (tenantid, unitid) ON DELETE CASCADE
)
;

CREATE INDEX repo_eassoc_idx1 ON repo_external_assoc
(
     assoctype, assocstring
)
;

CREATE INDEX repo_eassoc_idx2 ON repo_external_assoc
(
     tenantid, unitid, assoctype
)
;
