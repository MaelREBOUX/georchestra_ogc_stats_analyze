﻿
-- CREATE DATABASE sysig WITH OWNER = postgres ;

-- CREATE SCHEMA statistiques AUTHORIZATION postgres;
-- CREATE EXTENSION dblink SCHEMA public ;
-- GRANT ALL ON SCHEMA statistiques TO geocarto;


-- Table site ( 1 = intranet; 2 = extranet)

DROP TABLE IF EXISTS statistiques.ogc_services_sites ;
CREATE TABLE statistiques.ogc_services_sites
(
  siteid integer,
  sitename text,
  CONSTRAINT ogc_services_sites_pk PRIMARY KEY (siteid)
);


-- table des login des comptes utilisateurs

DROP TABLE IF EXISTS statistiques.ogc_services_users ;
CREATE TABLE statistiques.ogc_services_users (
	siteid int4 NOT NULL,
	org text NULL,
	user_name text NULL,
	first_name text NULL,
	last_name text NULL,
	mail text NULL
);


-- Création de tables jour/semaine/mois/année qui récupèrent les logs georchestra
-- date du jour, organisme (RM, VdR, commune), nom d'utilisateur, service (WFS, WMS, WMTS), requête (get.., describe.., ...)
-- couche utilisée, nombre d'utilisation (d/w/m/y), attributs de dates


-- Table jour

DROP TABLE IF EXISTS statistiques.ogc_services_stats_daily ;
CREATE TABLE statistiques.ogc_services_stats_daily
(
  siteid integer,
  date date,
  org character varying(255),
  user_name character varying(255),
  service character varying(5),
  request character varying(100),
  layer character varying(255),
  count bigint,
  week integer,
  month integer,
  year integer,
  weekyear character varying(10),
  monthyear character varying(10),
  CONSTRAINT ogc_services_stats_daily_pk PRIMARY KEY (siteid, date, org, user_name, service, request, layer)
);


CREATE INDEX ogc_services_stats_daily_date_idx
  ON statistiques.ogc_services_stats_daily USING btree (date);

CREATE INDEX ogc_services_stats_daily_user_name_idx
  ON statistiques.ogc_services_stats_daily USING btree (user_name);

CREATE INDEX ogc_services_stats_daily_service_idx
  ON statistiques.ogc_services_stats_daily USING btree (service);

CREATE INDEX ogc_services_stats_daily_request_idx
  ON statistiques.ogc_services_stats_daily USING btree (request);

CREATE INDEX ogc_services_stats_daily_org_idx
  ON statistiques.ogc_services_stats_daily USING btree (org);

CREATE INDEX ogc_services_stats_daily_layer_idx
  ON statistiques.ogc_services_stats_daily USING btree (layer);


--Table semaine

DROP TABLE IF EXISTS statistiques.ogc_services_stats_weekly ;
CREATE TABLE statistiques.ogc_services_stats_weekly
(
  siteid integer,
  org character varying(255),
  user_name character varying(255),
  service character varying(5),
  request character varying(100),
  layer character varying(255),
  count bigint,
  week integer,
  year integer,
  weekyear character varying(10),
  CONSTRAINT ogc_services_stats_weekly_pk PRIMARY KEY (siteid, weekyear, org, user_name, service, request, layer)
);


CREATE INDEX ogc_services_stats_weekly_user_name_idx
  ON statistiques.ogc_services_stats_weekly USING btree (user_name);

CREATE INDEX ogc_services_stats_weekly_service_idx
  ON statistiques.ogc_services_stats_weekly USING btree (service);

CREATE INDEX ogc_services_stats_weekly_request_idx
  ON statistiques.ogc_services_stats_weekly USING btree (request);

CREATE INDEX ogc_services_stats_weekly_org_idx
  ON statistiques.ogc_services_stats_weekly USING btree (org);

CREATE INDEX ogc_services_stats_weekly_layer_idx
  ON statistiques.ogc_services_stats_weekly USING btree (layer);


--Table mois

DROP TABLE IF EXISTS statistiques.ogc_services_stats_monthly ;
CREATE TABLE statistiques.ogc_services_stats_monthly
(
  siteid integer,
  org character varying(255),
  user_name character varying(255),
  service character varying(5),
  request character varying(100),
  layer character varying(255),
  count bigint,
  month integer,
  year integer,
  monthyear character varying(10),
  CONSTRAINT ogc_services_stats_monthly_pk PRIMARY KEY (siteid, monthyear, org, user_name, service, request, layer)
);


CREATE INDEX ogc_services_stats_monthly_user_name_idx
  ON statistiques.ogc_services_stats_monthly USING btree (user_name);

CREATE INDEX ogc_services_stats_monthly_service_idx
  ON statistiques.ogc_services_stats_monthly USING btree (service);

CREATE INDEX ogc_services_stats_monthly_request_idx
  ON statistiques.ogc_services_stats_monthly USING btree (request);

CREATE INDEX ogc_services_stats_monthly_org_idx
  ON statistiques.ogc_services_stats_monthly USING btree (org);

CREATE INDEX ogc_services_stats_monthly_layer_idx
  ON statistiques.ogc_services_stats_monthly USING btree (layer);


-- Table année

DROP TABLE IF EXISTS statistiques.ogc_services_stats_yearly ;
CREATE TABLE statistiques.ogc_services_stats_yearly
(
  siteid integer,
  org character varying(255),
  user_name character varying(255),
  service character varying(5),
  request character varying(100),
  layer character varying(255),
  count bigint,
  year integer
);


CREATE INDEX ogc_services_stats_yearly_user_name_idx
  ON statistiques.ogc_services_stats_yearly USING btree (user_name);

CREATE INDEX ogc_services_stats_yearly_service_idx
  ON statistiques.ogc_services_stats_yearly USING btree (service);

CREATE INDEX ogc_services_stats_yearly_request_idx
  ON statistiques.ogc_services_stats_yearly USING btree (request);

CREATE INDEX ogc_services_stats_yearly_org_idx
  ON statistiques.ogc_services_stats_yearly USING btree (org);

CREATE INDEX ogc_services_stats_yearly_layer_idx
  ON statistiques.ogc_services_stats_yearly USING btree (layer);

  
-- Table live

DROP TABLE IF EXISTS statistiques.ogc_services_stats_live ;
CREATE TABLE statistiques.ogc_services_stats_live
(
  siteid integer,
  org character varying(255),
  user_name character varying(255),
  service character varying(5),
  hits bigint,
  layers_nb bigint,
  first_hit character varying(8),
  last_hit character varying(8),
  CONSTRAINT ogc_services_stats_live_pk PRIMARY KEY (siteid, org, user_name, service)
);
  
  