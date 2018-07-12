-- Table site ( 1 = intranet; 2 = extranet)

CREATE TABLE ogcstatistics.ogc_services_sites
(
  siteid integer,
  sitename text
);


-- Création de tables jour/semaine/mois/année qui récupèrent les logs georchestra
-- date du jour, organisme (RM, VdR, commune), nom d'utilisateur, service (WFS, WMS, WMTS), requête (get.., describe.., ...)
-- couche utilisée, nombre d'utilisation (d/w/m/y), attributs de dates


-- Table jour

CREATE TABLE ogcstatistics.ogc_services_stats_daily
(
  siteid integer,
  date date,
  org character varying(255),
  user_name character varying(255),
  service character varying(5),
  request character varying(20),
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
  ON ogcstatistics.ogc_services_stats_daily USING btree (date);

CREATE INDEX ogc_services_stats_daily_user_name_idx
  ON ogcstatistics.ogc_services_stats_daily USING btree (user_name);

CREATE INDEX ogc_services_stats_daily_service_idx
  ON ogcstatistics.ogc_services_stats_daily USING btree (service);

CREATE INDEX ogc_services_stats_daily_request_idx
  ON ogcstatistics.ogc_services_stats_daily USING btree (request);

CREATE INDEX ogc_services_stats_daily_org_idx
  ON ogcstatistics.ogc_services_stats_daily USING btree (org);

CREATE INDEX ogc_services_stats_daily_layer_idx
  ON ogcstatistics.ogc_services_stats_daily USING btree (layer);


--Table semaine

CREATE TABLE ogcstatistics.ogc_services_stats_weekly
(
  siteid integer,
  org character varying(255),
  user_name character varying(255),
  service character varying(5),
  request character varying(20),
  layer character varying(255),
  count bigint,
  week integer,
  month integer,
  year integer,
  weekyear character varying(10),
  monthyear character varying(10),
  CONSTRAINT ogc_services_stats_weekly_pk PRIMARY KEY (siteid, weekyear, org, user_name, service, request, layer)
);


CREATE INDEX ogc_services_stats_weekly_user_name_idx
  ON ogcstatistics.ogc_services_stats_weekly USING btree (user_name);

CREATE INDEX ogc_services_stats_weekly_service_idx
  ON ogcstatistics.ogc_services_stats_weekly USING btree (service);

CREATE INDEX ogc_services_stats_weekly_request_idx
  ON ogcstatistics.ogc_services_stats_weekly USING btree (request);

CREATE INDEX ogc_services_stats_weekly_org_idx
  ON ogcstatistics.ogc_services_stats_weekly USING btree (org);

CREATE INDEX ogc_services_stats_weekly_layer_idx
  ON ogcstatistics.ogc_services_stats_weekly USING btree (layer);


--Table mois


CREATE TABLE ogcstatistics.ogc_services_stats_monthly
(
  siteid integer,
  org character varying(255),
  user_name character varying(255),
  service character varying(5),
  request character varying(20),
  layer character varying(255),
  count bigint,
  month integer,
  year integer,
  monthyear character varying(10),
  CONSTRAINT ogc_services_stats_monthly_pk PRIMARY KEY (siteid, monthyear, org, user_name, service, request, layer)
);


CREATE INDEX ogc_services_stats_monthly_user_name_idx
  ON ogcstatistics.ogc_services_stats_monthly USING btree (user_name);

CREATE INDEX ogc_services_stats_monthly_service_idx
  ON ogcstatistics.ogc_services_stats_monthly USING btree (service);

CREATE INDEX ogc_services_stats_monthly_request_idx
  ON ogcstatistics.ogc_services_stats_monthly USING btree (request);

CREATE INDEX ogc_services_stats_monthly_org_idx
  ON ogcstatistics.ogc_services_stats_monthly USING btree (org);

CREATE INDEX ogc_services_stats_monthly_layer_idx
  ON ogcstatistics.ogc_services_stats_monthly USING btree (layer);


-- Table année


CREATE TABLE ogcstatistics.ogc_services_stats_yearly
(
  siteid integer,
  org character varying(255),
  user_name character varying(255),
  service character varying(5),
  request character varying(20),
  layer character varying(255),
  count bigint,
  year integer
);


CREATE INDEX ogc_services_stats_yearly_user_name_idx
  ON ogcstatistics.ogc_services_stats_yearly USING btree (user_name);

CREATE INDEX ogc_services_stats_yearly_service_idx
  ON ogcstatistics.ogc_services_stats_yearly USING btree (service);

CREATE INDEX ogc_services_stats_yearly_request_idx
  ON ogcstatistics.ogc_services_stats_yearly USING btree (request);

CREATE INDEX ogc_services_stats_yearly_org_idx
  ON ogcstatistics.ogc_services_stats_yearly USING btree (org);

CREATE INDEX ogc_services_stats_yearly_layer_idx
  ON ogcstatistics.ogc_services_stats_yearly USING btree (layer);