CREATE TYPE public.abtestgroup_enum AS ENUM (
'Control',
'Variant_A',
'Variant_B'
);


CREATE TYPE public.apiversion_enum AS ENUM (
'v3',
'v2',
'v1'
);


CREATE TYPE public.articlecategory_enum AS ENUM (
'Entertainment',
'Business',
'Sports',
'News',
'Technology'
);


CREATE TYPE public.articledifficultylevel_enum AS ENUM (
'Basic',
'Intermediate',
'Advanced'
);


CREATE TYPE public.articlesubcategory_enum AS ENUM (
'International',
'Opinion',
'Local',
'Feature'
);


CREATE TYPE public.bouncestatus_enum AS ENUM (
'Yes',
'No'
);


CREATE TYPE public.browsertype_enum AS ENUM (
'Safari',
'Edge',
'Chrome',
'Firefox'
);


CREATE TYPE public.cachestatus_enum AS ENUM (
'Hit',
'Expired',
'Miss'
);


CREATE TYPE public.clickcontext_enum AS ENUM (
'Headline',
'Summary',
'Image',
'Author'
);


CREATE TYPE public.clickposition_enum AS ENUM (
'1',
'2',
'3',
'4',
'5',
'6',
'7',
'8',
'9',
'10'
);


CREATE TYPE public.clicksource_enum AS ENUM (
'Article',
'Homepage',
'Search',
'External'
);


CREATE TYPE public.clicktype_enum AS ENUM (
'Related',
'Trending',
'Direct',
'Recommended'
);


CREATE TYPE public.connectiontype_enum AS ENUM (
'Cable',
'5G',
'4G',
'WiFi'
);


CREATE TYPE public.contentformat_enum AS ENUM (
'Mobile',
'Text',
'HTML',
'AMP'
);


CREATE TYPE public.contenttype_enum AS ENUM (
'Article',
'Gallery',
'Video',
'Interactive'
);


CREATE TYPE public.conversionstatus_enum AS ENUM (
'Share',
'Newsletter',
'Subscription'
);


CREATE TYPE public.devicetype_enum AS ENUM (
'iOS',
'Windows',
'MacOS',
'Android'
);


CREATE TYPE public.errorcount_enum AS ENUM (
'0',
'1',
'2',
'3',
'4',
'5'
);


CREATE TYPE public.eventtype_enum AS ENUM (
'bookmark',
'click',
'scroll',
'share',
'view'
);


CREATE TYPE public.exittype_enum AS ENUM (
'Timeout',
'Natural',
'Bounce',
'External'
);


CREATE TYPE public.feedbackcategory_enum AS ENUM (
'Relevance',
'Content',
'Format'
);


CREATE TYPE public.feedbackscore_enum AS ENUM (
'1.0',
'2.0',
'3.0',
'4.0',
'5.0'
);


CREATE TYPE public.interactiontype_enum AS ENUM (
'Scroll',
'Share',
'Click',
'Hover'
);


CREATE TYPE public.languagecode_enum AS ENUM (
'fr',
'es',
'en',
'de',
'zh'
);


CREATE TYPE public.nextaction_enum AS ENUM (
'Exit',
'Another Article',
'Share',
'Search'
);


CREATE TYPE public.paywallstatus_enum AS ENUM (
'Metered',
'Premium',
'Free'
);


CREATE TYPE public.personalizationversion_enum AS ENUM (
'v4',
'v1',
'v3',
'v2',
'v5'
);


CREATE TYPE public.recalgorithm_enum AS ENUM (
'Contextual',
'Content-based',
'Collaborative',
'Hybrid'
);


CREATE TYPE public.recpage_enum AS ENUM (
'Search',
'Category',
'Home',
'Article'
);


CREATE TYPE public.recsection_enum AS ENUM (
'Bottom',
'Sidebar',
'Related',
'Top'
);


CREATE TYPE public.recstrategy_enum AS ENUM (
'Trending',
'Editorial',
'Similar',
'Personalized'
);


CREATE TYPE public.subscriptionstatus_enum AS ENUM (
'Premium',
'Enterprise',
'Basic'
);


CREATE TYPE public.timezoneoffset_enum AS ENUM (
'8',
'1',
'0',
'-5',
'-8'
);


CREATE TYPE public.useractivitylevel_enum AS ENUM (
'Low',
'High',
'Medium'
);


CREATE TYPE public.useragent_enum AS ENUM (
'Desktop',
'App',
'Mobile',
'Tablet'
);


CREATE TYPE public.userfeedback_enum AS ENUM (
'Like',
'Dislike'
);


CREATE TYPE public.usergender_enum AS ENUM (
'M',
'F',
'Other'
);


CREATE TYPE public.useroccupation_enum AS ENUM (
'Retired',
'Professional',
'Other',
'Student'
);


CREATE TYPE public.usersegment_enum AS ENUM (
'New',
'Dormant',
'Active',
'Regular'
);


CREATE TYPE public.usertype_enum AS ENUM (
'Trial',
'Premium',
'Free'
);


CREATE TABLE public.articles (
artkey bigint NOT NULL,
catlabel public.articlecategory_enum,
subcatlbl public.articlesubcategory_enum,
pubtime timestamp without time zone,
authname character varying(200),
srcref character varying(150),
wordlen integer,
readsec integer,
difflevel public.articledifficultylevel_enum,
freshscore numeric(5,2),
qualscore numeric(5,2),
sentscore numeric(5,2),
contrscore numeric(5,2),
tagset text,
conttype public.contenttype_enum,
contformat public.contentformat_enum,
accscore numeric(5,2),
mediacount integer,
vidsec integer,
paywall public.paywallstatus_enum,
authref bigint,
engagement_metrics jsonb
);


CREATE SEQUENCE public.articles_artkey_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.articles_artkey_seq OWNED BY public.articles.artkey;


CREATE TABLE public.devices (
devkey bigint NOT NULL,
devtype public.devicetype_enum,
brwtype public.browsertype_enum,
osver character varying(40),
appver character varying(20),
scrres character varying(15),
vpsize character varying(15),
conntype public.connectiontype_enum,
netspd numeric(6,2),
uselink bigint
);


CREATE SEQUENCE public.devices_devkey_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.devices_devkey_seq OWNED BY public.devices.devkey;


CREATE TABLE public.interactionmetrics (
intmetkey bigint NOT NULL,
interaction_behavior jsonb
);


CREATE TABLE public.interactions (
intkey bigint NOT NULL,
seshlink2 bigint,
reclink bigint,
artval bigint,
intts timestamp without time zone,
evttype public.eventtype_enum,
seqval integer,
agentval public.useragent_enum,
clkts timestamp without time zone,
clkpos public.clickposition_enum,
clktype public.clicktype_enum,
clksrc public.clicksource_enum,
clkctx public.clickcontext_enum,
inttype public.interactiontype_enum
);


CREATE SEQUENCE public.interactions_intkey_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.interactions_intkey_seq OWNED BY public.interactions.intkey;


CREATE TABLE public.recommendations (
reckey bigint NOT NULL,
alglabel public.recalgorithm_enum,
stratlabel public.recstrategy_enum,
posval integer,
recpage public.recpage_enum,
recsec public.recsection_enum,
recscore numeric(5,2),
confval numeric(5,2),
divval numeric(5,2),
novval numeric(5,2),
seryval numeric(5,2),
artlink bigint
);


CREATE SEQUENCE public.recommendations_reckey_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.recommendations_reckey_seq OWNED BY public.recommendations.reckey;


CREATE TABLE public.sessions (
seshkey bigint NOT NULL,
userel bigint,
devrel bigint,
seshstart timestamp without time zone,
seshdur integer,
seshviews integer,
bncrate numeric(5,2),
seshdepth integer,
engscore numeric(5,2),
seshrecs integer,
seshclicks integer,
ctrval numeric(5,2),
langcode public.languagecode_enum,
tzoffset public.timezoneoffset_enum,
ipaddr inet,
geoctry character varying(100),
georeg character varying(100),
geocity character varying(100),
expref character varying(30),
persver public.personalizationversion_enum,
recset character varying(50),
relscore numeric(5,2),
persacc numeric(5,2),
recutil numeric(5,2)
);


CREATE SEQUENCE public.sessions_seshkey_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.sessions_seshkey_seq OWNED BY public.sessions.seshkey;


CREATE TABLE public.systemperformance (
rectrace bigint NOT NULL,
perfts timestamp without time zone,
resptime integer,
loadscore numeric(5,2),
errcount public.errorcount_enum,
warncount integer,
perfscore numeric(5,2),
cachestate public.cachestatus_enum,
apiver public.apiversion_enum,
cliver character varying(50),
featset text,
devlink bigint,
seshlink bigint
);


CREATE SEQUENCE public.systemperformance_rectrace_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.systemperformance_rectrace_seq OWNED BY public.systemperformance.rectrace;


CREATE TABLE public.users (
userkey bigint NOT NULL,
regmoment date,
typelabel public.usertype_enum,
seglabel public.usersegment_enum,
substatus public.subscriptionstatus_enum,
subdays integer,
ageval integer,
gendlbl public.usergender_enum,
occulbl public.useroccupation_enum,
testgrp public.abtestgroup_enum,
user_preferences jsonb
);


CREATE SEQUENCE public.users_userkey_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.users_userkey_seq OWNED BY public.users.userkey;


ALTER TABLE ONLY public.articles ALTER COLUMN artkey SET DEFAULT nextval('public.articles_artkey_seq'::regclass);


ALTER TABLE ONLY public.devices ALTER COLUMN devkey SET DEFAULT nextval('public.devices_devkey_seq'::regclass);


ALTER TABLE ONLY public.interactions ALTER COLUMN intkey SET DEFAULT nextval('public.interactions_intkey_seq'::regclass);


ALTER TABLE ONLY public.recommendations ALTER COLUMN reckey SET DEFAULT nextval('public.recommendations_reckey_seq'::regclass);


ALTER TABLE ONLY public.sessions ALTER COLUMN seshkey SET DEFAULT nextval('public.sessions_seshkey_seq'::regclass);


ALTER TABLE ONLY public.systemperformance ALTER COLUMN rectrace SET DEFAULT nextval('public.systemperformance_rectrace_seq'::regclass);


ALTER TABLE ONLY public.users ALTER COLUMN userkey SET DEFAULT nextval('public.users_userkey_seq'::regclass);
