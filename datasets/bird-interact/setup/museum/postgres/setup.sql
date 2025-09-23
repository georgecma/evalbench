CREATE TYPE public.auditstatus_enum AS ENUM (
'Passed',
'Failed',
'Pending',
'Review'
);


CREATE TYPE public.certificationstatus_enum AS ENUM (
'Expired',
'Current',
'Pending'
);


CREATE TYPE public.compliancestatus_enum AS ENUM (
'Non-compliant',
'Partial',
'Compliant'
);


CREATE TYPE public.conservationstatus_enum AS ENUM (
'Good',
'Excellent',
'Fair',
'Poor',
'Critical'
);


CREATE TYPE public.conservefreq_enum AS ENUM (
'Rare',
'Occasional',
'Frequent'
);


CREATE TYPE public.evacpriority_enum AS ENUM (
'Low',
'Medium',
'High'
);


CREATE TYPE public.materialtype_enum AS ENUM (
'Stone',
'Textile',
'Bronze',
'Wood',
'Paper',
'Glass',
'Ceramic',
'Jade',
'Other'
);


CREATE TYPE public.qualitycontrolstatus_enum AS ENUM (
'Passed',
'Failed',
'Review'
);


CREATE TYPE public.riskassesslevel_enum AS ENUM (
'Low',
'Medium',
'High'
);


CREATE TYPE public.securitylevel_enum AS ENUM (
'Low',
'Medium',
'High'
);


CREATE TABLE public.airqualityreadings (
aqrecordregistry bigint NOT NULL,
envreadref bigint NOT NULL,
co2ppm smallint,
tvocppb integer,
ozoneppb integer,
so2ppb smallint,
no2ppb bigint,
pm25conc real,
pm10conc numeric(5,2),
hchoconc numeric(7,4),
airexrate numeric(4,1),
airvelms numeric(5,2)
);


CREATE SEQUENCE public.airqualityreadings_aqrecordregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.airqualityreadings_aqrecordregistry_seq OWNED BY public.airqualityreadings.aqrecordregistry;


CREATE TABLE public.artifactratings (
ratingrecordregistry bigint NOT NULL,
artref character(10) NOT NULL,
histsignrating smallint,
researchvalrating integer,
exhibitvalrating integer,
cultscore smallint,
publicaccessrating smallint,
eduvaluerating bigint,
conservediff public.evacpriority_enum,
treatcomplexity character(10),
matstability character varying(30),
deteriorrate text
);


CREATE SEQUENCE public.artifactratings_ratingrecordregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.artifactratings_ratingrecordregistry_seq OWNED BY public.artifactratings.ratingrecordregistry;


CREATE TABLE public.artifactscore (
artregistry character(10) NOT NULL,
artname character varying(255) NOT NULL,
artdynasty character varying(50),
artageyears integer,
mattype public.materialtype_enum,
conservestatus public.conservationstatus_enum
);


CREATE TABLE public.artifactsecurityaccess (
secrecordregistry bigint NOT NULL,
artref character(10) NOT NULL,
ratingref bigint,
loanstatus character(25),
insvalueusd numeric(15,2),
seclevel character varying(20),
accessrestrictions text,
docustatus character varying(60),
photodocu character varying(100),
condreportstatus character varying(80),
conserverecstatus character(20),
researchaccessstatus character varying(40),
digitalrecstatus text
);


CREATE SEQUENCE public.artifactsecurityaccess_secrecordregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.artifactsecurityaccess_secrecordregistry_seq OWNED BY public.artifactsecurityaccess.secrecordregistry;


CREATE TABLE public.conditionassessments (
conditionassessregistry bigint NOT NULL,
artrefexamined text NOT NULL,
showcaserefexamined text,
lightreadrefobserved bigint,
condassessscore integer,
conserveassessdate date,
nextassessdue date
);


CREATE SEQUENCE public.conditionassessments_conditionassessregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.conditionassessments_conditionassessregistry_seq OWNED BY public.conditionassessments.conditionassessregistry;


CREATE TABLE public.conservationandmaintenance (
conservemaintregistry bigint NOT NULL,
artrefmaintained character(10) NOT NULL,
hallrefmaintained character(8),
surfreadrefobserved bigint,
conservetreatstatus character varying(50),
treatpriority character(10),
lastcleaningdate date,
nextcleaningdue date,
cleanintervaldays smallint,
maintlog text,
incidentreportstatus character varying(50),
emergencydrillstatus character(25),
stafftrainstatus character(20),
budgetallocstatus character varying(50),
maintbudgetstatus character(25),
conservefreq public.conservefreq_enum,
intervhistory text,
prevtreatments smallint,
treateffectiveness character varying(100),
reversibilitypotential public.securitylevel_enum
);


CREATE SEQUENCE public.conservationandmaintenance_conservemaintregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.conservationandmaintenance_conservemaintregistry_seq OWNED BY public.conservationandmaintenance.conservemaintregistry;


CREATE TABLE public.environmentalreadingscore (
envreadregistry bigint NOT NULL,
monitorcode character(10) NOT NULL,
readtimestamp timestamp without time zone NOT NULL,
showcaseref character(12),
tempc smallint,
tempvar24h real,
relhumidity integer,
humvar24h smallint,
airpresshpa real
);


CREATE SEQUENCE public.environmentalreadingscore_envreadregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.environmentalreadingscore_envreadregistry_seq OWNED BY public.environmentalreadingscore.envreadregistry;


CREATE TABLE public.exhibitionhalls (
hallrecord character(10) NOT NULL,
cctvcoverage character varying(100),
motiondetectstatus character varying(50),
alarmsysstatus character(25),
accessctrlstatus character varying(80),
visitorcountdaily integer,
visitorflowrate public.riskassesslevel_enum,
visitordwellmin smallint,
visitorbehaviornotes text
);


CREATE TABLE public.lightandradiationreadings (
lightradregistry bigint NOT NULL,
envreadref bigint NOT NULL,
lightlux integer,
uvuwcm2 numeric(6,2),
irwm2 numeric(6,2),
visibleexplxh integer
);


CREATE SEQUENCE public.lightandradiationreadings_lightradregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.lightandradiationreadings_lightradregistry_seq OWNED BY public.lightandradiationreadings.lightradregistry;


CREATE TABLE public.riskassessments (
riskassessregistry bigint NOT NULL,
artrefconcerned character(10) NOT NULL,
hallrefconcerned character(8),
riskassesslevel public.riskassesslevel_enum,
emergresponseplan text,
evacpriority character varying(20),
handlerestrictions character varying(100),
conservepriorityscore smallint
);


CREATE SEQUENCE public.riskassessments_riskassessregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.riskassessments_riskassessregistry_seq OWNED BY public.riskassessments.riskassessregistry;


CREATE TABLE public.sensitivitydata (
sensitivityregistry bigint NOT NULL,
artref character(10) NOT NULL,
envsensitivity character(20),
lightsensitivity character varying(80),
tempsensitivity character varying(50),
humiditysensitivity text,
vibrasensitivity character(20),
pollutantsensitivity character varying(100),
pestsensitivity text,
handlesensitivity character(20),
transportsensitivity character varying(50),
displaysensitivity character varying(120),
storagesensitivity text
);


CREATE SEQUENCE public.sensitivitydata_sensitivityregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.sensitivitydata_sensitivityregistry_seq OWNED BY public.sensitivitydata.sensitivityregistry;


CREATE TABLE public.showcases (
showcasereg character(12) NOT NULL,
hallref character(8),
airtightness real,
showcasematerial character varying(80),
sealcondition character varying(30),
maintstatus character(25),
filterstatus text,
silicagelstatus character(20),
silicagelchangedate date,
humiditybuffercap smallint,
pollutantabsorbcap numeric(5,2),
leakrate real,
pressurepa bigint,
inertgassysstatus character varying(50),
firesuppresssys character varying(50),
empowerstatus character(10),
backupsysstatus text
);


CREATE TABLE public.surfaceandphysicalreadings (
surfphysregistry bigint NOT NULL,
envreadref bigint NOT NULL,
vibralvlmms2 real,
noisedb smallint,
dustaccummgm2 numeric(5,2),
microbialcountcfu integer,
moldriskidx numeric(4,2),
pestactivitylvl character varying(50),
pesttrapcount smallint,
pestspeciesdetected text,
surfaceph numeric(3,1),
matmoistcontent numeric(4,2),
saltcrystalrisk character(20),
metalcorroderate numeric(4,2),
organicdegradidx numeric(4,2),
colorchangedeltae real,
surfacetempc numeric(5,2),
surfacerh numeric(4,1),
condenserisk character varying(60),
thermalimgstatus character(25),
structstability character varying(50),
crackmonitor text,
deformmm numeric(5,2),
wtchangepct numeric(6,5),
surfdustcoverage smallint,
o2concentration numeric(4,2),
n2concentration numeric(4,2)
);


CREATE SEQUENCE public.surfaceandphysicalreadings_surfphysregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.surfaceandphysicalreadings_surfphysregistry_seq OWNED BY public.surfaceandphysicalreadings.surfphysregistry;


CREATE TABLE public.usagerecords (
usagerecordregistry bigint NOT NULL,
artrefused character(10) NOT NULL,
showcaserefused character(12),
sensdatalink bigint,
displayrotatesched character(20),
displaydurmonths smallint,
restperiodmonths smallint,
displayreqs character varying(120),
storagereqs character varying(60),
handlingreqs character varying(80),
transportreqs text,
packingreqs character varying(90),
resaccessfreq character(10),
publicdispfreq character(25),
loanfreq character(12),
handlefreq character(10),
docufreq character varying(20),
monitorfreq character varying(35),
assessfreq character(25),
maintfreq character(25),
inspectfreq character(25),
calibfreq character(25),
certstatus character varying(40),
compliancestatus character varying(55),
auditstatus public.auditstatus_enum,
qualityctrlstatus public.qualitycontrolstatus_enum
);


CREATE SEQUENCE public.usagerecords_usagerecordregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.usagerecords_usagerecordregistry_seq OWNED BY public.usagerecords.usagerecordregistry;


ALTER TABLE ONLY public.airqualityreadings ALTER COLUMN aqrecordregistry SET DEFAULT nextval('public.airqualityreadings_aqrecordregistry_seq'::regclass);


ALTER TABLE ONLY public.artifactratings ALTER COLUMN ratingrecordregistry SET DEFAULT nextval('public.artifactratings_ratingrecordregistry_seq'::regclass);


ALTER TABLE ONLY public.artifactsecurityaccess ALTER COLUMN secrecordregistry SET DEFAULT nextval('public.artifactsecurityaccess_secrecordregistry_seq'::regclass);


ALTER TABLE ONLY public.conditionassessments ALTER COLUMN conditionassessregistry SET DEFAULT nextval('public.conditionassessments_conditionassessregistry_seq'::regclass);


ALTER TABLE ONLY public.conservationandmaintenance ALTER COLUMN conservemaintregistry SET DEFAULT nextval('public.conservationandmaintenance_conservemaintregistry_seq'::regclass);


ALTER TABLE ONLY public.environmentalreadingscore ALTER COLUMN envreadregistry SET DEFAULT nextval('public.environmentalreadingscore_envreadregistry_seq'::regclass);


ALTER TABLE ONLY public.lightandradiationreadings ALTER COLUMN lightradregistry SET DEFAULT nextval('public.lightandradiationreadings_lightradregistry_seq'::regclass);


ALTER TABLE ONLY public.riskassessments ALTER COLUMN riskassessregistry SET DEFAULT nextval('public.riskassessments_riskassessregistry_seq'::regclass);


ALTER TABLE ONLY public.sensitivitydata ALTER COLUMN sensitivityregistry SET DEFAULT nextval('public.sensitivitydata_sensitivityregistry_seq'::regclass);


ALTER TABLE ONLY public.surfaceandphysicalreadings ALTER COLUMN surfphysregistry SET DEFAULT nextval('public.surfaceandphysicalreadings_surfphysregistry_seq'::regclass);


ALTER TABLE ONLY public.usagerecords ALTER COLUMN usagerecordregistry SET DEFAULT nextval('public.usagerecords_usagerecordregistry_seq'::regclass);
