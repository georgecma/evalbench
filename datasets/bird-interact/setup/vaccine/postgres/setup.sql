CREATE TYPE public.alertstatus_enum AS ENUM (
'None',
'Temperature',
'Security',
'Battery'
);


CREATE TYPE public.contaminationrisk_enum AS ENUM (
'Low',
'Medium',
'High'
);


CREATE TYPE public.customsflag_enum AS ENUM (
'Cleared',
'In Process',
'Pending'
);


CREATE TYPE public.dataloggerstatus_enum AS ENUM (
'Active',
'Malfunction',
'Battery Low'
);


CREATE TYPE public.gpstrackingstatus_enum AS ENUM (
'Active',
'Lost',
'Limited'
);


CREATE TYPE public.insuranceflag_enum AS ENUM (
'Active',
'Expired',
'Pending'
);


CREATE TYPE public.integritystatus_enum AS ENUM (
'Intact',
'Under Investigation',
'Compromised'
);


CREATE TYPE public.networksignal_enum AS ENUM (
'Excellent',
'Good',
'Poor',
'None'
);


CREATE TYPE public.packagecondition_enum AS ENUM (
'Good',
'Excellent',
'Fair',
'Poor'
);


CREATE TYPE public.qualitycheck_enum AS ENUM (
'Passed',
'Failed',
'Pending'
);


CREATE TYPE public.securitysealstatus_enum AS ENUM (
'Intact',
'Broken',
'Missing'
);


CREATE TYPE public.sensorstatus_enum AS ENUM (
'Normal',
'Triggered',
'Malfunction'
);


CREATE TYPE public.sterilitystatus_enum AS ENUM (
'Compromised',
'Maintained',
'Unknown'
);


CREATE TYPE public.systemhealth_enum AS ENUM (
'Good',
'Poor',
'Fair'
);


CREATE TYPE public.tamperevidence_enum AS ENUM (
'None Detected',
'Confirmed',
'Suspected'
);


CREATE TYPE public.transmissionstatus_enum AS ENUM (
'Failed',
'Delayed',
'Real-time'
);


CREATE TYPE public.transportmode_enum AS ENUM (
'Road',
'Rail',
'Air',
'Sea'
);


CREATE TYPE public.vehiclekind_enum AS ENUM (
'Refrigerated Truck',
'Cargo Aircraft',
'Reefer Container'
);


CREATE TABLE public.container (
containregistry character varying(20) NOT NULL,
containmodel character varying(40),
volliters integer,
masskg real,
containflag character varying(30),
coolkind character varying(40),
coolmass real,
coolremainpct numeric(5,2),
coolrefills smallint,
refilllatest date,
refillnext date,
batterypct real,
pwrfeed character varying(40),
pwrbackupflag character varying(40),
shipown character varying(20) NOT NULL
);


CREATE TABLE public.datalogger (
loggerreg character varying(20) NOT NULL,
logflag public.dataloggerstatus_enum,
loginterval smallint,
transmitflag public.transmissionstatus_enum,
datauptime timestamp without time zone,
datapct real,
batteryswap date,
firmvers character varying(50),
softupdate text,
syshealth public.systemhealth_enum,
memusepct smallint,
storecapmb integer,
storeremainmb integer,
netsignal public.networksignal_enum,
commproto character varying(20),
syncflag character varying(20),
syncfreqhr smallint,
containlog character varying(20) NOT NULL,
shiplog character varying(20)
);


CREATE TABLE public.regulatoryandmaintenance (
regmaintregistry integer NOT NULL,
maintflag character varying(20),
maintdatelast date,
maintdatenext date,
calibflag character varying(20),
calibdatelast date,
calibdatenext date,
docuflag character varying(20),
compscore real,
riskflag character varying(50),
incidents smallint,
resolveflag character varying(50),
respperson character varying(100),
contactno character varying(50),
contactemerg character varying(50),
inspectdatelast date,
inspectdatenext date,
inspectoutcome character varying(50),
correctactions text,
preventsteps text,
validflag character varying(20),
verifymethod character varying(50),
shipgov character varying(20) NOT NULL,
vehgov character varying(20)
);


CREATE SEQUENCE public.regulatoryandmaintenance_regmaintregistry_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.regulatoryandmaintenance_regmaintregistry_seq OWNED BY public.regulatoryandmaintenance.regmaintregistry;


CREATE TABLE public.sensordata (
sensortrack integer NOT NULL,
storetempc real,
temptolc numeric(4,1),
tempnowc real,
tempdevcount smallint,
tempmaxc numeric(5,2),
tempminc numeric(5,2),
tempstabidx real,
humiditypct numeric(4,1),
presskpa real,
shockflag character varying(20),
tiltflag character varying(20),
impactflag character varying(20),
vibelvlmms numeric(6,2),
lightlux integer,
acceldata real,
handleevents smallint,
critevents smallint,
alerts smallint,
alerttime timestamp without time zone,
alertkind character varying(50),
containlink character varying(20) NOT NULL,
vehsenseref character varying(20)
);


CREATE SEQUENCE public.sensordata_sensortrack_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.sensordata_sensortrack_seq OWNED BY public.sensordata.sensortrack;


CREATE TABLE public.shipments (
shipmentregistry character varying(20) NOT NULL,
timemark timestamp without time zone NOT NULL,
routealign character varying(30),
customsflag public.customsflag_enum,
imppermitref character varying(50),
exppermitref character varying(50),
regprofile character varying(50),
insureflag public.insuranceflag_enum,
insureref character varying(50),
qualcheck public.qualitycheck_enum,
integritymark public.integritystatus_enum,
contamlevel public.contaminationrisk_enum,
sterilemark public.sterilitystatus_enum,
packagestate public.packagecondition_enum,
sealflag public.securitysealstatus_enum,
sealref character varying(50),
tampersign public.tamperevidence_enum,
handlingguide text,
storagepose character varying(40),
generalnote text
);


CREATE TABLE public.transportinfo (
vehiclereg character varying(20) NOT NULL,
vehiclekind public.vehiclekind_enum,
vehtempc real,
speedkm real,
distdonekm numeric(9,2),
distleftkm numeric(9,2),
eta timestamp without time zone,
departsite character varying(100),
currentsite character varying(100),
destsite character varying(100),
gpsflag public.gpstrackingstatus_enum,
latvalue double precision,
lonvalue double precision,
altmeter real,
locupdatemin smallint,
locupdatemark timestamp without time zone,
transmode public.transportmode_enum,
carrlabel text,
carrcert character varying(50),
shiptransit character varying(20) NOT NULL,
containtransit character varying(20)
);


CREATE TABLE public.vaccinedetails (
vacregistry integer NOT NULL,
vacvariant character varying(50),
mfgsource text,
batchlabel character varying(50),
prodday date,
expireday date,
lotmeasure bigint,
vialtally smallint,
dosepervial smallint,
dosetotal integer,
shipinject character varying(20) NOT NULL,
containvac character varying(20)
);


CREATE SEQUENCE public.vaccinedetails_vacregistry_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.vaccinedetails_vacregistry_seq OWNED BY public.vaccinedetails.vacregistry;


ALTER TABLE ONLY public.regulatoryandmaintenance ALTER COLUMN regmaintregistry SET DEFAULT nextval('public.regulatoryandmaintenance_regmaintregistry_seq'::regclass);


ALTER TABLE ONLY public.sensordata ALTER COLUMN sensortrack SET DEFAULT nextval('public.sensordata_sensortrack_seq'::regclass);


ALTER TABLE ONLY public.vaccinedetails ALTER COLUMN vacregistry SET DEFAULT nextval('public.vaccinedetails_vacregistry_seq'::regclass);
