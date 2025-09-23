CREATE TABLE public.equipment (
equipregistry character(50) NOT NULL,
equipform character varying(28),
equipdesign character varying(14),
equiptune date,
equipstatus character varying(16),
powerlevel smallint
);


CREATE TABLE public.personnel (
crewregistry character(50) NOT NULL,
crewlabel character varying(50),
leadregistry character(50),
leadlabel character varying(40)
);


CREATE TABLE public.projects (
arcregistry character varying(50) NOT NULL,
vesseltag character varying(60),
fundflux text,
authpin character(50),
authhalt date
);


CREATE TABLE public.scanconservation (
cureregistry bigint NOT NULL,
arcref character varying(50) NOT NULL,
zoneref character varying(12) NOT NULL,
harmassess character varying(15),
curerank character varying(15),
structstate character varying(15),
intervhistory text,
priordocs text
);


CREATE SEQUENCE public.scanconservation_cureregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.scanconservation_cureregistry_seq OWNED BY public.scanconservation.cureregistry;


CREATE TABLE public.scanenvironment (
airregistry bigint NOT NULL,
zoneref character varying(12) NOT NULL,
equipref character(50) NOT NULL,
ambictemp numeric(5,2),
humepct numeric(5,2),
illumelux integer,
geosignal character varying(15),
trackstatus character varying(12),
linkstatus character varying(12),
photomap character(4),
imgcount smallint
);


CREATE SEQUENCE public.scanenvironment_airregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.scanenvironment_airregistry_seq OWNED BY public.scanenvironment.airregistry;


CREATE TABLE public.scanfeatures (
traitregistry bigint NOT NULL,
zoneref character varying(12) NOT NULL,
equipref character(50) NOT NULL,
traitextract character varying(25),
traitcount integer,
articount integer,
structkind character varying(15),
matkind character varying(15),
huestudy character varying(15),
texturestudy character varying(15),
patternnote text
);


CREATE SEQUENCE public.scanfeatures_traitregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.scanfeatures_traitregistry_seq OWNED BY public.scanfeatures.traitregistry;


CREATE TABLE public.scanmesh (
facetregistry bigint NOT NULL,
zoneref character varying(12) NOT NULL,
equipref character(50) NOT NULL,
facetverts bigint,
facetfaces bigint,
facetresmm numeric(5,2),
texdist character varying(5),
texpix integer,
uvmapqual character varying(50),
geomdeltamm numeric(6,3)
);


CREATE SEQUENCE public.scanmesh_facetregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.scanmesh_facetregistry_seq OWNED BY public.scanmesh.facetregistry;


CREATE TABLE public.scanpointcloud (
cloudregistry bigint NOT NULL,
crewref character(50) NOT NULL,
arcref character varying(50) NOT NULL,
scanresolmm numeric(5,2),
pointdense integer,
coverpct numeric(4,1),
totalpts bigint,
clouddense integer,
lappct numeric(4,1),
noisedb numeric(6,3),
refpct numeric(4,1)
);


CREATE SEQUENCE public.scanpointcloud_cloudregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.scanpointcloud_cloudregistry_seq OWNED BY public.scanpointcloud.cloudregistry;


CREATE TABLE public.scanprocessing (
flowregistry bigint NOT NULL,
equipref character(50) NOT NULL,
zoneref character varying(12) NOT NULL,
flowsoft character varying(25),
flowhrs numeric(5,2),
proccpu smallint,
memusagegb numeric(6,2),
procgpu smallint,
stashloc character varying(12),
safebak character(50),
datalevel text,
metabench character varying(50),
coordframe character varying(12),
elevref character varying(16),
remaingb numeric(7,2),
stationlink character(50),
camcal text,
lensdist character varying(14),
colortune character(50),
flowstage character varying(18),
fmtver character(3)
);


CREATE SEQUENCE public.scanprocessing_flowregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.scanprocessing_flowregistry_seq OWNED BY public.scanprocessing.flowregistry;


CREATE TABLE public.scanqc (
qualregistry bigint NOT NULL,
arcref character varying(50) NOT NULL,
crewref character(50) NOT NULL,
accucheck character varying(22),
ctrlstate character(50),
valimeth character varying(18),
valistate text,
archstat character varying(50),
pubstat character varying(24),
copystat character(50),
refmention character varying(60),
remark text
);


CREATE SEQUENCE public.scanqc_qualregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.scanqc_qualregistry_seq OWNED BY public.scanqc.qualregistry;


CREATE TABLE public.scanregistration (
logregistry bigint NOT NULL,
crewref character(50) NOT NULL,
arcref character varying(50) NOT NULL,
logaccumm numeric(5,3),
refmark character varying(6),
ctrlpts character varying(6),
logmethod character varying(15),
transform character varying(15),
errscale character varying(20),
errvalmm numeric(6,3)
);


CREATE SEQUENCE public.scanregistration_logregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.scanregistration_logregistry_seq OWNED BY public.scanregistration.logregistry;


CREATE TABLE public.scans (
questregistry character varying(16) NOT NULL,
chronotag timestamp without time zone,
arcref character varying(50) NOT NULL,
crewref character(50) NOT NULL,
zoneref character varying(12) NOT NULL,
scancount smallint,
climtune character varying(22),
huecatch character varying(50),
fmtfile character(4),
gbsize numeric(5,2),
pressratio numeric(4,2),
spanmin numeric(5,2)
);


CREATE TABLE public.scanspatial (
domainregistry bigint NOT NULL,
arcref character varying(50) NOT NULL,
crewref character(50) NOT NULL,
aream2 numeric(8,3),
volm3 numeric(9,4),
boxx numeric(8,2),
boxy numeric(8,3),
boxz numeric(9,2),
angleaz real,
angletilt double precision,
groundspan numeric(6,3)
);


CREATE SEQUENCE public.scanspatial_domainregistry_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.scanspatial_domainregistry_seq OWNED BY public.scanspatial.domainregistry;


CREATE TABLE public.sites (
zoneregistry character varying(12) NOT NULL,
zonelabel text,
digunit character varying(50),
gridtrace character varying(12),
geox numeric(8,5),
geoy numeric(8,5),
heightm numeric(7,1),
depthc numeric(7,1),
phasefactor character varying(25),
guessdate character varying(20),
typesite character varying(25),
presstat character varying(25),
guardhint character(50),
entrystat character varying(50),
saferank character varying(15),
insurstat character varying(15),
riskeval text,
healtheval character varying(12),
envhaz character(50)
);


ALTER TABLE ONLY public.scanconservation ALTER COLUMN cureregistry SET DEFAULT nextval('public.scanconservation_cureregistry_seq'::regclass);


ALTER TABLE ONLY public.scanenvironment ALTER COLUMN airregistry SET DEFAULT nextval('public.scanenvironment_airregistry_seq'::regclass);


ALTER TABLE ONLY public.scanfeatures ALTER COLUMN traitregistry SET DEFAULT nextval('public.scanfeatures_traitregistry_seq'::regclass);


ALTER TABLE ONLY public.scanmesh ALTER COLUMN facetregistry SET DEFAULT nextval('public.scanmesh_facetregistry_seq'::regclass);


ALTER TABLE ONLY public.scanpointcloud ALTER COLUMN cloudregistry SET DEFAULT nextval('public.scanpointcloud_cloudregistry_seq'::regclass);


ALTER TABLE ONLY public.scanprocessing ALTER COLUMN flowregistry SET DEFAULT nextval('public.scanprocessing_flowregistry_seq'::regclass);


ALTER TABLE ONLY public.scanqc ALTER COLUMN qualregistry SET DEFAULT nextval('public.scanqc_qualregistry_seq'::regclass);


ALTER TABLE ONLY public.scanregistration ALTER COLUMN logregistry SET DEFAULT nextval('public.scanregistration_logregistry_seq'::regclass);


ALTER TABLE ONLY public.scanspatial ALTER COLUMN domainregistry SET DEFAULT nextval('public.scanspatial_domainregistry_seq'::regclass);

