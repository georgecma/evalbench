CREATE TYPE public.auditstatus_enum AS ENUM (
'Complete',
'Incomplete',
'InProgress'
);


CREATE TYPE public.checkstatus_enum AS ENUM (
'Passed',
'Failed',
'Success',
'Pending',
'Verified'
);


CREATE TYPE public.compliancelevel_enum AS ENUM (
'Compliant',
'Non-compliant',
'Partial'
);


CREATE TYPE public.encryptionstatus_enum AS ENUM (
'Full',
'Partial'
);


CREATE TYPE public.partialnone_enum AS ENUM (
'None',
'Partial'
);


CREATE TYPE public.securityrating_enum AS ENUM (
'A',
'B',
'C',
'D'
);


CREATE TABLE public.auditandcompliance (
audittrace integer NOT NULL,
profjoin integer NOT NULL,
compjoin integer NOT NULL,
vendjoin integer NOT NULL,
recordregistry character(10),
audtrailstate public.auditstatus_enum,
findtally smallint,
critfindnum smallint,
remedstate character varying(40),
remeddue date,
authnotify character varying(40),
bordermech character varying(40),
transimpassess text,
localreqs text,
datamapstate character varying(40),
sysintstate character varying(40),
accreqnum smallint,
delreqnum smallint,
rectreqnum smallint,
portreqnum smallint,
resptimeday numeric(4,1)
);


CREATE SEQUENCE public.auditandcompliance_audittrace_seq1
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.auditandcompliance_audittrace_seq1 OWNED BY public.auditandcompliance.audittrace;


CREATE TABLE public.compliance (
compliancetrace integer NOT NULL,
risktie integer NOT NULL,
vendortie integer NOT NULL,
recordregistry character(10),
legalbase character varying(150),
consentstate character varying(30),
consentcoll date,
consentexp date,
purplimit character varying(300),
purpdesc text,
gdprcomp public.compliancelevel_enum,
ccpacomp public.compliancelevel_enum,
piplcomp public.compliancelevel_enum,
loclawcomp public.compliancelevel_enum,
regapprovals character varying(300),
privimpassess text,
datasubjright character varying(40)
);


CREATE SEQUENCE public.compliance_compliancetrace_seq1
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.compliance_compliancetrace_seq1 OWNED BY public.compliance.compliancetrace;


CREATE TABLE public.dataflow (
recordregistry character(10) NOT NULL,
flowstamp timestamp(6) without time zone,
flowtag character varying(20),
orignation character varying(80),
destnation character varying(80),
origactor character varying(150),
destactor character varying(150),
chanproto character varying(30),
chanfreq character varying(25),
datasizemb numeric(12,2),
durmin smallint,
bwidthpct numeric(5,2),
successpct numeric(5,2),
errtally smallint,
rtrytally smallint
);


CREATE TABLE public.dataprofile (
profiletrace integer NOT NULL,
flowsign character(10) NOT NULL,
riskjoin integer NOT NULL,
recordregistry character(10),
datatype character varying(80),
datasense character varying(30),
volgb numeric(10,2),
rectally bigint,
subjtally bigint,
retdays integer,
formattype character varying(80),
qltyscore numeric(4,2),
intcheck public.checkstatus_enum,
csumverify public.checkstatus_enum,
srcvalstate public.checkstatus_enum,
destvalstate public.checkstatus_enum
);


CREATE SEQUENCE public.dataprofile_profiletrace_seq1
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.dataprofile_profiletrace_seq1 OWNED BY public.dataprofile.profiletrace;


CREATE TABLE public.riskmanagement (
risktrace integer NOT NULL,
flowlink character(10) NOT NULL,
recordregistry character(10),
riskassess numeric(4,2),
riskmitstate character varying(40),
secureaction text,
breachnotify text,
incidentplan text,
incidentcount smallint,
breachcount smallint,
nearmissnum smallint,
avgresolhrs numeric(5,2),
slapct numeric(4,2),
costusd numeric(11,2),
penusd numeric(11,2),
coveragestate character varying(40),
residrisklevel character varying(40),
ctrleff numeric(4,2),
compscore numeric(4,2),
maturitylevel character varying(40),
nextrevdate date,
planstate character varying(45)
);


CREATE SEQUENCE public.riskmanagement_risktrace_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.riskmanagement_risktrace_seq OWNED BY public.riskmanagement.risktrace;


CREATE TABLE public.securityprofile (
securitytrace integer NOT NULL,
flowkey character(10) NOT NULL,
riskkey integer NOT NULL,
profilekey integer NOT NULL,
recordregistry character(10),
encstate public.encryptionstatus_enum,
encmeth character varying(40),
keymanstate character varying(40),
masklevel public.partialnone_enum,
anonmeth character varying(40),
psymstate character varying(40),
authmeth character varying(40),
authzframe character varying(45),
aclstate character varying(30),
apisecstate character varying(30),
logintcheck character varying(30),
logretdays smallint,
bkpstate character varying(35),
drecstate character varying(35),
bcstate character varying(35)
);


CREATE SEQUENCE public.securityprofile_securitytrace_seq1
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.securityprofile_securitytrace_seq1 OWNED BY public.securityprofile.securitytrace;


CREATE TABLE public.vendormanagement (
vendortrace integer NOT NULL,
secjoin integer NOT NULL,
riskassoc integer NOT NULL,
recordregistry character(10),
vendassess character varying(40),
vendsecrate public.securityrating_enum,
vendauddate date,
contrstate character varying(30),
contrexpire date,
dpastate character varying(30),
sccstate character varying(30),
bcrstate character varying(30),
docustate character varying(30),
polcomp character varying(30),
proccomp character varying(30),
trainstate character varying(30),
certstate character varying(30),
monstate character varying(30),
repstate character varying(30),
stakecomm text
);


CREATE SEQUENCE public.vendormanagement_vendortrace_seq1
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.vendormanagement_vendortrace_seq1 OWNED BY public.vendormanagement.vendortrace;


ALTER TABLE ONLY public.auditandcompliance ALTER COLUMN audittrace SET DEFAULT nextval('public.auditandcompliance_audittrace_seq1'::regclass);


ALTER TABLE ONLY public.compliance ALTER COLUMN compliancetrace SET DEFAULT nextval('public.compliance_compliancetrace_seq1'::regclass);


ALTER TABLE ONLY public.dataprofile ALTER COLUMN profiletrace SET DEFAULT nextval('public.dataprofile_profiletrace_seq1'::regclass);


ALTER TABLE ONLY public.riskmanagement ALTER COLUMN risktrace SET DEFAULT nextval('public.riskmanagement_risktrace_seq'::regclass);


ALTER TABLE ONLY public.securityprofile ALTER COLUMN securitytrace SET DEFAULT nextval('public.securityprofile_securitytrace_seq1'::regclass);


ALTER TABLE ONLY public.vendormanagement ALTER COLUMN vendortrace SET DEFAULT nextval('public.vendormanagement_vendortrace_seq1'::regclass);
