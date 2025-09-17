CREATE TABLE public.alerts (
alertreg character varying(50) NOT NULL,
compreg uuid,
deviceref character varying(50),
incidentref character varying(50),
alertmoment timestamp without time zone,
alertstat character varying(50),
alertcnt smallint,
maintprior character varying(50),
replaceprior character varying(50),
optpotential character varying(100)
);


CREATE TABLE public.electrical (
elecregistry character varying(50) NOT NULL,
engyunitref character varying(50),
efflogref character varying(50),
iscinita numeric(7,3),
isccurra numeric(7,3),
vocinitv numeric(7,3),
voccurrv numeric(7,3),
impinita numeric(7,3),
impcurra numeric(7,3),
vmpinitv numeric(7,3),
vmpcurrv numeric(6,2),
ffactorinit numeric(7,3),
ffactorcurr numeric(7,3),
seriesresohm numeric(7,3),
shuntresohm numeric(4,1)
);


CREATE TABLE public.environment (
envregistry character varying(50) NOT NULL,
arearegistry uuid,
envmoment timestamp without time zone,
celltempc numeric(7,3),
ambtempc numeric(7,3),
soillosspct numeric(7,3),
dustdengm2 numeric(7,3),
cleancycledays smallint,
lastcleandt date,
relhumpct numeric(7,3),
windspdms numeric(7,3),
winddirdeg numeric(7,3),
preciptmm numeric(6,2),
airpresshpa numeric(6,2),
uv_idx numeric(7,3),
cloudcovpct numeric(7,3),
snowcovpct numeric(7,3),
irradiance_conditions jsonb
);


CREATE TABLE public.inverter (
invertregistry character varying(50) NOT NULL,
siteref uuid,
invertmoment timestamp without time zone,
inverttempc numeric(7,3),
gridvolt numeric(7,3),
gridfreqhz numeric(7,3),
pwrqualidx numeric(7,3),
power_metrics jsonb
);


CREATE TABLE public.maintenance (
maintregistry character varying(50) NOT NULL,
powerref uuid,
compref character varying(50),
obsref character varying(50),
inspectmeth character varying(100),
inspectres character varying(150),
inspectdate date,
maintsched character varying(100),
wtystatus character varying(50),
wtyclaimcnt smallint,
maintcostusd numeric(9,2),
cleancostusd numeric(8,3),
replacecostusd numeric(9,3),
revlossusd numeric(7,2)
);


CREATE TABLE public.panel (
panemark character varying(50) NOT NULL,
hubregistry uuid,
panemfr character varying(100),
paneline character varying(100),
panetype character varying(50),
powratew smallint,
paneeffpct numeric(7,3),
nomtempc numeric(7,3),
tempcoef numeric(4,3)
);


CREATE TABLE public.performance (
perfregistry character varying(50) NOT NULL,
solmodref character varying(50),
perfmoment timestamp without time zone,
measpoww numeric(9,3),
powlossw numeric(8,3),
efficiency_profile jsonb
);


CREATE TABLE public.plant (
growregistry uuid NOT NULL,
growalias character varying(100),
gencapmw numeric(7,3),
initdate date
);
