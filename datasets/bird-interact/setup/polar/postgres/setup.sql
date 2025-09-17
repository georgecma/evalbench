CREATE TYPE public.alarmstatus_enum AS ENUM (
'Normal',
'Critical',
'Warning'
);


CREATE TYPE public.antennastatus_enum AS ENUM (
'Error',
'Normal',
'Warning'
);


CREATE TYPE public.backuppowerstatus_enum AS ENUM (
'Fault',
'Active',
'Standby'
);


CREATE TYPE public.bluetoothstatus_enum AS ENUM (
'On',
'Error',
'Off',
'Pairing'
);


CREATE TYPE public.calibrationstatus_enum AS ENUM (
'Expired',
'Valid',
'Due'
);


CREATE TYPE public.chargingstatus_enum AS ENUM (
'Error',
'Not Charging',
'Charging',
'Full'
);


CREATE TYPE public.codetectionstatus_enum AS ENUM (
'Fault',
'Alert',
'Normal'
);


CREATE TYPE public.compliancestatus_enum AS ENUM (
'Review',
'Non-compliant',
'Compliant'
);


CREATE TYPE public.crewcertificationstatus_enum AS ENUM (
'Valid',
'Pending',
'Expired'
);


CREATE TYPE public.dataloggingstatus_enum AS ENUM (
'Active',
'Paused',
'Error'
);


CREATE TYPE public.defrosterstatus_enum AS ENUM (
'On',
'Auto',
'Off'
);


CREATE TYPE public.documentationstatus_enum AS ENUM (
'Updated',
'Incomplete',
'Complete'
);


CREATE TYPE public.doorstatus_enum AS ENUM (
'Closed',
'Locked',
'Open'
);


CREATE TYPE public.emergencybeaconstatus_enum AS ENUM (
'Active',
'Standby',
'Testing'
);


CREATE TYPE public.emergencylightstatus_enum AS ENUM (
'On',
'Off',
'Testing'
);


CREATE TYPE public.emergencystopstatus_enum AS ENUM (
'Activated',
'Reset',
'Ready'
);


CREATE TYPE public.equipmenttype_enum AS ENUM (
'Shelter',
'Scientific',
'Safety',
'Vehicle',
'Generator',
'Communication'
);


CREATE TYPE public.externallightstatus_enum AS ENUM (
'Off',
'On',
'Auto'
);


CREATE TYPE public.firedetectionstatus_enum AS ENUM (
'Normal',
'Alert',
'Fault'
);


CREATE TYPE public.fuelcellstatus_enum AS ENUM (
'Standby',
'Fault',
'Operating'
);


CREATE TYPE public.gasdetectionstatus_enum AS ENUM (
'Alert',
'Fault',
'Normal'
);


CREATE TYPE public.gpssignalstrength_enum AS ENUM (
'Strong',
'None',
'Weak',
'Medium'
);


CREATE TYPE public.hatchstatus_enum AS ENUM (
'Closed',
'Open',
'Locked'
);


CREATE TYPE public.heaterstatus_enum AS ENUM (
'Off',
'On',
'Auto'
);


CREATE TYPE public.inspectionstatus_enum AS ENUM (
'Failed',
'Passed',
'Pending'
);


CREATE TYPE public.insulationstatus_enum AS ENUM (
'Fair',
'Poor',
'Good'
);


CREATE TYPE public.lifesupportstatus_enum AS ENUM (
'Warning',
'Critical',
'Normal'
);


CREATE TYPE public.lightingstatus_enum AS ENUM (
'Off',
'Auto',
'On'
);


CREATE TYPE public.locationtype_enum AS ENUM (
'Arctic',
'Antarctic'
);


CREATE TYPE public.medicalequipmentstatus_enum AS ENUM (
'Normal',
'Critical',
'Warning'
);


CREATE TYPE public.operationalstatus_enum AS ENUM (
'Storage',
'Standby',
'Repair',
'Active',
'Maintenance'
);


CREATE TYPE public.oxygensupplystatus_enum AS ENUM (
'Warning',
'Normal',
'Critical'
);


CREATE TYPE public.powergridstatus_enum AS ENUM (
'Connected',
'Disconnected',
'Island Mode'
);


CREATE TYPE public.powersource_enum AS ENUM (
'Wind',
'Solar',
'Diesel',
'Hybrid',
'Battery'
);


CREATE TYPE public.powerstatus_enum AS ENUM (
'Sleep',
'Charging',
'On',
'Off'
);


CREATE TYPE public.precipitationtype_enum AS ENUM (
'Blowing Snow',
'Ice',
'Snow',
'None'
);


CREATE TYPE public.safetysystemstatus_enum AS ENUM (
'Fault',
'Active',
'Standby'
);


CREATE TYPE public.satelliteconnectionstatus_enum AS ENUM (
'Limited',
'Connected',
'Disconnected'
);


CREATE TYPE public.scientificequipmentstatus_enum AS ENUM (
'Standby',
'Operating',
'Fault'
);


CREATE TYPE public.sensorstatus_enum AS ENUM (
'Error',
'Warning',
'Normal'
);


CREATE TYPE public.smokedetectionstatus_enum AS ENUM (
'Fault',
'Alert',
'Normal'
);


CREATE TYPE public.solarpanelstatus_enum AS ENUM (
'Fault',
'Inactive',
'Active'
);


CREATE TYPE public.structuralintegritystatus_enum AS ENUM (
'Warning',
'Critical',
'Normal'
);


CREATE TYPE public.thermalimagingstatus_enum AS ENUM (
'Warning',
'Critical',
'Normal'
);


CREATE TYPE public.ventilationstatus_enum AS ENUM (
'On',
'Auto',
'Off'
);


CREATE TYPE public.wastemanagementstatus_enum AS ENUM (
'Critical',
'Warning',
'Normal'
);


CREATE TYPE public.watersupplystatus_enum AS ENUM (
'Normal',
'Warning',
'Critical'
);


CREATE TYPE public.windowstatus_enum AS ENUM (
'Partial',
'Closed',
'Open'
);


CREATE TYPE public.windturbinestatus_enum AS ENUM (
'Fault',
'Operating',
'Stopped'
);


CREATE TABLE public.cabinenvironment (
cabinregistry integer NOT NULL,
cabineqref character varying(70) NOT NULL,
cabinlocref integer,
emergencybeaconstatus public.emergencybeaconstatus_enum,
ventilationstatus public.ventilationstatus_enum,
ventilationspeedpercent numeric(5,2),
heaterstatus public.heaterstatus_enum,
heatertemperaturec double precision,
defrosterstatus public.defrosterstatus_enum,
windowstatus public.windowstatus_enum,
doorstatus public.doorstatus_enum,
hatchstatus public.hatchstatus_enum,
cabincommref integer,
cabinclimate jsonb
);


CREATE SEQUENCE public.cabinenvironment_cabinregistry_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.cabinenvironment_cabinregistry_seq OWNED BY public.cabinenvironment.cabinregistry;


CREATE TABLE public.chassisandvehicle (
chassisregistry integer NOT NULL,
chassiseqref character varying(70) NOT NULL,
chassistransref integer,
brakepadwearpercent real,
brakefluidlevelpercent numeric(5,2),
brakepressurekpa integer,
tracktensionkn numeric(7,3),
trackwearpercent double precision,
suspensionheightmm numeric(6,1),
vehiclespeedkmh real,
vehicleloadkg numeric(9,2),
vehicleangledegrees numeric(6,2),
vehicleheadingdegrees numeric(5,1),
chassisengref integer,
tiremetrics jsonb
);


CREATE SEQUENCE public.chassisandvehicle_chassisregistry_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.chassisandvehicle_chassisregistry_seq OWNED BY public.chassisandvehicle.chassisregistry;


CREATE TABLE public.communication (
commregistry integer NOT NULL,
commeqref character varying(70) NOT NULL,
commlocref integer,
radiofrequencymhz real,
antennastatus public.antennastatus_enum,
bluetoothstatus public.bluetoothstatus_enum,
commopmaintref integer,
signalmetrics jsonb
);


CREATE SEQUENCE public.communication_commregistry_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.communication_commregistry_seq OWNED BY public.communication.commregistry;


CREATE TABLE public.engineandfluids (
engineregistry integer NOT NULL,
engfluidseqref character varying(60) NOT NULL,
engfluidspbref integer,
enginespeedrpm integer,
engineloadpercent real,
enginetemperaturec double precision,
enginehours numeric(8,1),
engfluidsopmaintref integer,
fluidmetrics jsonb
);


CREATE SEQUENCE public.engineandfluids_engineregistry_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.engineandfluids_engineregistry_seq OWNED BY public.engineandfluids.engineregistry;


CREATE TABLE public.equipment (
equipmentcode character varying(50) NOT NULL,
equipmenttype public.equipmenttype_enum,
equipmentmodel character varying(80),
manufacturer character varying(120),
servicelifeyears smallint,
equipmentutilizationpercent numeric(5,2),
reliabilityindex numeric(6,3),
performanceindex numeric(7,2),
efficiencyindex real,
safetyindex numeric(5,3),
environmentalimpactindex double precision
);


CREATE TABLE public.lightingandsafety (
lightregistry integer NOT NULL,
lighteqref character varying(70) NOT NULL,
lightingstatus public.lightingstatus_enum,
lightingintensitypercent numeric(5,2),
externallightstatus public.externallightstatus_enum,
emergencylightstatus public.emergencylightstatus_enum,
emergencystopstatus public.emergencystopstatus_enum,
alarmstatus public.alarmstatus_enum,
safetysystemstatus public.safetysystemstatus_enum,
lifesupportstatus public.lifesupportstatus_enum,
oxygensupplystatus public.oxygensupplystatus_enum,
medicalequipmentstatus public.medicalequipmentstatus_enum,
wastemanagementstatus public.wastemanagementstatus_enum,
watersupplystatus public.watersupplystatus_enum,
safetysensors jsonb
);


CREATE SEQUENCE public.lightingandsafety_lightregistry_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.lightingandsafety_lightregistry_seq OWNED BY public.lightingandsafety.lightregistry;


CREATE TABLE public.location (
locationregistry integer NOT NULL,
loceqref character varying(70) NOT NULL,
"Timestamp" timestamp without time zone,
stationname text,
locationtype public.locationtype_enum,
latitude numeric(9,6),
longitude numeric(9,6),
altitudem numeric(7,2)
);


CREATE SEQUENCE public.location_locationregistry_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.location_locationregistry_seq OWNED BY public.location.locationregistry;


CREATE TABLE public.operationmaintenance (
opmaintregistry integer NOT NULL,
opmainteqref character varying(60) NOT NULL,
opmaintlocref integer,
operationhours numeric(8,3),
maintenancecyclehours numeric(7,2),
lastmaintenancedate date,
nextmaintenancedue date,
operationalstatus public.operationalstatus_enum,
crewcertificationstatus public.crewcertificationstatus_enum,
inspectionstatus public.inspectionstatus_enum,
compliancestatus public.compliancestatus_enum,
documentationstatus public.documentationstatus_enum,
opmaintcommref integer,
costmetrics jsonb
);


CREATE SEQUENCE public.operationmaintenance_opmaintregistry_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.operationmaintenance_opmaintregistry_seq OWNED BY public.operationmaintenance.opmaintregistry;


CREATE TABLE public.powerbattery (
powerbattregistry integer NOT NULL,
pwrbatteqref character varying(70) NOT NULL,
powerstatus public.powerstatus_enum,
powersource public.powersource_enum,
chargingstatus public.chargingstatus_enum,
powerconsumptionw numeric(10,4),
energyefficiencypercent numeric(6,3),
batterystatus jsonb
);


CREATE SEQUENCE public.powerbattery_powerbattregistry_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.powerbattery_powerbattregistry_seq OWNED BY public.powerbattery.powerbattregistry;


CREATE TABLE public.scientific (
sciregistry integer NOT NULL,
scieqref character varying(70) NOT NULL,
scientificequipmentstatus public.scientificequipmentstatus_enum,
dataloggingstatus public.dataloggingstatus_enum,
sensorstatus public.sensorstatus_enum,
calibrationstatus public.calibrationstatus_enum,
measurementaccuracypercent real
);


CREATE SEQUENCE public.scientific_sciregistry_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.scientific_sciregistry_seq OWNED BY public.scientific.sciregistry;


CREATE TABLE public.thermalsolarwindandgrid (
thermalregistry integer NOT NULL,
thermaleqref character varying(70) NOT NULL,
thermalcommref integer,
thermalimagingstatus public.thermalimagingstatus_enum,
insulationstatus public.insulationstatus_enum,
heatlossratekwh numeric(8,3),
solarpanelstatus public.solarpanelstatus_enum,
windturbinestatus public.windturbinestatus_enum,
powergridstatus public.powergridstatus_enum,
powerqualityindex numeric(5,2),
backuppowerstatus public.backuppowerstatus_enum,
fuelcellstatus public.fuelcellstatus_enum,
fuelcelloutputw double precision,
fuelcellefficiencypercent numeric(4,1),
hydrogenlevelpercent numeric(5,2),
oxygenlevelpercent numeric(5,2),
thermalpowerref integer,
renewablemetrics jsonb
);


CREATE SEQUENCE public.thermalsolarwindandgrid_thermalregistry_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.thermalsolarwindandgrid_thermalregistry_seq OWNED BY public.thermalsolarwindandgrid.thermalregistry;


CREATE TABLE public.transmission (
transregistry integer NOT NULL,
transeqref character varying(70) NOT NULL,
transengfluidsref integer,
transmissiontemperaturec real,
transmissionpressurekpa numeric(8,2),
transmissiongear smallint,
differentialtemperaturec double precision,
axletemperaturec double precision,
transopmaintref integer
);


CREATE SEQUENCE public.transmission_transregistry_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.transmission_transregistry_seq OWNED BY public.transmission.transregistry;


CREATE TABLE public.waterandwaste (
waterregistry integer NOT NULL,
watereqref character varying(70) NOT NULL,
waterlevelpercent real,
waterpressurekpa numeric(7,2),
watertemperaturec double precision,
waterflowlpm numeric(8,3),
waterqualityindex integer,
wastetanklevelpercent numeric(5,2)
);


CREATE SEQUENCE public.waterandwaste_waterregistry_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.waterandwaste_waterregistry_seq OWNED BY public.waterandwaste.waterregistry;


CREATE TABLE public.weatherandstructure (
weatherregistry integer NOT NULL,
weatherlocref integer NOT NULL,
externaltemperaturec real,
windspeedms double precision,
winddirectiondegrees numeric(5,1),
barometricpressurehpa numeric(7,2),
solarradiationwm2 numeric(8,3),
snowdepthcm smallint,
icethicknesscm numeric(5,2),
visibilitykm real,
precipitationtype public.precipitationtype_enum,
precipitationratemmh numeric(7,3),
snowloadkgm2 integer,
structuralloadpercent real,
structuralintegritystatus public.structuralintegritystatus_enum,
vibrationlevelmms2 double precision,
noiseleveldb numeric(7,2),
weatheropmaintref integer
);


CREATE SEQUENCE public.weatherandstructure_weatherregistry_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.weatherandstructure_weatherregistry_seq OWNED BY public.weatherandstructure.weatherregistry;


ALTER TABLE ONLY public.cabinenvironment ALTER COLUMN cabinregistry SET DEFAULT nextval('public.cabinenvironment_cabinregistry_seq'::regclass);


ALTER TABLE ONLY public.chassisandvehicle ALTER COLUMN chassisregistry SET DEFAULT nextval('public.chassisandvehicle_chassisregistry_seq'::regclass);


ALTER TABLE ONLY public.communication ALTER COLUMN commregistry SET DEFAULT nextval('public.communication_commregistry_seq'::regclass);


ALTER TABLE ONLY public.engineandfluids ALTER COLUMN engineregistry SET DEFAULT nextval('public.engineandfluids_engineregistry_seq'::regclass);


ALTER TABLE ONLY public.lightingandsafety ALTER COLUMN lightregistry SET DEFAULT nextval('public.lightingandsafety_lightregistry_seq'::regclass);


ALTER TABLE ONLY public.location ALTER COLUMN locationregistry SET DEFAULT nextval('public.location_locationregistry_seq'::regclass);


ALTER TABLE ONLY public.operationmaintenance ALTER COLUMN opmaintregistry SET DEFAULT nextval('public.operationmaintenance_opmaintregistry_seq'::regclass);


ALTER TABLE ONLY public.powerbattery ALTER COLUMN powerbattregistry SET DEFAULT nextval('public.powerbattery_powerbattregistry_seq'::regclass);


ALTER TABLE ONLY public.scientific ALTER COLUMN sciregistry SET DEFAULT nextval('public.scientific_sciregistry_seq'::regclass);


ALTER TABLE ONLY public.thermalsolarwindandgrid ALTER COLUMN thermalregistry SET DEFAULT nextval('public.thermalsolarwindandgrid_thermalregistry_seq'::regclass);


ALTER TABLE ONLY public.transmission ALTER COLUMN transregistry SET DEFAULT nextval('public.transmission_transregistry_seq'::regclass);


ALTER TABLE ONLY public.waterandwaste ALTER COLUMN waterregistry SET DEFAULT nextval('public.waterandwaste_waterregistry_seq'::regclass);


ALTER TABLE ONLY public.weatherandstructure ALTER COLUMN weatherregistry SET DEFAULT nextval('public.weatherandstructure_weatherregistry_seq'::regclass);
