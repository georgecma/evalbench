CREATE TYPE public.accesslimitation_enum AS ENUM (
'Severe',
'Partial'
);


CREATE TYPE public.auditstate_enum AS ENUM (
'Completed',
'Due',
'Overdue'
);


CREATE TYPE public.beneregister_enum AS ENUM (
'Complete',
'Pending',
'Partial'
);


CREATE TYPE public.commengagelvl_enum AS ENUM (
'High',
'Low',
'Medium'
);


CREATE TYPE public.commequipment_enum AS ENUM (
'Sufficient',
'Insufficient',
'Limited'
);


CREATE TYPE public.commnetstate_enum AS ENUM (
'Limited',
'Operational',
'Down'
);


CREATE TYPE public.compliancestate_enum AS ENUM (
'Partial',
'Compliant',
'Non-Compliant'
);


CREATE TYPE public.contingencyplanstage_enum AS ENUM (
'Overdue',
'Due',
'Updated'
);


CREATE TYPE public.coordeffectlvl_enum AS ENUM (
'Medium',
'Low',
'High'
);


CREATE TYPE public.damagereport_enum AS ENUM (
'Severe',
'Moderate',
'Minor',
'Catastrophic'
);


CREATE TYPE public.diseaserisk_enum AS ENUM (
'High',
'Medium',
'Low'
);


CREATE TYPE public.documentationstate_enum AS ENUM (
'Partial',
'Incomplete',
'Complete'
);


CREATE TYPE public.emerglevel_enum AS ENUM (
'Black',
'Orange',
'Red',
'Yellow'
);


CREATE TYPE public.envimpactrate_enum AS ENUM (
'Low',
'High',
'Medium'
);


CREATE TYPE public.evaluationstage_enum AS ENUM (
'Overdue',
'Due',
'Current'
);


CREATE TYPE public.fundingstate_enum AS ENUM (
'Critical',
'Adequate',
'Limited'
);


CREATE TYPE public.hazlevel_enum AS ENUM (
'Level 3',
'Level 5',
'Level 4',
'Level 1',
'Level 2'
);


CREATE TYPE public.haztype_enum AS ENUM (
'Wildfire',
'Earthquake',
'Tsunami',
'Flood',
'Hurricane'
);


CREATE TYPE public.infosharingstate_enum AS ENUM (
'Poor',
'Limited',
'Effective'
);


CREATE TYPE public.insurancescope_enum AS ENUM (
'Full',
'Partial'
);


CREATE TYPE public.lastmilestatus_enum AS ENUM (
'On Track',
'Delayed',
'Suspended'
);


CREATE TYPE public.lessonslearnedstage_enum AS ENUM (
'In Progress',
'Documented',
'Pending'
);


CREATE TYPE public.localcapacitygrowth_enum AS ENUM (
'Limited',
'Active'
);


CREATE TYPE public.maintenancestate_enum AS ENUM (
'Overdue',
'Up to Date',
'Due'
);


CREATE TYPE public.mediacoversentiment_enum AS ENUM (
'Positive',
'Neutral',
'Negative'
);


CREATE TYPE public.medicalemergencycapacity_enum AS ENUM (
'Adequate',
'Critical',
'Limited'
);


CREATE TYPE public.mentalhealthaid_enum AS ENUM (
'Limited',
'Available'
);


CREATE TYPE public.monitoringfreq_enum AS ENUM (
'Monthly',
'Daily',
'Weekly'
);


CREATE TYPE public.needsassessstatus_enum AS ENUM (
'Due',
'Overdue',
'Updated'
);


CREATE TYPE public.opsstatus_enum AS ENUM (
'Completed',
'Scaling Down',
'Active',
'Planning'
);


CREATE TYPE public.ppestatus_enum AS ENUM (
'Limited',
'Critical',
'Adequate'
);


CREATE TYPE public.priorityrank_enum AS ENUM (
'High',
'Medium',
'Low',
'Critical'
);


CREATE TYPE public.qualitycontrolsteps_enum AS ENUM (
'Moderate',
'Strong',
'Weak'
);


CREATE TYPE public.resourceallocstate_enum AS ENUM (
'Limited',
'Critical',
'Sufficient'
);


CREATE TYPE public.respphase_enum AS ENUM (
'Reconstruction',
'Recovery',
'Emergency',
'Initial'
);


CREATE TYPE public.riskmitigationsteps_enum AS ENUM (
'Insufficient',
'Partial',
'Adequate'
);


CREATE TYPE public.routeoptstatus_enum AS ENUM (
'In Progress',
'Optimized',
'Required'
);


CREATE TYPE public.safetyranking_enum AS ENUM (
'Safe',
'Moderate',
'High Risk'
);


CREATE TYPE public.supplyflowstate_enum AS ENUM (
'Disrupted',
'Stable',
'Strained'
);


CREATE TYPE public.trainingstate_enum AS ENUM (
'Complete',
'In Progress',
'Required'
);


CREATE TYPE public.transportaccess_enum AS ENUM (
'Full',
'Limited',
'Minimal'
);


CREATE TYPE public.vulnerabilityreview_enum AS ENUM (
'Complete',
'Pending',
'In Progress'
);


CREATE TYPE public.warehousestate_enum AS ENUM (
'Fair',
'Excellent',
'Good',
'Poor'
);


CREATE TYPE public.wastemanagementstate_enum AS ENUM (
'Adequate',
'Limited',
'Critical'
);


CREATE TABLE public.beneficiariesandassessments (
beneregistry character varying(20) NOT NULL,
benedistref character varying(20),
beneopsref character varying(20),
beneregister public.beneregister_enum,
vulnerabilityreview public.vulnerabilityreview_enum,
needsassessstatus public.needsassessstatus_enum,
distequityidx numeric(5,2),
benefeedbackscore numeric(5,2),
commengagelvl public.commengagelvl_enum,
localcapacitygrowth public.localcapacitygrowth_enum
);


CREATE TABLE public.coordinationandevaluation (
coordevalregistry character varying(20) NOT NULL,
coorddistref character varying(20),
coordopsref character varying(20),
secincidentcount integer,
safetyranking public.safetyranking_enum,
accesslimitation public.accesslimitation_enum,
coordeffectlvl public.coordeffectlvl_enum,
partnerorgs text,
infosharingstate public.infosharingstate_enum,
reportcompliance numeric(4,1),
dataqualityvalue integer,
monitoringfreq public.monitoringfreq_enum,
evaluationstage public.evaluationstage_enum,
lessonslearnedstage public.lessonslearnedstage_enum,
contingencyplanstage public.contingencyplanstage_enum,
riskmitigationsteps public.riskmitigationsteps_enum,
insurancescope public.insurancescope_enum,
compliancestate public.compliancestate_enum,
auditstate public.auditstate_enum,
qualitycontrolsteps public.qualitycontrolsteps_enum,
stakeholdersatisf numeric(3,2),
mediacoversentiment public.mediacoversentiment_enum,
publicperception numeric(5,1),
documentationstate public.documentationstate_enum,
lessonsrecorded text,
bestpracticeslisted text,
improvementrecs text,
nextreviewdate date,
notes text
);


CREATE TABLE public.disasterevents (
distregistry character varying(20) NOT NULL,
timemark timestamp without time zone NOT NULL,
haztype public.haztype_enum NOT NULL,
hazlevel public.hazlevel_enum,
affectedarea character varying(100),
regiontag character(10),
latcoord numeric(9,6),
loncoord numeric(10,7),
impactmetrics jsonb
);


CREATE TABLE public.distributionhubs (
hubregistry character varying(20) NOT NULL,
disteventref character varying(20),
hubcaptons numeric(11,2),
hubutilpct numeric(7,3),
storecapm3 numeric(9,2),
storeavailm3 numeric(8,3),
coldstorecapm3 numeric(10,3),
coldstoretempc numeric(4,1),
warehousestate public.warehousestate_enum,
invaccpct numeric(5,2),
stockturnrate numeric(5,2)
);


CREATE TABLE public.environmentandhealth (
envhealthregistry character varying(20) NOT NULL,
envdistref character varying(20),
envimpactrate public.envimpactrate_enum,
wastemanagementstate public.wastemanagementstate_enum,
recyclepct numeric(4,1),
carbontons numeric(10,3),
renewenergypct numeric(5,2),
waterqualityindex numeric(5,2),
sanitationcoverage numeric(7,3),
diseaserisk public.diseaserisk_enum,
medicalemergencycapacity public.medicalemergencycapacity_enum,
vaccinationcoverage numeric(6,3),
mentalhealthaid public.mentalhealthaid_enum
);


CREATE TABLE public.financials (
financeregistry character varying(20) NOT NULL,
findistref character varying(20),
finopsref character varying(20),
budgetallotusd numeric(16,3),
fundsutilpct numeric(7,3),
costbeneusd numeric(14,3),
opscostsusd numeric(15,2),
transportcostsusd integer,
storagecostsusd numeric(13,3),
personnelcostsusd numeric(15,4),
fundingstate public.fundingstate_enum,
donorcommitmentsusd numeric(14,2),
resourcegapsusd integer
);


CREATE TABLE public.humanresources (
hrregistry character varying(20) NOT NULL,
hrdistref character varying(20),
hropsref character varying(20),
staffingprofile jsonb
);


CREATE TABLE public.operations (
opsregistry character varying(20) NOT NULL,
opsdistref character varying(20),
opshubref character varying(20),
emerglevel public.emerglevel_enum,
respphase public.respphase_enum,
opsstatus public.opsstatus_enum,
coordcenter character varying(80),
opsstartdate date,
estdurationdays integer,
priorityrank public.priorityrank_enum,
resourceallocstate public.resourceallocstate_enum,
supplyflowstate public.supplyflowstate_enum
);


CREATE TABLE public.supplies (
supplyregistry character varying(20) NOT NULL,
supplydistref character varying(20),
supplyhubref character varying(20),
resourceinventory jsonb
);


CREATE TABLE public.transportation (
transportregistry character varying(20) NOT NULL,
transportdistref character varying(20),
transporthubref character varying(20),
transportsupref character varying(20),
vehiclecount integer,
trucksavailable integer,
helosavailable integer,
boatsavailable bigint,
totaldeliverytons numeric(9,3),
dailydeliverytons numeric(8,2),
lastmilestatus public.lastmilestatus_enum,
distributionpoints integer,
avgdeliveryhours numeric(5,2),
deliverysuccessrate numeric(7,3),
routeoptstatus public.routeoptstatus_enum,
fuelefficiencylpk numeric(6,3),
maintenancestate public.maintenancestate_enum,
vehiclebreakrate numeric(4,1)
);
