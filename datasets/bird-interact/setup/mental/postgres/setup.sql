CREATE TYPE public.adlfunctioning_enum AS ENUM (
'Minimal Help',
'Independent',
'Moderate Help',
'Dependent'
);


CREATE TYPE public.assessmentlanguage_enum AS ENUM (
'Chinese',
'French',
'Spanish',
'English',
'Other'
);


CREATE TYPE public.assessmentlimitations_enum AS ENUM (
'Cognitive',
'Engagement',
'Language'
);


CREATE TYPE public.assessmentmethod_enum AS ENUM (
'Phone',
'Self-report',
'In-person',
'Telehealth'
);


CREATE TYPE public.assessmenttype_enum AS ENUM (
'Initial',
'Emergency',
'Routine',
'Follow-up'
);


CREATE TYPE public.assessmentvalidity_enum AS ENUM (
'Questionable',
'Invalid',
'Valid'
);


CREATE TYPE public.carecoordination_enum AS ENUM (
'Intensive',
'Regular',
'Limited'
);


CREATE TYPE public.clinicianconfidence_enum AS ENUM (
'Medium',
'Low',
'High'
);


CREATE TYPE public.communityresources_enum AS ENUM (
'Limited',
'Comprehensive',
'Adequate'
);


CREATE TYPE public.copingskills_enum AS ENUM (
'Good',
'Poor',
'Fair',
'Limited'
);


CREATE TYPE public.crisisplanstatus_enum AS ENUM (
'Not Needed',
'Needs Update',
'In Place'
);


CREATE TYPE public.culturalfactors_enum AS ENUM (
'Language',
'Beliefs',
'Family',
'Multiple'
);


CREATE TYPE public.disabilitystatus_enum AS ENUM (
'Pending',
'Permanent',
'Temporary'
);


CREATE TYPE public.documentationstatus_enum AS ENUM (
'Complete',
'Incomplete',
'Pending'
);


CREATE TYPE public.educationlevel_enum AS ENUM (
'High School',
'Other',
'Bachelor',
'Master',
'Doctorate'
);


CREATE TYPE public.employmentstatus_enum AS ENUM (
'Retired',
'Employed',
'Unemployed',
'Disabled',
'Student'
);


CREATE TYPE public.environmentalstressors_enum AS ENUM (
'Mild',
'Moderate',
'Severe'
);


CREATE TYPE public.familyinvolvement_enum AS ENUM (
'Low',
'High',
'Medium'
);


CREATE TYPE public.financialstress_enum AS ENUM (
'Severe',
'Mild',
'Moderate'
);


CREATE TYPE public.followupfrequency_enum AS ENUM (
'Weekly',
'Quarterly',
'Biweekly',
'Monthly'
);


CREATE TYPE public.followuptype_enum AS ENUM (
'Therapy',
'Routine',
'Urgent',
'Medication Check'
);


CREATE TYPE public.functionalimpairment_enum AS ENUM (
'Severe',
'Moderate',
'Mild'
);


CREATE TYPE public.functionalimprovement_enum AS ENUM (
'Moderate',
'Minimal',
'Significant'
);


CREATE TYPE public.gad7severity_enum AS ENUM (
'Mild',
'Moderate',
'Severe'
);


CREATE TYPE public.housingstability_enum AS ENUM (
'Homeless',
'Stable',
'At Risk',
'Unstable'
);


CREATE TYPE public.insightlevel_enum AS ENUM (
'Fair',
'Good',
'Poor'
);


CREATE TYPE public.insurancestatus_enum AS ENUM (
'Pending',
'Approved',
'Denied'
);


CREATE TYPE public.insurancetype_enum AS ENUM (
'Medicaid',
'Medicare',
'Private'
);


CREATE TYPE public.legalissues_enum AS ENUM (
'Resolved',
'Pending',
'Ongoing'
);


CREATE TYPE public.lifeeventsimpact_enum AS ENUM (
'Mild',
'Moderate',
'Severe'
);


CREATE TYPE public.livingarrangement_enum AS ENUM (
'Alone',
'Partner',
'Family',
'Group Home',
'Homeless'
);


CREATE TYPE public.maritalstatus_enum AS ENUM (
'Widowed',
'Married',
'Single',
'Divorced'
);


CREATE TYPE public.medicationadherence_enum AS ENUM (
'Medium',
'Low',
'Non-compliant',
'High'
);


CREATE TYPE public.medicationchanges_enum AS ENUM (
'Dose Adjustment',
'Augmentation',
'Switch'
);


CREATE TYPE public.medicationsideeffects_enum AS ENUM (
'Mild',
'Moderate',
'Severe'
);


CREATE TYPE public.motivationlevel_enum AS ENUM (
'High',
'Low',
'Medium'
);


CREATE TYPE public.patientethnicity_enum AS ENUM (
'Other',
'Hispanic',
'African',
'Asian',
'Caucasian'
);


CREATE TYPE public.patientgender_enum AS ENUM (
'Other',
'F',
'M'
);


CREATE TYPE public.phq9severity_enum AS ENUM (
'Moderately Severe',
'Mild',
'Severe',
'Moderate'
);


CREATE TYPE public.primarydx_enum AS ENUM (
'Anxiety',
'PTSD',
'Bipolar',
'Schizophrenia',
'Depression'
);


CREATE TYPE public.recoverygoalsstatus_enum AS ENUM (
'Not Started',
'Achieved',
'In Progress',
'Modified'
);


CREATE TYPE public.recoverystatus_enum AS ENUM (
'Relapse',
'Stable',
'Advanced',
'Early'
);


CREATE TYPE public.referralneeds_enum AS ENUM (
'Services',
'Testing',
'Specialist'
);


CREATE TYPE public.referralsource_enum AS ENUM (
'Self',
'Court',
'Physician',
'Emergency',
'Family'
);


CREATE TYPE public.relationshipquality_enum AS ENUM (
'Poor',
'Conflicted',
'Good',
'Fair'
);


CREATE TYPE public.responseconsistency_enum AS ENUM (
'Medium',
'High',
'Low'
);


CREATE TYPE public.safetyplanstatus_enum AS ENUM (
'Needs Update',
'In Place',
'Not Needed'
);


CREATE TYPE public.seasonalpattern_enum AS ENUM (
'Summer',
'Winter',
'Variable'
);


CREATE TYPE public.secondarydx_enum AS ENUM (
'OCD',
'Personality Disorder',
'Substance Use',
'Eating Disorder'
);


CREATE TYPE public.selfharm_enum AS ENUM (
'Recent',
'Past',
'Current'
);


CREATE TYPE public.sideeffectburden_enum AS ENUM (
'Mild',
'Moderate',
'Severe'
);


CREATE TYPE public.socialfunctioning_enum AS ENUM (
'Isolated',
'Fair',
'Good',
'Poor'
);


CREATE TYPE public.socialsupportlevel_enum AS ENUM (
'Strong',
'Limited',
'Moderate'
);


CREATE TYPE public.stigmaimpact_enum AS ENUM (
'Moderate',
'Mild',
'Severe'
);


CREATE TYPE public.substanceuse_enum AS ENUM (
'Cannabis',
'Opioids',
'Alcohol',
'Multiple'
);


CREATE TYPE public.substanceusefrequency_enum AS ENUM (
'Daily',
'Never',
'Occasional',
'Regular'
);


CREATE TYPE public.substanceuseseverity_enum AS ENUM (
'Mild',
'Moderate',
'Severe'
);


CREATE TYPE public.suicidalideation_enum AS ENUM (
'Intent',
'Active',
'Plan',
'Passive'
);


CREATE TYPE public.suiciderisk_enum AS ENUM (
'Medium',
'High',
'Low',
'Severe'
);


CREATE TYPE public.supportsystemchanges_enum AS ENUM (
'Variable',
'Improved',
'Declined'
);


CREATE TYPE public.symptomimprovement_enum AS ENUM (
'Moderate',
'Minimal',
'Significant'
);


CREATE TYPE public.symptomvalidity_enum AS ENUM (
'Questionable',
'Valid',
'Invalid'
);


CREATE TYPE public.therapeuticalliance_enum AS ENUM (
'Moderate',
'Poor',
'Strong',
'Weak'
);


CREATE TYPE public.therapychanges_enum AS ENUM (
'Frequency Change',
'Modality Change',
'Therapist Change'
);


CREATE TYPE public.therapyengagement_enum AS ENUM (
'Medium',
'High',
'Low',
'Non-compliant'
);


CREATE TYPE public.therapyfrequency_enum AS ENUM (
'Biweekly',
'Monthly',
'Weekly'
);


CREATE TYPE public.therapyprogress_enum AS ENUM (
'Fair',
'Good',
'Poor'
);


CREATE TYPE public.therapytype_enum AS ENUM (
'DBT',
'Group',
'Psychodynamic',
'CBT'
);


CREATE TYPE public.treatmentadherence_enum AS ENUM (
'Non-compliant',
'Medium',
'Low',
'High'
);


CREATE TYPE public.treatmentbarriers_enum AS ENUM (
'Multiple',
'Time',
'Financial',
'Transportation'
);


CREATE TYPE public.treatmentengagement_enum AS ENUM (
'Non-compliant',
'High',
'Medium',
'Low'
);


CREATE TYPE public.treatmentgoalsstatus_enum AS ENUM (
'Not Started',
'Achieved',
'In Progress',
'Modified'
);


CREATE TYPE public.treatmentresponse_enum AS ENUM (
'Poor',
'Good',
'Partial'
);


CREATE TYPE public.treatmentsatisfaction_enum AS ENUM (
'Medium',
'Dissatisfied',
'Low',
'High'
);


CREATE TYPE public.violencerisk_enum AS ENUM (
'Medium',
'Low',
'High'
);


CREATE TYPE public.workfunctioning_enum AS ENUM (
'Disabled',
'Poor',
'Fair',
'Good'
);


CREATE TYPE public.workstatuschanges_enum AS ENUM (
'Leave',
'Reduced Hours',
'Terminated'
);


CREATE TABLE public.assessmentbasics (
abkey character varying(30) NOT NULL,
atype public.assessmenttype_enum,
amethod public.assessmentmethod_enum,
adurmin smallint,
alang public.assessmentlanguage_enum,
avalid public.assessmentvalidity_enum,
respconsist public.responseconsistency_enum,
symptvalid public.symptomvalidity_enum,
patownerref character varying(20)
);


CREATE TABLE public.assessmentsocialanddiagnosis (
asdkey character varying(30) NOT NULL,
recstatus public.recoverystatus_enum,
socsup public.socialsupportlevel_enum,
faminv public.familyinvolvement_enum,
relqual public.relationshipquality_enum,
workfunc public.workfunctioning_enum,
socfunc public.socialfunctioning_enum,
adlfunc public.adlfunctioning_enum,
strslvl numeric(3,1),
copskill public.copingskills_enum,
resscr numeric(3,1),
inlevel public.insightlevel_enum,
motivlevel public.motivationlevel_enum,
primdx public.primarydx_enum,
secdx public.secondarydx_enum,
dxdurm smallint,
prevhosp smallint,
lasthospdt date,
qolscr smallint,
funcimp public.functionalimpairment_enum
);


CREATE TABLE public.assessmentsymptomsandrisk (
asrkey character varying(30) NOT NULL,
suicideation public.suicidalideation_enum,
suicrisk public.suiciderisk_enum,
selfharm public.selfharm_enum,
violrisk public.violencerisk_enum,
subuse public.substanceuse_enum,
subusefreq public.substanceusefrequency_enum,
subusesev public.substanceuseseverity_enum,
mental_health_scores jsonb
);


CREATE TABLE public.clinicians (
clinkey character varying(20) NOT NULL,
clinconf public.clinicianconfidence_enum,
assesslim public.assessmentlimitations_enum,
docustat public.documentationstatus_enum,
billcode character varying(15),
nxtrevdt date,
carecoord public.carecoordination_enum,
refneed public.referralneeds_enum,
fuptype public.followuptype_enum,
fupfreq public.followupfrequency_enum,
facconnect character varying(20)
);


CREATE TABLE public.encounters (
enckey character varying(30) NOT NULL,
timemark timestamp without time zone NOT NULL,
abref character varying(30) NOT NULL,
patref character varying(20) NOT NULL,
clinid character varying(20),
facid character varying(20),
missappt numeric(2,1),
txbarrier public.treatmentbarriers_enum,
nxapptdt date,
dqscore smallint,
assesscomplete character varying(250)
);


CREATE TABLE public.facilities (
fackey character varying(20) NOT NULL,
rsource public.referralsource_enum,
envstress public.environmentalstressors_enum,
lifeimpact public.lifeeventsimpact_enum,
seasonpat public.seasonalpattern_enum,
leglissue public.legalissues_enum,
ssystemchg public.supportsystemchanges_enum,
support_and_resources jsonb
);


CREATE TABLE public.patients (
patkey character varying(20) NOT NULL,
patage smallint,
patgender public.patientgender_enum,
pateth public.patientethnicity_enum,
edulevel public.educationlevel_enum,
empstat public.employmentstatus_enum,
maristat public.maritalstatus_enum,
livingarr public.livingarrangement_enum,
insurtype public.insurancetype_enum,
insurstat public.insurancestatus_enum,
disabstat public.disabilitystatus_enum,
housestable public.housingstability_enum,
cultfactor public.culturalfactors_enum,
stigmaimp public.stigmaimpact_enum,
finstress public.financialstress_enum,
clinleadref character varying(20)
);


CREATE TABLE public.treatmentbasics (
txkey integer NOT NULL,
encref character varying(30) NOT NULL,
curmed text,
medadh public.medicationadherence_enum,
medside public.medicationsideeffects_enum,
medchg public.medicationchanges_enum,
crisisint numeric(2,1),
therapy_details jsonb
);


CREATE SEQUENCE public.treatmentbasics_txkey_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.treatmentbasics_txkey_seq OWNED BY public.treatmentbasics.txkey;


CREATE TABLE public.treatmentoutcomes (
txoutkey integer NOT NULL,
txref integer NOT NULL,
thprog public.therapyprogress_enum,
txadh public.treatmentadherence_enum,
txresp public.treatmentresponse_enum,
sideburd public.sideeffectburden_enum,
txgoalstat public.treatmentgoalsstatus_enum,
recgoalstat public.recoverygoalsstatus_enum,
sympimp public.symptomimprovement_enum,
funcimpv public.functionalimprovement_enum,
workstatchg public.workstatuschanges_enum,
satscr numeric(3,1),
theralliance public.therapeuticalliance_enum,
txeng public.treatmentengagement_enum,
txsat public.treatmentsatisfaction_enum
);


CREATE SEQUENCE public.treatmentoutcomes_txoutkey_seq
AS integer
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.treatmentoutcomes_txoutkey_seq OWNED BY public.treatmentoutcomes.txoutkey;


ALTER TABLE ONLY public.treatmentbasics ALTER COLUMN txkey SET DEFAULT nextval('public.treatmentbasics_txkey_seq'::regclass);


ALTER TABLE ONLY public.treatmentoutcomes ALTER COLUMN txoutkey SET DEFAULT nextval('public.treatmentoutcomes_txoutkey_seq'::regclass);
