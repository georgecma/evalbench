CREATE TYPE public.accountrestrictionlevel_enum AS ENUM (
'Full',
'Partial'
);


CREATE TYPE public.actiontaken_enum AS ENUM (
'Termination',
'Warning',
'Restriction'
);


CREATE TYPE public.alertcategory_enum AS ENUM (
'Pattern',
'Transaction',
'Behavior',
'Security'
);


CREATE TYPE public.alertsev_enum AS ENUM (
'Low',
'Medium',
'High',
'Critical'
);


CREATE TYPE public.anonlevel_enum AS ENUM (
'Low',
'Medium',
'High'
);


CREATE TYPE public.authenticationmethod_enum AS ENUM (
'Basic',
'2FA',
'Multi-factor'
);


CREATE TYPE public.buychecklvl_enum AS ENUM (
'Advanced',
'Basic'
);


CREATE TYPE public.buyfreqcat_enum AS ENUM (
'Heavy',
'Regular',
'One-time',
'Occasional'
);


CREATE TYPE public.buyspending_enum AS ENUM (
'Variable',
'High',
'Low',
'Medium'
);


CREATE TYPE public.casestatus_enum AS ENUM (
'New',
'In Progress',
'Resolved',
'Closed'
);


CREATE TYPE public.commchannel_enum AS ENUM (
'Mixed',
'External',
'Internal'
);


CREATE TYPE public.commfreq_enum AS ENUM (
'Low',
'Medium',
'High'
);


CREATE TYPE public.crossborderflag_enum AS ENUM (
'Yes',
'No'
);


CREATE TYPE public.dataprotectionlevel_enum AS ENUM (
'Maximum',
'Enhanced',
'Basic'
);


CREATE TYPE public.dataretentionstatus_enum AS ENUM (
'Deleted',
'Active',
'Archived'
);


CREATE TYPE public.encryptionstrength_enum AS ENUM (
'Strong',
'Military-grade',
'Standard'
);


CREATE TYPE public.encryptmethod_enum AS ENUM (
'Custom',
'Standard',
'Enhanced'
);


CREATE TYPE public.escalationlevel_enum AS ENUM (
'Level1',
'Level2',
'Level3'
);


CREATE TYPE public.escrowused_enum AS ENUM (
'Yes',
'No'
);


CREATE TYPE public.followuprequired_enum AS ENUM (
'Yes',
'No'
);


CREATE TYPE public.investpriority_enum AS ENUM (
'Low',
'Medium',
'High'
);


CREATE TYPE public.investstat_enum AS ENUM (
'Monitoring',
'Closed',
'Active'
);


CREATE TYPE public.langpattern_enum AS ENUM (
'Variable',
'Suspicious',
'Consistent'
);


CREATE TYPE public.marketstatus_enum AS ENUM (
'Active',
'Under Investigation',
'Suspended',
'Closed'
);


CREATE TYPE public.mktclass_enum AS ENUM (
'Forum',
'Service',
'Marketplace',
'Exchange'
);


CREATE TYPE public.multisigflag_enum AS ENUM (
'Yes',
'No'
);


CREATE TYPE public.paymethod_enum AS ENUM (
'Crypto_A',
'Crypto_B',
'Crypto_C',
'Token'
);


CREATE TYPE public.prodsubcat_enum AS ENUM (
'Type_A',
'Type_B',
'Type_C',
'Type_D'
);


CREATE TYPE public.prodtheme_enum AS ENUM (
'Digital',
'Data',
'Service',
'Physical'
);


CREATE TYPE public.reviewfrequency_enum AS ENUM (
'Weekly',
'Monthly',
'Daily'
);


CREATE TYPE public.risklevel_enum AS ENUM (
'Low',
'Medium',
'High',
'Unknown'
);


CREATE TYPE public.routecomplexity_enum AS ENUM (
'Complex',
'Medium',
'Simple'
);


CREATE TYPE public.securityauditstatus_enum AS ENUM (
'Warning',
'Pass',
'Fail'
);


CREATE TYPE public.shipmethod_enum AS ENUM (
'Express',
'Standard',
'Custom',
'Digital'
);


CREATE TYPE public.shipregiondst_enum AS ENUM (
'Region_X',
'Region_Y',
'Region_Z',
'Unknown'
);


CREATE TYPE public.shipregionsrc_enum AS ENUM (
'Region_A',
'Region_B',
'Region_C',
'Unknown'
);


CREATE TYPE public.sizecluster_enum AS ENUM (
'Mega',
'Medium',
'Large',
'Small'
);


CREATE TYPE public.txpatterncat_enum AS ENUM (
'High-risk',
'Suspicious',
'Normal'
);


CREATE TYPE public.txstatus_enum AS ENUM (
'Pending',
'Cancelled',
'Completed',
'Disputed'
);


CREATE TYPE public.vendchecklvl_enum AS ENUM (
'Basic',
'Advanced',
'Premium'
);


CREATE TYPE public.vpnflag_enum AS ENUM (
'No',
'Suspected',
'Yes'
);


CREATE TABLE public.buyers (
buyregistry character varying(30) NOT NULL,
buyspan integer,
buytxtally smallint,
buyspending public.buyspending_enum,
buyfreqcat public.buyfreqcat_enum,
buychecklvl public.buychecklvl_enum,
buyriskrate numeric(5,2),
mktref character varying(30),
vendref character varying(30)
);


CREATE TABLE public.communication (
commregistry character varying(30) NOT NULL,
iptally smallint,
tornodecount smallint,
vpnflag public.vpnflag_enum,
brwsrunique numeric(6,3),
devfpscore numeric(6,3),
connpatscore numeric(5,2),
encryptmethod public.encryptmethod_enum,
commchannel public.commchannel_enum,
msgtally smallint,
commfreq public.commfreq_enum,
langpattern public.langpattern_enum,
sentiscore numeric(5,3),
keymatchcount smallint,
susppatscore numeric(5,2),
riskindiccount smallint,
txref character varying(30),
prodref character varying(30)
);


CREATE TABLE public.investigation (
investregistry character varying(30) NOT NULL,
investstat public.investstat_enum,
lawinterest public.risklevel_enum,
regrisklvl public.risklevel_enum,
compliancescore numeric(4,2),
investpriority public.investpriority_enum,
resptimemins integer,
escalationlevel public.escalationlevel_enum,
casestatus public.casestatus_enum,
resolutiontimehours smallint,
actiontaken public.actiontaken_enum,
followuprequired public.followuprequired_enum,
reviewfrequency public.reviewfrequency_enum,
nextreviewdate date,
notescount smallint,
dataretentionstatus public.dataretentionstatus_enum,
lastupdated timestamp without time zone,
updatefrequencyhours integer,
secref character varying(30),
riskref character varying(30)
);


CREATE TABLE public.markets (
mktregistry character varying(30) NOT NULL,
mktdenom character varying(80),
mktclass public.mktclass_enum,
mktspan integer,
sizecluster public.sizecluster_enum,
dlyflow bigint,
mthactive bigint,
vendcount integer,
buycount integer,
listtotal bigint,
interscore numeric(6,3),
esccomprate numeric(5,3),
market_status_reputation jsonb
);


CREATE TABLE public.products (
prodregistry character varying(30) NOT NULL,
prodtheme public.prodtheme_enum,
prodsubcat public.prodsubcat_enum,
prodlistdays integer,
prodpriceusd numeric(10,2),
prodqty integer,
vendref character varying(30),
buyref character varying(30)
);


CREATE TABLE public.riskanalysis (
riskregistry character varying(30) NOT NULL,
fraudprob numeric(5,3),
moneyrisk public.risklevel_enum,
linkedtxcount smallint,
txchainlen smallint,
wallrisksc numeric(5,2),
wallage integer,
wallbalusd numeric(15,2),
wallturnrt numeric(5,3),
txvel numeric(6,2),
profilecomplete numeric(4,1),
idverifyscore numeric(4,1),
feedbackauthscore numeric(4,1),
commref character varying(30),
txref character varying(30),
network_behavior_analytics jsonb
);


CREATE TABLE public.securitymonitoring (
secmonregistry character varying(30) NOT NULL,
securityauditstatus public.securityauditstatus_enum,
vulntally smallint,
inctally smallint,
securitymeasurecount smallint,
encryptionstrength public.encryptionstrength_enum,
authenticationmethod public.authenticationmethod_enum,
sessionsecurityscore numeric(5,2),
dataprotectionlevel public.dataprotectionlevel_enum,
privprotscore numeric(5,2),
operationalsecurityscore numeric(5,2),
fpprob numeric(5,4),
alertsev public.alertsev_enum,
alertcategory public.alertcategory_enum,
alertconfidencescore numeric(4,2),
riskref character varying(30),
commref character varying(30),
threat_analysis_metrics jsonb
);


CREATE TABLE public.transactions (
txregistry character varying(30) NOT NULL,
rectag character varying(30),
eventstamp timestamp without time zone,
paymethod public.paymethod_enum,
payamtusd numeric(14,2),
txfeeusd numeric(10,2),
escrowused public.escrowused_enum,
escrowhrs smallint,
multisigflag public.multisigflag_enum,
txstatus public.txstatus_enum,
txfinishhrs numeric(5,2),
shipmethod public.shipmethod_enum,
shipregionsrc public.shipregionsrc_enum,
shipregiondst public.shipregiondst_enum,
crossborderflag public.crossborderflag_enum,
routecomplexity public.routecomplexity_enum,
mktref character varying(30),
prodref character varying(30),
buyref character varying(30)
);


CREATE TABLE public.vendors (
vendregistry character varying(30) NOT NULL,
vendspan integer,
vendrate numeric(4,2),
vendtxcount integer,
vendsucccount integer,
venddisputecount integer,
vendplacecount smallint,
vendpaymethods smallint,
vendchecklvl public.vendchecklvl_enum,
vendlastmoment date,
mktref character varying(30)
);
