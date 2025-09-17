CREATE TYPE public.additionalincomesource_enum AS ENUM (
'Investment',
'Rental',
'Part-time'
);


CREATE TYPE public.amlscreeningresult_enum AS ENUM (
'Flag',
'Pass',
'Fail'
);


CREATE TYPE public.banktransactionfrequency_enum AS ENUM (
'Low',
'Medium',
'High'
);


CREATE TYPE public.complainthistory_enum AS ENUM (
'Low',
'High',
'Medium'
);


CREATE TYPE public.creditseekingbehavior_enum AS ENUM (
'High',
'Low',
'Medium'
);


CREATE TYPE public.customersegment_enum AS ENUM (
'Premium',
'Standard',
'Basic'
);


CREATE TYPE public.decisionstatus_enum AS ENUM (
'Pending',
'Rejected',
'Approved'
);


CREATE TYPE public.documentverificationstatus_enum AS ENUM (
'Pending',
'Verified',
'Failed'
);


CREATE TYPE public.educationlevel_enum AS ENUM (
'Doctorate',
'High School',
'Master',
'Bachelor'
);


CREATE TYPE public.employmentstatus_enum AS ENUM (
'Self-employed',
'Employed',
'Unemployed',
'Retired'
);


CREATE TYPE public.gender_enum AS ENUM (
'M',
'F'
);


CREATE TYPE public.healthinsurancestatus_enum AS ENUM (
'Basic',
'Premium'
);


CREATE TYPE public.incomeverification_enum AS ENUM (
'Pending',
'Verified',
'Failed'
);


CREATE TYPE public.industrysector_enum AS ENUM (
'Education',
'Other',
'Technology',
'Healthcare',
'Finance'
);


CREATE TYPE public.insurancecoverage_enum AS ENUM (
'Comprehensive',
'Basic'
);


CREATE TYPE public.investmentexperience_enum AS ENUM (
'Extensive',
'Moderate',
'Limited'
);


CREATE TYPE public.investmentportfolio_enum AS ENUM (
'Conservative',
'Moderate',
'Aggressive'
);


CREATE TYPE public.jobtitle_enum AS ENUM (
'Manager',
'Teacher',
'Doctor',
'Other',
'Engineer'
);


CREATE TYPE public.kycstatus_enum AS ENUM (
'Pending',
'Failed',
'Completed'
);


CREATE TYPE public.legalstatus_enum AS ENUM (
'Clear',
'Under Review',
'Restricted'
);


CREATE TYPE public.maritalstatus_enum AS ENUM (
'Single',
'Married',
'Widowed',
'Divorced'
);


CREATE TYPE public.mobilebankingusage_enum AS ENUM (
'Medium',
'High',
'Low'
);


CREATE TYPE public.onlinebankingusage_enum AS ENUM (
'High',
'Low',
'Medium'
);


CREATE TYPE public.overdraftfrequency_enum AS ENUM (
'Frequent',
'Occasional',
'Rare',
'Never'
);


CREATE TYPE public.overridereason_enum AS ENUM (
'Policy Exception',
'Management Decision'
);


CREATE TYPE public.overridestatus_enum AS ENUM (
'Policy',
'Manual'
);


CREATE TYPE public.paymenthistory_enum AS ENUM (
'Poor',
'Fair',
'Good',
'Excellent',
'Current',
'Past'
);


CREATE TYPE public.pepscreeningresult_enum AS ENUM (
'Flag',
'Pass',
'Fail'
);


CREATE TYPE public.propertyownership_enum AS ENUM (
'Rent',
'Living with Parents',
'Own'
);


CREATE TYPE public.propertytype_enum AS ENUM (
'Apartment',
'House',
'Condo'
);


CREATE TYPE public.recentcreditbehavior_enum AS ENUM (
'Stable',
'Improving',
'Deteriorating'
);


CREATE TYPE public.regulatorycompliance_enum AS ENUM (
'Non-compliant',
'Compliant'
);


CREATE TYPE public.residentialstatus_enum AS ENUM (
'Temporary',
'Permanent',
'Foreign'
);


CREATE TYPE public.risk3_enum AS ENUM (
'Low',
'Medium',
'High'
);


CREATE TYPE public.risklevel_enum AS ENUM (
'Low',
'Medium',
'High',
'Very High'
);


CREATE TYPE public.sanctionsscreeningresult_enum AS ENUM (
'Fail',
'Flag',
'Pass'
);


CREATE TYPE public.tradingactivity_enum AS ENUM (
'Medium',
'Low',
'High'
);


CREATE TYPE public.vehicleownership_enum AS ENUM (
'Lease',
'Own'
);


CREATE TYPE public.yesno_enum AS ENUM (
'Yes',
'No'
);


CREATE TABLE public.bank_and_transactions (
bankexpref character varying(20) NOT NULL,
banktxfreq public.banktransactionfrequency_enum,
banktxamt numeric(14,2),
bankrelscore numeric(4,3),
ovrfreq public.overdraftfrequency_enum,
bouncecount smallint,
inscoverage public.insurancecoverage_enum,
lifeinsval numeric(14,2),
hlthinsstat public.healthinsurancestatus_enum,
fraudrisk numeric(5,3),
idverscore numeric(5,3),
docverstat public.documentverificationstatus_enum,
kycstat public.kycstatus_enum,
amlresult public.amlscreeningresult_enum,
chaninvdatablock jsonb
);


CREATE TABLE public.core_record (
coreregistry character varying(20) NOT NULL,
timemark timestamp(6) without time zone,
clientref character varying(20),
appref character varying(20),
modelline character varying(10),
scoredate date,
nextcheck date,
dataqscore numeric(5,3),
confscore numeric(5,3),
overridestat public.overridestatus_enum,
overridenote public.overridereason_enum,
decidestat public.decisionstatus_enum,
decidedate date,
agespan smallint,
gendlabel public.gender_enum,
maritalform public.maritalstatus_enum,
depcount smallint,
resdform public.residentialstatus_enum,
addrstab smallint,
phonestab smallint,
emailstab character varying(50),
clientseg public.customersegment_enum,
tenureyrs smallint,
crossratio numeric(4,3),
profitscore numeric(4,3),
churnrate numeric(4,3)
);


CREATE TABLE public.credit_accounts_and_history (
histcompref character varying(20) NOT NULL,
newaccage smallint,
avgaccage numeric(4,1),
accmixscore numeric(4,3),
credlimusage numeric(4,3),
payconsist numeric(4,3),
recentbeh public.recentcreditbehavior_enum,
seekbeh public.creditseekingbehavior_enum,
cardcount smallint,
totcredlimit numeric(14,2),
credutil numeric(5,3),
cardpayhist public.paymenthistory_enum,
loancount smallint,
activeloan smallint,
totloanamt bigint,
loanpayhist public.paymenthistory_enum,
custservint smallint,
complainthist public.complainthistory_enum,
produsescore numeric(5,3),
chanusescore numeric(5,3),
custlifeval numeric(14,2)
);


CREATE TABLE public.credit_and_compliance (
compbankref character varying(20) NOT NULL,
sancresult public.sanctionsscreeningresult_enum,
pepresult public.pepscreeningresult_enum,
legalstat public.legalstatus_enum,
regcompliance public.regulatorycompliance_enum,
credscore smallint,
risklev public.risklevel_enum,
defhist public.paymenthistory_enum,
delinqcount smallint,
latepaycount smallint,
collacc integer,
choffs smallint,
bankr smallint,
taxlien smallint,
civiljudge smallint,
credinq smallint,
hardinq smallint,
softinq smallint,
credrepdisp character varying(50),
credageyrs smallint,
oldaccage smallint
);


CREATE TABLE public.employment_and_income (
emplcoreref character varying(20) NOT NULL,
emplstat public.employmentstatus_enum,
empllen smallint,
joblabel public.jobtitle_enum,
indsector public.industrysector_enum,
employerref text,
annlincome numeric(12,2),
mthincome numeric(12,2),
incverify public.incomeverification_enum,
incstabscore real,
addincome numeric(12,2),
addincomesrc public.additionalincomesource_enum,
hshincome numeric(12,2),
emplstable smallint,
indrisklvl public.risk3_enum,
occrisklvl public.risk3_enum,
incsrcrisk public.risk3_enum,
georisk public.risk3_enum,
demrisk public.risk3_enum,
edulevel public.educationlevel_enum,
debincratio numeric(5,3)
);


CREATE TABLE public.expenses_and_assets (
expemplref character varying(20) NOT NULL,
mthexp numeric(14,2),
fixexpratio numeric(5,4),
discexpratio numeric(5,4),
savamount numeric(14,2),
investamt numeric(14,2),
liqassets numeric(15,2),
totassets numeric(15,2),
totliabs numeric(15,2),
networth numeric(15,2),
vehown public.vehicleownership_enum,
vehvalue numeric(15,2),
bankacccount smallint,
bankaccage smallint,
bankaccbal numeric(14,2),
propfinancialdata jsonb
);
