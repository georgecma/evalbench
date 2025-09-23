CREATE TYPE public.accountstatus_enum AS ENUM (
'Active',
'Deleted',
'Suspended',
'Dormant'
);


CREATE TYPE public.accounttype_enum AS ENUM (
'Personal',
'Bot',
'Hybrid',
'Business'
);


CREATE TYPE public.actiontaken_enum AS ENUM (
'Suspension',
'Warning',
'Restriction'
);


CREATE TYPE public.appealcount_enum AS ENUM (
'0',
'1',
'2',
'3',
'4',
'5'
);


CREATE TYPE public.biokeywordmatch_enum AS ENUM (
'Suspicious',
'Normal',
'Spam',
'Promo'
);


CREATE TYPE public.biolanguage_enum AS ENUM (
'en',
'multiple',
'mixed',
'unknown'
);


CREATE TYPE public.biolinkcount_enum AS ENUM (
'0',
'1',
'2',
'3',
'4',
'5'
);


CREATE TYPE public.clusterrole_enum AS ENUM (
'Isolated',
'Follower',
'Leader',
'Amplifier'
);


CREATE TYPE public.connectiongrowthpattern_enum AS ENUM (
'Suspicious',
'Burst',
'Bot-like',
'Organic'
);


CREATE TYPE public.contentlanguagecount_enum AS ENUM (
'1',
'2',
'3',
'4',
'5'
);


CREATE TYPE public.detectionsource_enum AS ENUM (
'Manual Review',
'User Report',
'Pattern Match',
'Algorithm'
);


CREATE TYPE public.emaildomaintype_enum AS ENUM (
'Free',
'Unknown',
'Custom',
'Disposable'
);


CREATE TYPE public.hashtagusagepattern_enum AS ENUM (
'Trending',
'Normal',
'Random',
'Spam'
);


CREATE TYPE public.investigationstatus_enum AS ENUM (
'Pending',
'Active',
'Completed'
);


CREATE TYPE public.locationprovided_enum AS ENUM (
'Fake',
'No',
'Yes',
'Multiple'
);


CREATE TYPE public.loginfrequency_enum AS ENUM (
'Medium',
'High',
'Low',
'Suspicious'
);


CREATE TYPE public.logintimepattern_enum AS ENUM (
'Burst',
'Bot-like',
'Random',
'Regular'
);


CREATE TYPE public.mentionpattern_enum AS ENUM (
'Normal',
'Random',
'Targeted',
'Spam'
);


CREATE TYPE public.monitoringpriority_enum AS ENUM (
'Low',
'Medium',
'Urgent',
'High'
);


CREATE TYPE public.phonenumberstatus_enum AS ENUM (
'Invalid',
'VOIP',
'Valid'
);


CREATE TYPE public.platformtype_enum AS ENUM (
'Microblog',
'Social Network',
'Video Platform',
'Forum'
);


CREATE TYPE public.profilenamepattern_enum AS ENUM (
'Sequential',
'Template',
'Random',
'Natural'
);


CREATE TYPE public.profilepicturetype_enum AS ENUM (
'Stock',
'AI Generated',
'Real',
'Celebrity'
);


CREATE TYPE public.responsetimepattern_enum AS ENUM (
'Natural',
'Delayed',
'Random',
'Instant'
);


CREATE TYPE public.reviewfrequency_enum AS ENUM (
'Monthly',
'Quarterly',
'Daily',
'Weekly'
);


CREATE TYPE public.suspensionhistory_enum AS ENUM (
'0',
'1',
'2',
'3',
'4',
'5'
);


CREATE TYPE public.temporalinteractionpattern_enum AS ENUM (
'Natural',
'Periodic',
'Random',
'Automated'
);


CREATE TYPE public.threatlevel_enum AS ENUM (
'Critical',
'High',
'Low',
'Medium'
);


CREATE TYPE public.torusagedetected_enum AS ENUM (
'Yes',
'Suspected',
'No'
);


CREATE TYPE public.usernamepattern_enum AS ENUM (
'Random',
'Generated',
'Meaningful',
'AlphaNum'
);


CREATE TYPE public.verificationstatus_enum AS ENUM (
'Unverified',
'Pending',
'Failed',
'Suspicious'
);


CREATE TABLE public.account (
accindex character(12) NOT NULL,
acctident character varying(14),
platident character varying(8),
plattype public.platformtype_enum,
acctcreatedate date,
acctagespan smallint,
acctstatus public.accountstatus_enum,
acctcategory public.accounttype_enum,
authstatus public.verificationstatus_enum
);


CREATE TABLE public.contentbehavior (
cntref character(12) NOT NULL,
cntsessref character(12),
postnum integer,
postfreq numeric(5,3),
postintvar numeric(6,3),
cntsimscore numeric(4,2),
cntuniqscore numeric(5,4),
cntdiverseval numeric(6,3),
cntlangnum public.contentlanguagecount_enum,
cnttopicent numeric(4,3),
hashusepat public.hashtagusagepattern_enum,
hashratio numeric(3,2),
mentionpat public.mentionpattern_enum,
mentionratio numeric(5,3),
urlsharefreq character varying(24),
urldomdiv numeric(4,2),
mediaupratio numeric(5,3),
mediareratio numeric(6,4)
);


CREATE TABLE public.messaginganalysis (
msgkey character(12) NOT NULL,
msgcntref character(12),
msgnetref character(12),
msgsimscore numeric(4,3),
msgfreq numeric(6,2),
msgtgtdiv numeric(4,2),
resptimepat public.responsetimepattern_enum,
convnatval numeric(4,3),
sentvar numeric(6,4),
langsoph numeric(5,3),
txtuniq numeric(4,2),
keypatmatch character varying(32),
topiccoh numeric(5,4)
);


CREATE TABLE public.moderationaction (
modactkey character(12) NOT NULL,
masedetref character(12),
macntref character(12),
abuserepnum smallint,
violtypedist jsonb,
susphist public.suspensionhistory_enum,
warnnum smallint,
appealnum public.appealcount_enum,
linkacctnum smallint,
clustsize smallint,
clustrole public.clusterrole_enum,
netinflscore numeric(5,2),
coordscore numeric(4,2),
authenscore numeric(5,3),
credscore numeric(4,1),
reputscore numeric(5,4),
trustval numeric(4,2),
impactval numeric(4,3),
monitorpriority public.monitoringpriority_enum,
investstatus public.investigationstatus_enum,
actiontaken public.actiontaken_enum,
reviewfreq public.reviewfrequency_enum,
lastrevdate date,
nextrevdate date
);


CREATE TABLE public.networkmetrics (
netkey character(12) NOT NULL,
netsessref character(12),
network_engagement_metrics jsonb
);


CREATE TABLE public.profile (
profkey character(12) NOT NULL,
profaccref character(12),
profile_composition jsonb
);


CREATE TABLE public.securitydetection (
secdetkey character(12) NOT NULL,
sectechref character(12),
detecttime timestamp without time zone,
detectsource public.detectionsource_enum,
lastupd timestamp without time zone,
updfreqhrs smallint,
detection_score_profile jsonb
);


CREATE TABLE public.sessionbehavior (
sessref character(12) NOT NULL,
sessprofref character(12),
logintimepat public.logintimepattern_enum,
loginfreq public.loginfrequency_enum,
loginlocvar numeric(4,1),
sesslenmean numeric(7,2),
sesscount integer,
actregval numeric(4,2),
acttimedist jsonb
);


CREATE TABLE public.technicalinfo (
techkey character(12) NOT NULL,
technetref character(12),
techmsgref character(12),
regip inet,
iprepscore numeric(6,3),
ipcountrynum smallint,
vpnratio numeric(7,4),
proxycount smallint,
torflag public.torusagedetected_enum,
devtotal smallint,
devtypedist jsonb,
browserdiv numeric(5,3),
uaconsval numeric(6,5)
);
