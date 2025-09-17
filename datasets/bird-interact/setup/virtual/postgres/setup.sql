CREATE TYPE public.accessset_enum AS ENUM (
'Standard',
'Custom',
'Enhanced'
);


CREATE TYPE public.ageverification_enum AS ENUM (
'Not Required',
'Verified',
'Pending'
);


CREATE TYPE public.betatestingparticipation_enum AS ENUM (
'Former',
'Yes',
'No'
);


CREATE TYPE public.campaignparticipation_enum AS ENUM (
'Selective',
'All',
'Active'
);


CREATE TYPE public.chatlang_enum AS ENUM (
'Mixed',
'Translation',
'English',
'Native'
);


CREATE TYPE public.churnrisk_enum AS ENUM (
'High',
'Medium',
'Low',
'None'
);


CREATE TYPE public.commpref_enum AS ENUM (
'SMS',
'Email',
'Push'
);


CREATE TYPE public.communitycontribution_enum AS ENUM (
'Low',
'High',
'Medium'
);


CREATE TYPE public.connectionquality_enum AS ENUM (
'Poor',
'Excellent',
'Good',
'Fair'
);


CREATE TYPE public.contentcompliance_enum AS ENUM (
'Warning',
'Violation',
'Compliant'
);


CREATE TYPE public.contentcreationstatus_enum AS ENUM (
'Active',
'Occasional'
);


CREATE TYPE public.contentlanguagepreference_enum AS ENUM (
'Both',
'Original',
'Translated'
);


CREATE TYPE public.contentpreference_enum AS ENUM (
'Music',
'Dance',
'Gaming',
'Chat'
);


CREATE TYPE public.datasharingconsent_enum AS ENUM (
'Partial',
'Minimal',
'Full'
);


CREATE TYPE public.devicetype_enum AS ENUM (
'Windows',
'iOS',
'Android',
'Mac'
);


CREATE TYPE public.eventparticipation_enum AS ENUM (
'Regular',
'Rare',
'Always',
'Never'
);


CREATE TYPE public.fanclubcontribution_enum AS ENUM (
'Medium',
'Outstanding',
'Low',
'High'
);


CREATE TYPE public.fanclubstatus_enum AS ENUM (
'Non-member',
'Premium',
'Elite',
'Basic'
);


CREATE TYPE public.fangender_enum AS ENUM (
'Other',
'Male',
'Undisclosed',
'Female'
);


CREATE TYPE public.faninterests_enum AS ENUM (
'Technology',
'Anime',
'Art',
'Music',
'Gaming'
);


CREATE TYPE public.fanlang_enum AS ENUM (
'Multiple',
'Korean',
'English',
'Japanese',
'Chinese'
);


CREATE TYPE public.fanoccupation_enum AS ENUM (
'Professional',
'Student',
'Other',
'Creative'
);


CREATE TYPE public.fanstatus_enum AS ENUM (
'Inactive',
'VIP',
'Active',
'Blocked'
);


CREATE TYPE public.favoritegifttype_enum AS ENUM (
'Limited',
'Custom',
'Premium',
'Standard'
);


CREATE TYPE public.giftsendingfrequency_enum AS ENUM (
'Often',
'Rarely',
'Never',
'Frequent'
);


CREATE TYPE public.grouprole_enum AS ENUM (
'Member',
'Leader',
'Moderator'
);


CREATE TYPE public.idolgenre_enum AS ENUM (
'Electronic',
'Dance',
'Pop',
'Traditional',
'Rock'
);


CREATE TYPE public.idoltype_enum AS ENUM (
'2D',
'AI Generated',
'3D',
'Mixed Reality'
);


CREATE TYPE public.interactionfrequency_enum AS ENUM (
'Weekly',
'Monthly',
'Occasional',
'Daily'
);


CREATE TYPE public.interactionplatform_enum AS ENUM (
'YouTube',
'Twitter',
'Official App',
'TikTok'
);


CREATE TYPE public.interactiontype_enum AS ENUM (
'Vote',
'Comment',
'Share',
'Gift',
'Live Stream'
);


CREATE TYPE public.langset_enum AS ENUM (
'Translated',
'Auto',
'Original'
);


CREATE TYPE public.loginfrequency_enum AS ENUM (
'Rare',
'Monthly',
'Weekly',
'Daily'
);


CREATE TYPE public.markpref_enum AS ENUM (
'Opted In',
'Selective',
'Opted Out'
);


CREATE TYPE public.membershiptype_enum AS ENUM (
'Free',
'Basic',
'Diamond',
'Premium'
);


CREATE TYPE public.messagesentiment_enum AS ENUM (
'Negative',
'Positive',
'Neutral'
);


CREATE TYPE public.moderationstatus_enum AS ENUM (
'Warning',
'Good Standing',
'Restricted'
);


CREATE TYPE public.notifpref_enum AS ENUM (
'Important',
'All'
);


CREATE TYPE public.paymentmethod_enum AS ENUM (
'Credit Card',
'Mobile Payment',
'PayPal',
'Crypto'
);


CREATE TYPE public.paymentverification_enum AS ENUM (
'Pending',
'Verified'
);


CREATE TYPE public.peakactivitytime_enum AS ENUM (
'Afternoon',
'Evening',
'Night',
'Morning'
);


CREATE TYPE public.platformused_enum AS ENUM (
'Tablet',
'Mobile',
'Console',
'PC'
);


CREATE TYPE public.privacysettings_enum AS ENUM (
'Private',
'Friends Only',
'Public'
);


CREATE TYPE public.reputationlevel_enum AS ENUM (
'Respected',
'Elite',
'New',
'Established'
);


CREATE TYPE public.rewardtier_enum AS ENUM (
'Bronze',
'Platinum',
'Gold',
'Silver'
);


CREATE TYPE public.spendingfrequency_enum AS ENUM (
'Occasional',
'Weekly',
'Monthly',
'Daily'
);


CREATE TYPE public.surveyparticipation_enum AS ENUM (
'Never',
'Active',
'Occasional'
);


CREATE TYPE public.tradingactivitylevel_enum AS ENUM (
'High',
'Low',
'Medium'
);


CREATE TYPE public.translationusage_enum AS ENUM (
'Always',
'Sometimes',
'Never'
);


CREATE TYPE public.violationhistory_enum AS ENUM (
'Major',
'Minor'
);


CREATE TABLE public.additionalnotes (
notesreg character varying(20) NOT NULL,
notesretainpivot character varying(20),
noteinfo text
);


CREATE TABLE public.commerceandcollection (
commercereg character varying(20) NOT NULL,
commerceengagepivot character varying(20),
commercememberpivot character varying(20),
merchbuy smallint,
merchspendusd numeric(10,2),
digown integer,
physown integer,
collcomprate numeric(5,1),
tradelevel public.tradingactivitylevel_enum
);


CREATE TABLE public.engagement (
engagereg character varying(20) NOT NULL,
engageactivitypivot character varying(20),
engagememberpivot character varying(20),
socintscore numeric(6,2),
engrate numeric(6,3),
actfreq public.interactionfrequency_enum,
peaktime public.peakactivitytime_enum,
actdayswk smallint,
avgsesscount smallint,
contpref public.contentpreference_enum,
langpref public.contentlanguagepreference_enum,
transuse public.translationusage_enum
);


CREATE TABLE public.eventsandclub (
eventsreg character varying(20) NOT NULL,
eventssocialpivot character varying(20),
eventsmemberpivot character varying(20),
clubjdate date,
participation_summary jsonb
);


CREATE TABLE public.fans (
userregistry character varying(20) NOT NULL,
nicklabel character varying(100),
regmoment date,
tierstep smallint,
ptsval integer,
statustag public.fanstatus_enum,
personal_attributes jsonb
);


CREATE TABLE public.interactions (
activityreg character varying(20) NOT NULL,
timemark timestamp without time zone,
interactfanpivot character varying(20),
interactidolpivot character varying(20),
actkind public.interactiontype_enum,
actplat public.interactionplatform_enum,
platused public.platformused_enum,
devtype public.devicetype_enum,
appver character varying(20),
giftfreq public.giftsendingfrequency_enum,
gifttot integer,
giftvalusd numeric(10,2),
favgifttag public.favoritegifttype_enum,
engagement_metrics jsonb
);


CREATE TABLE public.loyaltyandachievements (
loyaltyreg character varying(20) NOT NULL,
loyaltyeventspivot character varying(20),
loyaltyengagepivot character varying(20),
rankpos integer,
inflscore numeric(5,2),
reputelv public.reputationlevel_enum,
trustval numeric(4,1),
reward_progress jsonb
);


CREATE TABLE public.membershipandspending (
memberreg character varying(20) NOT NULL,
memberfanpivot character varying(20),
membkind public.membershiptype_enum,
membdays smallint,
spendusd numeric(10,2),
spendfreq public.spendingfrequency_enum,
paymethod public.paymentmethod_enum
);


CREATE TABLE public.moderationandcompliance (
modreg character varying(20) NOT NULL,
moderationinteractpivot character varying(20),
moderationsocialpivot character varying(20),
rptcount smallint,
warncount smallint,
violhist public.violationhistory_enum,
modstat public.moderationstatus_enum,
contcomp public.contentcompliance_enum,
ageverif public.ageverification_enum,
payverif public.paymentverification_enum,
idverif character varying(50)
);


CREATE TABLE public.preferencesandsettings (
prefreg character varying(20) NOT NULL,
preferencesmemberpivot character varying(20),
preferencessocialpivot character varying(20),
privset public.privacysettings_enum,
dsconsent public.datasharingconsent_enum,
notifpref public.notifpref_enum,
commpref public.commpref_enum,
markpref public.markpref_enum,
langset public.langset_enum,
accessset public.accessset_enum,
devcount smallint,
logfreq public.loginfrequency_enum,
lastlogdt date,
sesscount integer,
timehrs integer,
avgdailymin smallint,
peaksess smallint,
intconsist numeric(3,2),
platstable numeric(3,2),
connqual public.connectionquality_enum
);


CREATE TABLE public.retentionandinfluence (
retreg character varying(20) NOT NULL,
retainengagepivot character varying(20),
retainloyaltypivot character varying(20),
churnflag public.churnrisk_enum,
reactcount smallint,
refcount smallint,
contreach integer,
viralcont smallint,
trendpart smallint,
hashuse smallint
);


CREATE TABLE public.socialcommunity (
socialreg character varying(20) NOT NULL,
socialengagepivot character varying(20),
socialcommercepivot character varying(20),
collabcount smallint,
community_engagement jsonb
);


CREATE TABLE public.supportandfeedback (
supportreg character varying(20) NOT NULL,
supportinteractpivot character varying(20),
supportprefpivot character varying(20),
techissuerpt smallint,
supptix smallint,
fbsubs smallint,
survpart public.surveyparticipation_enum,
betapart public.betatestingparticipation_enum,
featreqsubs smallint,
bugsubs smallint,
satrate numeric(3,1),
npsval smallint
);


CREATE TABLE public.virtualidols (
entityreg character varying(20) NOT NULL,
nametag character varying(100),
kindtag public.idoltype_enum,
debdate date,
assocgroup character varying(100),
genretag public.idolgenre_enum,
primlang character varying(50)
);
