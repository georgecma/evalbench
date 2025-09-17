CREATE TYPE public.access_restriction_enum AS ENUM (
'Internal',
'Public',
'Restricted'
);


CREATE TYPE public.action_taken_enum AS ENUM (
'Warning',
'Restriction',
'Suspension'
);


CREATE TYPE public.alert_level_enum AS ENUM (
'Low',
'Critical',
'High',
'Medium'
);


CREATE TYPE public.audit_trail_status_enum AS ENUM (
'Complete',
'Missing',
'Partial'
);


CREATE TYPE public.broker_reporting_status_enum AS ENUM (
'Incomplete',
'Late',
'Complete'
);


CREATE TYPE public.business_restriction_enum AS ENUM (
'Partial',
'Full'
);


CREATE TYPE public.case_status_enum AS ENUM (
'Investigation',
'Closed',
'Monitoring'
);


CREATE TYPE public.communication_pattern_enum AS ENUM (
'Regular',
'Irregular'
);


CREATE TYPE public.compliance_rating_enum AS ENUM (
'B',
'A',
'D',
'C'
);


CREATE TYPE public.confidentiality_level_enum AS ENUM (
'Normal',
'Highly Sensitive',
'Sensitive'
);


CREATE TYPE public.corporate_event_proximity_enum AS ENUM (
'Earnings',
'Restructuring',
'M&A'
);


CREATE TYPE public.data_retention_status_enum AS ENUM (
'Archived',
'Deleted',
'Current'
);


CREATE TYPE public.data_sharing_status_enum AS ENUM (
'Limited',
'Prohibited',
'Allowed'
);


CREATE TYPE public.detection_method_enum AS ENUM (
'Automated',
'Hybrid',
'Manual'
);


CREATE TYPE public.disclosure_compliance_enum AS ENUM (
'Full',
'Non-compliant',
'Partial'
);


CREATE TYPE public.documentation_status_enum AS ENUM (
'Incomplete',
'Partial',
'Complete'
);


CREATE TYPE public.escalation_level_enum AS ENUM (
'Compliance',
'Legal',
'Supervisor'
);


CREATE TYPE public.event_announcement_timing_enum AS ENUM (
'Pre-market',
'Intraday',
'Post-market'
);


CREATE TYPE public.evidence_strength_enum AS ENUM (
'Strong',
'Weak',
'Moderate'
);


CREATE TYPE public.exchange_notification_enum AS ENUM (
'Warning',
'Inquiry'
);


CREATE TYPE public.financial_relationship_enum AS ENUM (
'Business',
'Personal'
);


CREATE TYPE public.industry_enum AS ENUM (
'Oil & Gas',
'Banking',
'Biotech',
'Retail',
'Software'
);


CREATE TYPE public.investigation_priority_enum AS ENUM (
'Low',
'Medium',
'High'
);


CREATE TYPE public.layering_indicator_enum AS ENUM (
'Confirmed',
'Suspected'
);


CREATE TYPE public.legal_action_status_enum AS ENUM (
'Pending',
'Active'
);


CREATE TYPE public.marking_close_pattern_enum AS ENUM (
'Occasional',
'Frequent'
);


CREATE TYPE public.momentum_ignition_signal_enum AS ENUM (
'Strong',
'Weak'
);


CREATE TYPE public.monitoring_intensity_enum AS ENUM (
'Enhanced',
'Intensive',
'Standard'
);


CREATE TYPE public.order_timing_pattern_enum AS ENUM (
'Irregular',
'Regular',
'Suspicious'
);


CREATE TYPE public.order_type_distribution_enum AS ENUM (
'Market',
'Mixed',
'Limit'
);


CREATE TYPE public.penalty_imposed_enum AS ENUM (
'Warning',
'Fine',
'Ban'
);


CREATE TYPE public.policy_update_needed_enum AS ENUM (
'Yes',
'No',
'Urgent'
);


CREATE TYPE public.position_holding_period_enum AS ENUM (
'Intraday',
'Position',
'Long-term',
'Swing'
);


CREATE TYPE public.regulatory_filing_status_enum AS ENUM (
'Delayed',
'Missing',
'Current'
);


CREATE TYPE public.regulatory_investigation_enum AS ENUM (
'Preliminary',
'Active'
);


CREATE TYPE public.relationship_mapping_status_enum AS ENUM (
'Partial',
'Pending',
'Complete'
);


CREATE TYPE public.remediation_status_enum AS ENUM (
'Not Required',
'Pending',
'Completed'
);


CREATE TYPE public.report_generation_status_enum AS ENUM (
'Hybrid',
'Automated',
'Manual'
);


CREATE TYPE public.reputation_impact_enum AS ENUM (
'Severe',
'Moderate',
'Minimal'
);


CREATE TYPE public.resolution_status_enum AS ENUM (
'Resolved',
'In Progress',
'Pending'
);


CREATE TYPE public.review_frequency_enum AS ENUM (
'Monthly',
'Weekly',
'Daily'
);


CREATE TYPE public.risk_tolerance_enum AS ENUM (
'Conservative',
'Aggressive',
'Moderate'
);


CREATE TYPE public.sector_enum AS ENUM (
'Energy',
'Healthcare',
'Consumer',
'Technology',
'Finance'
);


CREATE TYPE public.settlement_status_enum AS ENUM (
'Negotiating',
'Settled'
);


CREATE TYPE public.shared_contact_info_enum AS ENUM (
'Email',
'Multiple',
'Phone'
);


CREATE TYPE public.stock_symbol_enum AS ENUM (
'AAPL',
'AMZN',
'GOOGL',
'META',
'MSFT'
);


CREATE TYPE public.surveillance_system_enum AS ENUM (
'Secondary',
'Multiple',
'Primary'
);


CREATE TYPE public.system_update_needed_enum AS ENUM (
'Minor',
'Major',
'No'
);


CREATE TYPE public.trader_type_enum AS ENUM (
'Market Maker',
'Broker',
'Individual',
'Institution'
);


CREATE TYPE public.trading_frequency_enum AS ENUM (
'Low',
'Medium',
'High'
);


CREATE TYPE public.trading_restriction_period_enum AS ENUM (
'Blackout',
'Special'
);


CREATE TYPE public.training_requirement_enum AS ENUM (
'Refresher',
'Comprehensive'
);


CREATE TYPE public.unusual_option_activity_enum AS ENUM (
'Moderate',
'High'
);


CREATE TYPE public.wash_trade_suspicion_enum AS ENUM (
'Low',
'Medium',
'High'
);


CREATE TABLE public.advancedbehavior (
abhvreg character varying(50) NOT NULL,
translink character varying(50) NOT NULL,
patsim numeric(7,4),
peercorr numeric(7,4),
mktcorr numeric(7,4),
secrotimp numeric(7,4)
);


CREATE TABLE public.compliancecase (
compreg character varying(50) NOT NULL,
transref character varying(50) NOT NULL,
regfilestat public.regulatory_filing_status_enum,
disclosecmp public.disclosure_compliance_enum,
brokrepstat public.broker_reporting_status_enum,
exchnotif public.exchange_notification_enum,
prevviol integer,
comprate public.compliance_rating_enum,
risksc numeric(7,4),
alertlvl public.alert_level_enum,
invstprior public.investigation_priority_enum,
casestat public.case_status_enum,
revfreq public.review_frequency_enum,
lastrevdt date,
nextrevdt date,
monitint public.monitoring_intensity_enum,
survsys public.surveillance_system_enum,
detectmth public.detection_method_enum,
fposrate numeric(5,2),
modelconf numeric(7,4),
abhvref character varying(50)
);


CREATE TABLE public.enforcementactions (
enforcereg character varying(50) NOT NULL,
compref2 character varying(50) NOT NULL,
acttake public.action_taken_enum,
esclvl public.escalation_level_enum,
resstat public.resolution_status_enum,
penimp public.penalty_imposed_enum,
penamt numeric(20,2),
legactstat public.legal_action_status_enum,
settlestat public.settlement_status_enum,
repimp public.reputation_impact_enum,
busrestr public.business_restriction_enum,
remedstat public.remediation_status_enum,
traderestr public.trading_restriction_period_enum,
sysupdneed public.system_update_needed_enum,
polupdneed public.policy_update_needed_enum,
trainreq public.training_requirement_enum,
repgenstat public.report_generation_status_enum,
dataretstat public.data_retention_status_enum,
auditstat public.audit_trail_status_enum,
conflvl public.confidentiality_level_enum,
accrestr public.access_restriction_enum,
datashare public.data_sharing_status_enum,
invdetref character varying(50)
);


CREATE TABLE public.investigationdetails (
invdetreg character varying(50) NOT NULL,
compref character varying(50) NOT NULL,
reginv public.regulatory_investigation_enum,
patrecsc numeric(7,4),
behansc numeric(7,4),
netansc numeric(7,4),
relmapstat public.relationship_mapping_status_enum,
connent character varying(100),
commaddr integer,
sharectc public.shared_contact_info_enum,
finrel public.financial_relationship_enum,
commpat public.communication_pattern_enum,
tcirclesz integer,
grpbehsc numeric(7,4),
mktabprob numeric(5,2),
evidstr public.evidence_strength_enum,
docustat public.documentation_status_enum,
sentref character varying(50)
);


CREATE TABLE public.sentimentandfundamentals (
sentreg character varying(50) NOT NULL,
transref character varying(50) NOT NULL,
newsscore numeric(5,2),
socscore numeric(5,2),
anlycount integer,
inholdpct numeric(5,2),
instownpct numeric(5,2),
shortintrt numeric(7,4),
optvolrt numeric(7,4),
putcallrt numeric(7,4),
impvolrank numeric(7,4),
unuoptact public.unusual_option_activity_enum,
corpeventprx public.corporate_event_proximity_enum,
eventannotm public.event_announcement_timing_enum,
infoleaksc numeric(5,2),
trdref2 character varying(50)
);


CREATE TABLE public.trader (
tradereg character varying(50) NOT NULL,
tradekind public.trader_type_enum,
acctdays integer,
acctbal numeric(20,2),
freqscope public.trading_frequency_enum,
voldaily numeric(20,4),
posavg numeric(20,4),
posspan public.position_holding_period_enum,
trading_performance jsonb
);


CREATE TABLE public.transactionrecord (
transreg character varying(50) NOT NULL,
transtime timestamp without time zone NOT NULL,
trdref character varying(50) NOT NULL,
ordervar numeric(20,4),
ordertimepat public.order_timing_pattern_enum,
ordertypedist public.order_type_distribution_enum,
cancelpct numeric(7,4),
modfreq numeric(7,4),
darkusage character varying(100),
offmkt character varying(100),
crossfreq numeric(10,4),
risk_indicators jsonb
);
