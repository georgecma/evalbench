CREATE TYPE public.acctscope_enum AS ENUM (
'Margin',
'Spot',
'Options',
'Futures'
);


CREATE TYPE public.cancelnote_enum AS ENUM (
'Expired',
'InsufficientFunds',
'UserRequested'
);


CREATE TYPE public.collcoin_enum AS ENUM (
'USDT',
'USDC',
'BTC',
'ETH'
);


CREATE TYPE public.dealedge_enum AS ENUM (
'Sell',
'Buy'
);


CREATE TYPE public.exectune_enum AS ENUM (
'Maker',
'Taker'
);


CREATE TYPE public.feecoin_enum AS ENUM (
'USDC',
'USD',
'USDT'
);


CREATE TYPE public.feerange_enum AS ENUM (
'Tier4',
'Tier1',
'Tier3',
'Tier2'
);


CREATE TYPE public.levscale_enum AS ENUM (
'1',
'2',
'3',
'5',
'10',
'20',
'50',
'100'
);


CREATE TYPE public.makermotion_enum AS ENUM (
'Low',
'Medium',
'High'
);


CREATE TYPE public.margform_enum AS ENUM (
'Isolated',
'Cross'
);


CREATE TYPE public.mktfeel_enum AS ENUM (
'Bearish',
'Bullish',
'Neutral'
);


CREATE TYPE public.orderbase_enum AS ENUM (
'API',
'Web',
'Mobile',
'Bot'
);


CREATE TYPE public.orderflow_enum AS ENUM (
'New',
'PartiallyFilled',
'Cancelled',
'Filled'
);


CREATE TYPE public.ordertune_enum AS ENUM (
'Stop',
'Market',
'Limit',
'StopLimit'
);


CREATE TYPE public.posedge_enum AS ENUM (
'Short',
'Long'
);


CREATE TYPE public.posmagn_enum AS ENUM (
'1',
'2',
'3',
'5',
'10',
'20',
'50',
'100'
);


CREATE TYPE public.techmeter_enum AS ENUM (
'Buy',
'Sell',
'Hold'
);


CREATE TYPE public.timespan_enum AS ENUM (
'IOC',
'GTC',
'GTD',
'FOK'
);


CREATE TYPE public.whalemotion_enum AS ENUM (
'Low',
'Medium',
'High'
);


CREATE TYPE public.wsstate_enum AS ENUM (
'Connected',
'Disconnected'
);


CREATE TABLE public.accountbalances (
accountbalancesnode bigint NOT NULL,
walletsum numeric(12,3),
availsum numeric(12,3),
frozensum numeric(12,3),
margsum numeric(12,3),
unrealline double precision,
realline double precision,
usertag character(36)
);


CREATE SEQUENCE public.accountbalances_accountbalancesnode_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.accountbalances_accountbalancesnode_seq OWNED BY public.accountbalances.accountbalancesnode;


CREATE TABLE public.analyticsindicators (
analyticsindicatorsnode bigint NOT NULL,
mdataref bigint,
mstatsref bigint,
market_sentiment_indicators jsonb
);


CREATE SEQUENCE public.analyticsindicators_analyticsindicatorsnode_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.analyticsindicators_analyticsindicatorsnode_seq OWNED BY public.analyticsindicators.analyticsindicatorsnode;


CREATE TABLE public.fees (
feesnode bigint NOT NULL,
feerange public.feerange_enum,
feerate numeric(8,5),
feetotal numeric(12,6),
feecoin public.feecoin_enum,
rebrate numeric(8,5),
rebtotal numeric(12,6),
orderslink character(36)
);


CREATE SEQUENCE public.fees_feesnode_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.fees_feesnode_seq OWNED BY public.fees.feesnode;


CREATE TABLE public.marketdata (
marketdatanode bigint NOT NULL,
quote_depth_snapshot jsonb
);


CREATE SEQUENCE public.marketdata_marketdatanode_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.marketdata_marketdatanode_seq OWNED BY public.marketdata.marketdatanode;


CREATE TABLE public.marketstats (
marketstatsmark bigint NOT NULL,
fundrate numeric(6,4),
fundspot timestamp without time zone,
openstake numeric(15,5),
volday double precision,
tradeday integer,
tnoverday numeric(12,3),
priceshiftday numeric(12,3),
highspotday numeric(12,3),
lowspotday numeric(12,3),
vwapday numeric(12,3),
mktsize numeric(13,3),
circtotal numeric(13,3),
totsupply numeric(13,3),
maxsupply numeric(13,3),
mkthold numeric(13,3),
traderank integer,
liquidscore numeric(8,2),
volmeter numeric(8,2),
mdlink bigint
);


CREATE SEQUENCE public.marketstats_marketstatsmark_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.marketstats_marketstatsmark_seq OWNED BY public.marketstats.marketstatsmark;


CREATE TABLE public.orderexecutions (
orderexecmark bigint NOT NULL,
fillcount numeric(8,4),
remaincount numeric(8,4),
fillquote numeric(12,3),
fillsum numeric(12,3),
expirespot timestamp without time zone,
cancelnote public.cancelnote_enum,
exectune public.exectune_enum,
ordersmark character(36)
);


CREATE SEQUENCE public.orderexecutions_orderexecmark_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.orderexecutions_orderexecmark_seq OWNED BY public.orderexecutions.orderexecmark;


CREATE TABLE public.orders (
orderspivot bigint NOT NULL,
recordvault character(36) NOT NULL,
timecode timestamp without time zone NOT NULL,
exchspot character(10),
mktnote character varying(30),
orderstamp character(36),
userlink character(36),
ordertune public.ordertune_enum,
dealedge public.dealedge_enum,
dealquote numeric(12,3),
dealcount numeric(12,4),
notionsum numeric(12,3),
orderflow public.orderflow_enum,
timespan public.timespan_enum,
orderbase public.orderbase_enum,
clientmark character varying(80),
createspot timestamp without time zone,
updatespot timestamp without time zone
);


CREATE SEQUENCE public.orders_orderspivot_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.orders_orderspivot_seq OWNED BY public.orders.orderspivot;


CREATE TABLE public.riskandmargin (
riskandmarginpivot bigint NOT NULL,
ordervault character(36),
risk_margin_profile jsonb
);


CREATE SEQUENCE public.riskandmargin_riskandmarginpivot_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.riskandmargin_riskandmarginpivot_seq OWNED BY public.riskandmargin.riskandmarginpivot;


CREATE TABLE public.systemmonitoring (
systemmonitoringpivot bigint NOT NULL,
apireqtotal integer,
apierrtotal integer,
apilatmark real,
wsstate public.wsstate_enum,
rateremain smallint,
lastupdnote character varying(60),
seqcode character varying(60),
slipratio numeric(12,3),
exectimespan numeric(8,2),
queueline integer,
mkteffect real,
priceeffect real,
aitrack bigint
);


CREATE SEQUENCE public.systemmonitoring_systemmonitoringpivot_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.systemmonitoring_systemmonitoringpivot_seq OWNED BY public.systemmonitoring.systemmonitoringpivot;


CREATE TABLE public.users (
usersnode bigint NOT NULL,
userstamp character(36) NOT NULL,
acctscope public.acctscope_enum
);


CREATE SEQUENCE public.users_usersnode_seq
START WITH 1
INCREMENT BY 1
NO MINVALUE
NO MAXVALUE
CACHE 1;


ALTER SEQUENCE public.users_usersnode_seq OWNED BY public.users.usersnode;


ALTER TABLE ONLY public.accountbalances ALTER COLUMN accountbalancesnode SET DEFAULT nextval('public.accountbalances_accountbalancesnode_seq'::regclass);


ALTER TABLE ONLY public.analyticsindicators ALTER COLUMN analyticsindicatorsnode SET DEFAULT nextval('public.analyticsindicators_analyticsindicatorsnode_seq'::regclass);


ALTER TABLE ONLY public.fees ALTER COLUMN feesnode SET DEFAULT nextval('public.fees_feesnode_seq'::regclass);


ALTER TABLE ONLY public.marketdata ALTER COLUMN marketdatanode SET DEFAULT nextval('public.marketdata_marketdatanode_seq'::regclass);


ALTER TABLE ONLY public.marketstats ALTER COLUMN marketstatsmark SET DEFAULT nextval('public.marketstats_marketstatsmark_seq'::regclass);


ALTER TABLE ONLY public.orderexecutions ALTER COLUMN orderexecmark SET DEFAULT nextval('public.orderexecutions_orderexecmark_seq'::regclass);


ALTER TABLE ONLY public.orders ALTER COLUMN orderspivot SET DEFAULT nextval('public.orders_orderspivot_seq'::regclass);


ALTER TABLE ONLY public.riskandmargin ALTER COLUMN riskandmarginpivot SET DEFAULT nextval('public.riskandmargin_riskandmarginpivot_seq'::regclass);


ALTER TABLE ONLY public.systemmonitoring ALTER COLUMN systemmonitoringpivot SET DEFAULT nextval('public.systemmonitoring_systemmonitoringpivot_seq'::regclass);


ALTER TABLE ONLY public.users ALTER COLUMN usersnode SET DEFAULT nextval('public.users_usersnode_seq'::regclass);
