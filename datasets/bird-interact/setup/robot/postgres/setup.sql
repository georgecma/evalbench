CREATE TABLE public.actuation_data (
actreg character varying(20) NOT NULL,
actoperref character varying(20),
actdetref character varying(20),
actrecref character varying(20),
tcpxval numeric(7,2),
tcpyval real,
tcpzval real,
tcp_rxval numeric(6,2),
tcp_ryval real,
tcp_rzval real,
tcpspeedval numeric(8,2),
tcpaccelval real,
pathaccmmval numeric(6,3),
poserrmmval real,
orienterrdegval real,
payloadwval numeric(6,2),
payloadival real,
m1currval numeric(6,2),
m2currval real,
m3currval real,
m4currval numeric(6,2),
m5currval real,
m6currval real,
m1voltval numeric(5,2),
m2voltval real,
m3voltval real,
m4voltval numeric(6,2),
m5voltval real,
m6voltval real
);


CREATE TABLE public.joint_condition (
jcondoperref character varying(20),
jcrecref character varying(20),
jcdetref character varying(20),
j1tempval numeric(5,2),
j2tempval real,
j3tempval numeric(6,3),
j4tempval real,
j5tempval numeric(7,2),
j6tempval real,
j1vibval numeric(5,3),
j2vibval real,
j3vibval real,
j4vibval numeric(5,3),
j5vibval real,
j6vibval real,
j1backval numeric(5,4),
j2backval numeric(6,3),
j3backval real,
j4backval real,
j5backval numeric(7,4),
j6backval real
);


CREATE TABLE public.joint_performance (
jperfoperref character varying(20),
jperfrecref character varying(20),
jperfdetref character varying(20),
joint_metrics jsonb
);


CREATE TABLE public.maintenance_and_fault (
upkeepactuation character varying(20) NOT NULL,
upkeepoperation character varying(20),
upkeeprobot character varying(20),
faultcodeval character varying(25),
issuecategoryval character varying(22),
issuelevelval character varying(18),
faultpredscore real,
faulttypeestimation character varying(20),
rulhours integer,
upkeepduedays smallint,
upkeepcostest numeric(9,3)
);


CREATE TABLE public.mechanical_status (
mechactref character varying(20),
mechoperref character varying(20),
mechdetref character varying(20),
brk1statval character varying(20),
brk2statval character varying(30),
brk3statval character varying(25),
brk4statval character varying(25),
brk5statval character varying(55),
brk6statval character varying(25),
enc1statval character varying(35),
enc2statval character varying(20),
enc3statval character varying(40),
enc4statval character varying(45),
enc5statval character varying(25),
enc6statval character varying(20),
gb1tempval numeric(6,2),
gb2tempval real,
gb3tempval real,
gb4tempval numeric(7,3),
gb5tempval real,
gb6tempval real,
gb1vibval numeric(6,3),
gb2vibval real,
gb3vibval real,
gb4vibval numeric(5,3),
gb5vibval real,
gb6vibval real
);


CREATE TABLE public.operation (
operreg character varying(20) NOT NULL,
operbotdetref character varying(20),
operrecref character varying(20),
totopshrval numeric(9,2),
apptypeval character varying(40),
opermodeval character(25),
currprogval character varying(40),
progcyclecount integer,
cycletimesecval numeric(8,3),
axiscountval smallint
);


CREATE TABLE public.performance_and_safety (
effectivenessactuation character varying(20) NOT NULL,
effectivenessoperation character varying(20),
effectivenessrobot character varying(20),
conditionindexval numeric(6,4),
effectivenessindexval numeric(4,2),
qualitymeasureval real,
energyusekwhval numeric(7,2),
pwrfactorval real,
airpressval numeric(6,3),
toolchangecount integer,
toolwearpct real,
safety_metrics jsonb
);


CREATE TABLE public.robot_details (
botdetreg character varying(20) NOT NULL,
mfgnameval character varying(60),
modelseriesval character varying(40),
bottypeval character(15),
payloadcapkg numeric(9,2),
reachmmval smallint,
instdateval date,
fwversionval character varying(25),
ctrltypeval character varying(40)
);


CREATE TABLE public.robot_record (
recreg character varying(20) NOT NULL,
rects timestamp without time zone NOT NULL,
botcode character varying(25) NOT NULL
);


CREATE TABLE public.system_controller (
systemoverseeractuation character varying(20) NOT NULL,
systemoverseeroperation character varying(20),
systemoverseerrobot character varying(20),
controller_metrics jsonb
);