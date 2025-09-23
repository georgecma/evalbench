CREATE TYPE public.btntens_enum AS ENUM (
'Light',
'Medium',
'Heavy'
);


CREATE TYPE public.devscope_enum AS ENUM (
'Mouse',
'Keyboard',
'Controller',
'Headset',
'Gamepad',
'Microphone'
);


CREATE TYPE public.driverstability_enum AS ENUM (
'Stable',
'Beta',
'Experimental'
);


CREATE TYPE public.gripcoat_enum AS ENUM (
'Rubberized',
'Matte',
'Glossy',
'Textured'
);


CREATE TYPE public.humidres_enum AS ENUM (
'Standard',
'Premium',
'Enhanced'
);


CREATE TYPE public.noisecanc_enum AS ENUM (
'None',
'Passive',
'Active'
);


CREATE TYPE public.scrollencoder_enum AS ENUM (
'Mechanical',
'Optical',
'Magnetic'
);


CREATE TYPE public.stabrattle_enum AS ENUM (
'None',
'Minimal',
'Moderate'
);


CREATE TYPE public.tempres_enum AS ENUM (
'Standard',
'Premium',
'Enhanced'
);


CREATE TABLE public.audioandmedia (
audregistry character varying(20) NOT NULL,
auddevref character varying(20) NOT NULL,
audperfref character varying(20) NOT NULL,
sndleveldb numeric(4,1),
sndsig character varying(30),
noiseisodb smallint,
audlatms numeric(4,1),
micsensedb numeric(5,2),
micfreqresp character varying(50),
spkimpohm smallint,
spksensedb smallint,
thdpct numeric(3,2),
freqresp character varying(50),
drvszmm smallint,
surrsnd character varying(30),
eqcount smallint,
micmon boolean,
noisecanc public.noisecanc_enum,
btversion character varying(35),
btrangem smallint,
btlatms numeric(5,2),
multidev boolean,
autoslpmin smallint,
wakems numeric(5,1)
);


CREATE TABLE public.deviceidentity (
devregistry character varying(20) NOT NULL,
devsessionref character varying(20) NOT NULL,
makername character varying(50),
modnum character varying(50),
fwver character varying(50),
conntype character varying(35),
wlrangem numeric(4,1),
wlinterf character varying(35),
wlchanhop boolean,
wllatvar numeric(4,2),
pwridlemw integer,
pwractmw integer,
pwrrgbmw integer,
brdmemmb smallint,
profcount smallint,
mcresptime numeric(4,2),
mcexecspeed numeric(4,2),
mctimacc numeric(5,2),
dpires integer,
dpisteps smallint,
senstype character varying(50),
sensres integer
);


CREATE TABLE public.interactionandcontrol (
interactregistry character varying(20) NOT NULL,
interactphysref character varying(20) NOT NULL,
interactdevref character varying(20) NOT NULL,
amblight boolean,
tempsense boolean,
accelsense boolean,
gyrosense boolean,
hapfeed character varying(30),
hapstr smallint,
vibmodes smallint,
forcefeed character varying(35),
trigres smallint,
trigtravmm numeric(3,1),
joydead numeric(4,2),
joyprec numeric(4,1),
btnspcmm numeric(4,1),
btnszmm numeric(4,1),
dpadvar character varying(30),
dpadacc numeric(4,1),
astickvar character varying(30),
driftres numeric(4,1)
);


CREATE TABLE public.mechanical (
mechregistry character varying(20) NOT NULL,
mechperfref character varying(20) NOT NULL,
mechdevref character varying(20) NOT NULL,
keyforceg numeric(5,2),
keytravmm numeric(3,1),
swtchvar character varying(40),
swtchdur bigint,
ghostkeys smallint,
keyrollo character varying(35),
swtchcons numeric(4,1),
ghosteff numeric(4,1),
keychatter numeric(3,2),
actpointmm numeric(3,1),
respointmm numeric(3,1),
tacbumpmm numeric(3,1),
tottravmm numeric(3,1),
stabrattle public.stabrattle_enum,
stabtype character varying(30),
capthkmm numeric(3,1),
capmat character varying(35),
caplegmeth character varying(40),
kbdangle smallint,
wristflag boolean,
palmangle smallint,
ergorate smallint
);


CREATE TABLE public.performance (
perfregistry character varying(20) NOT NULL,
perfsessionref character varying(20) NOT NULL,
accelmax smallint,
speedips smallint,
liftdistmm numeric(3,1),
angsnap boolean,
btntens public.btntens_enum,
clklat numeric(3,2),
clkdur bigint,
screnctyp public.scrollencoder_enum,
scrsteps smallint,
scraccy numeric(4,1)
);


CREATE TABLE public.physicaldurability (
physregistry character varying(20) NOT NULL,
physrgbref character varying(20) NOT NULL,
physperfref character varying(20) NOT NULL,
wgtgram smallint,
wgtdist character varying(30),
cablegram smallint,
cabledrag character varying(25),
feetmat character varying(25),
feetthkmm numeric(3,1),
glidecons numeric(4,1),
fricstatic numeric(3,2),
frickinetic numeric(3,2),
surfcompat character varying(25),
gripsty character varying(30),
gripcoat character varying(30),
gripdur smallint,
sweatres character varying(30),
tempres public.tempres_enum,
humidres public.humidres_enum,
dustres character varying(30),
waterres character varying(35),
impres character varying(30),
drophtm numeric(3,1),
bendforce smallint,
twistdeg smallint,
cablebend integer,
usbconndur integer
);


CREATE TABLE public.rgb (
rgbregistry character varying(20) NOT NULL,
rgbmechref character varying(20) NOT NULL,
rgbaudref character varying(20) NOT NULL,
rgbbright smallint,
rgbcoloracc numeric(4,1),
rgbrfrate smallint,
rgbmodes character varying(25),
rgbzones smallint,
rgbcolors integer
);


CREATE TABLE public.testsessions (
sessionregistry character varying(20) NOT NULL,
stampmoment timestamp(6) without time zone NOT NULL,
devscope public.devscope_enum,
cpuusepct numeric(5,2),
memusemb integer,
driverstatus public.driverstability_enum,
fwupdur smallint,
wlsignal numeric(5,2),
battlevel smallint,
battcapmah integer,
battlifeh numeric(4,1),
chgtimemin numeric(5,2),
qchgflag boolean,
usbpwrline character varying(25),
latms numeric(5,2),
inplagms numeric(5,2),
pollratehz smallint,
dbtimems numeric(4,2),
resptimems numeric(4,2),
clkregms numeric(4,3)
);
