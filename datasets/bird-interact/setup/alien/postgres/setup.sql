CREATE TABLE public.observationalconditions (
    signalref character(36) NOT NULL,
    obstime time without time zone,
    obsdate date,
    obsdurhrs numeric(5,2)
);

CREATE TABLE public.observatories (
    observstation character(60) NOT NULL,
    weathprofile character varying(40),
    seeingprofile character varying(50),
    atmostransparency numeric(5,3),
    lunarstage character varying(25),
    lunardistdeg numeric(7,2),
    solarstatus character varying(35),
    geomagstatus character varying(35),
    sidereallocal character(8),
    airtempc numeric(5,2),
    humidityrate numeric(6,3),
    windspeedms numeric(4,2),
    presshpa numeric(6,1)
);

CREATE TABLE public.researchprocess (
    signalref character(36) NOT NULL,
    analysisprio text,
    followstat character varying(25),
    peerrevstat character varying(25),
    pubstat character(25),
    resprio character varying(30),
    fundstat character(30),
    collabstat character(35),
    secclass character(35),
    discstat character varying(40),
    notesmemo text
);


CREATE TABLE public.signaladvancedphenomena (
    signalref character(36) NOT NULL,
    intermedeffects character varying(40),
    gravlens character varying(50),
    quanteffects character varying(85),
    encryptevid character varying(40),
    langstruct text,
    msgcontent text,
    cultsig character varying(60),
    sciimpact character varying(50)
);


CREATE TABLE public.signalclassification (
    signalref character(36) NOT NULL,
    sigclasstype character varying(40),
    sigpattern character varying(60),
    repeatcount smallint,
    periodsec numeric(12,3),
    complexidx numeric(6,3),
    entropyval numeric(6,2),
    infodense numeric(6,3),
    classconf numeric(5,2)
);


CREATE TABLE public.signaldecoding (
    signalref character(36) NOT NULL,
    encodetype character varying(40),
    compressratio numeric(6,3),
    errcorrlvl character varying(35),
    decodeconf numeric(5,2),
    decodemethod character varying(35),
    decodestat character varying(25),
    decodeiters smallint,
    proctimehrs numeric(6,2),
    compresources character varying(50),
    analysisdp character varying(25),
    veriflvl character varying(30),
    confirmstat character varying(30)
);


CREATE TABLE public.signaldynamics (
    signalref character(36) NOT NULL,
    sigintegrity character varying(30),
    sigrecurr character varying(25),
    sigevolve character varying(25),
    tempstab character varying(20),
    spatstab character varying(20),
    freqstab character varying(35),
    phasestab character varying(35),
    ampstab character varying(20),
    modstab character varying(30),
    sigcoherence character varying(25),
    sigdisp character varying(25),
    sigscint character varying(45)
);


CREATE TABLE public.signalprobabilities (
    signalref character(36) NOT NULL,
    falseposprob numeric(5,4),
    sigunique numeric(7,4),
    simindex numeric(5,4),
    corrscore numeric(5,4),
    anomscore double precision,
    techsigprob numeric(5,4),
    biosigprob numeric(6,2),
    natsrcprob numeric(7,3),
    artsrcprob numeric(3,1)
);


CREATE TABLE public.signals (
    signalregistry character(36) NOT NULL,
    timemark timestamp with time zone,
    telescref character(20) NOT NULL,
    detectinstr character varying(50),
    signalclass character varying(50),
    sigstrdb numeric(7,2),
    freqmhz numeric(9,3),
    bwhz numeric(10,3),
    centerfreqmhz numeric(8,3),
    freqdrifthzs numeric(9,3),
    doppshifthz double precision,
    sigdursec numeric(6,2),
    pulsepersec numeric(6,3),
    pulsewidms numeric(6,3),
    modtype character varying(30),
    modindex numeric(6,4),
    carrierfreqmhz numeric(9,3),
    phaseshiftdeg numeric(6,2),
    polarmode character varying(30),
    polarangledeg numeric(5,1),
    snrratio numeric(6,2),
    noisefloordbm double precision,
    interflvl character varying(30),
    rfistat character varying(30),
    atmointerf character varying(30)
);


CREATE TABLE public.sourceproperties (
    signalref character(36) NOT NULL,
    sourceradeg numeric(7,4),
    sourcedecdeg numeric(7,4),
    sourcedistly numeric(10,2),
    gallong numeric(6,2),
    gallat numeric(6,2),
    celestobj character varying(75),
    objtype character varying(50),
    objmag numeric(5,2),
    objtempk integer,
    objmasssol numeric(6,3),
    objagegyr numeric(6,3),
    objmetal numeric(5,3),
    objpropmotion numeric(7,2),
    objradvel numeric(7,2)
);


CREATE TABLE public.telescopes (
    telescregistry character(20) NOT NULL,
    observstation character(60) NOT NULL,
    equipstatus character varying(35),
    calibrstatus character varying(50),
    pointaccarc numeric(6,2),
    trackaccarc numeric(6,2),
    focusquality character varying(25),
    detecttempk numeric(7,2),
    coolsysstatus character varying(35),
    powerstatus character varying(30),
    datastorstatus character varying(35),
    netstatus character varying(40),
    bandusagepct numeric(5,2),
    procqueuestatus character varying(40)
);


ALTER TABLE ONLY public.observationalconditions
    ADD CONSTRAINT observationalconditions_pkey PRIMARY KEY (signalref);


ALTER TABLE ONLY public.observatories
    ADD CONSTRAINT observatories_pkey PRIMARY KEY (observstation);


ALTER TABLE ONLY public.researchprocess
    ADD CONSTRAINT researchprocess_pkey PRIMARY KEY (signalref);


ALTER TABLE ONLY public.signaladvancedphenomena
    ADD CONSTRAINT signaladvancedphenomena_pkey PRIMARY KEY (signalref);


ALTER TABLE ONLY public.signalclassification
    ADD CONSTRAINT signalclassification_pkey PRIMARY KEY (signalref);


ALTER TABLE ONLY public.signaldecoding
    ADD CONSTRAINT signaldecoding_pkey PRIMARY KEY (signalref);


ALTER TABLE ONLY public.signaldynamics
    ADD CONSTRAINT signaldynamics_pkey PRIMARY KEY (signalref);


ALTER TABLE ONLY public.signalprobabilities
    ADD CONSTRAINT signalprobabilities_pkey PRIMARY KEY (signalref);


ALTER TABLE ONLY public.signals
    ADD CONSTRAINT signals_pkey PRIMARY KEY (signalregistry);


ALTER TABLE ONLY public.sourceproperties
    ADD CONSTRAINT sourceproperties_pkey PRIMARY KEY (signalref);


ALTER TABLE ONLY public.telescopes
    ADD CONSTRAINT telescopes_pkey PRIMARY KEY (telescregistry);
