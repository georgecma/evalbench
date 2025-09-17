ALTER TABLE ONLY public.airqualityreadings
ADD CONSTRAINT airqualityreadings_pkey PRIMARY KEY (aqrecordregistry);


ALTER TABLE ONLY public.artifactratings
ADD CONSTRAINT artifactratings_pkey PRIMARY KEY (ratingrecordregistry);


ALTER TABLE ONLY public.artifactscore
ADD CONSTRAINT artifactscore_pkey PRIMARY KEY (artregistry);


ALTER TABLE ONLY public.artifactsecurityaccess
ADD CONSTRAINT artifactsecurityaccess_pkey PRIMARY KEY (secrecordregistry);


ALTER TABLE ONLY public.conditionassessments
ADD CONSTRAINT conditionassessments_pkey PRIMARY KEY (conditionassessregistry);


ALTER TABLE ONLY public.conservationandmaintenance
ADD CONSTRAINT conservationandmaintenance_pkey PRIMARY KEY (conservemaintregistry);


ALTER TABLE ONLY public.environmentalreadingscore
ADD CONSTRAINT environmentalreadingscore_pkey PRIMARY KEY (envreadregistry);


ALTER TABLE ONLY public.exhibitionhalls
ADD CONSTRAINT exhibitionhalls_pkey PRIMARY KEY (hallrecord);


ALTER TABLE ONLY public.lightandradiationreadings
ADD CONSTRAINT lightandradiationreadings_pkey PRIMARY KEY (lightradregistry);


ALTER TABLE ONLY public.riskassessments
ADD CONSTRAINT riskassessments_pkey PRIMARY KEY (riskassessregistry);


ALTER TABLE ONLY public.sensitivitydata
ADD CONSTRAINT sensitivitydata_pkey PRIMARY KEY (sensitivityregistry);


ALTER TABLE ONLY public.showcases
ADD CONSTRAINT showcases_pkey PRIMARY KEY (showcasereg);


ALTER TABLE ONLY public.surfaceandphysicalreadings
ADD CONSTRAINT surfaceandphysicalreadings_pkey PRIMARY KEY (surfphysregistry);


ALTER TABLE ONLY public.usagerecords
ADD CONSTRAINT usagerecords_pkey PRIMARY KEY (usagerecordregistry);


ALTER TABLE ONLY public.airqualityreadings
ADD CONSTRAINT airqualityreadings_envreadref_fkey FOREIGN KEY (envreadref) REFERENCES public.environmentalreadingscore(envreadregistry);


ALTER TABLE ONLY public.artifactratings
ADD CONSTRAINT artifactratings_artref_fkey FOREIGN KEY (artref) REFERENCES public.artifactscore(artregistry);


ALTER TABLE ONLY public.artifactsecurityaccess
ADD CONSTRAINT artifactsecurityaccess_artref_fkey FOREIGN KEY (artref) REFERENCES public.artifactscore(artregistry);


ALTER TABLE ONLY public.artifactsecurityaccess
ADD CONSTRAINT artifactsecurityaccess_ratingref_fkey FOREIGN KEY (ratingref) REFERENCES public.artifactratings(ratingrecordregistry);


ALTER TABLE ONLY public.conditionassessments
ADD CONSTRAINT conditionassessments_artrefexamined_fkey FOREIGN KEY (artrefexamined) REFERENCES public.artifactscore(artregistry);


ALTER TABLE ONLY public.conditionassessments
ADD CONSTRAINT conditionassessments_lightreadrefobserved_fkey FOREIGN KEY (lightreadrefobserved) REFERENCES public.lightandradiationreadings(lightradregistry);


ALTER TABLE ONLY public.conditionassessments
ADD CONSTRAINT conditionassessments_showcaserefexamined_fkey FOREIGN KEY (showcaserefexamined) REFERENCES public.showcases(showcasereg);


ALTER TABLE ONLY public.conservationandmaintenance
ADD CONSTRAINT conservationandmaintenance_artrefmaintained_fkey FOREIGN KEY (artrefmaintained) REFERENCES public.artifactscore(artregistry);


ALTER TABLE ONLY public.conservationandmaintenance
ADD CONSTRAINT conservationandmaintenance_hallrefmaintained_fkey FOREIGN KEY (hallrefmaintained) REFERENCES public.exhibitionhalls(hallrecord);


ALTER TABLE ONLY public.conservationandmaintenance
ADD CONSTRAINT conservationandmaintenance_surfreadrefobserved_fkey FOREIGN KEY (surfreadrefobserved) REFERENCES public.surfaceandphysicalreadings(surfphysregistry);


ALTER TABLE ONLY public.environmentalreadingscore
ADD CONSTRAINT environmentalreadingscore_showcaseref_fkey FOREIGN KEY (showcaseref) REFERENCES public.showcases(showcasereg);


ALTER TABLE ONLY public.exhibitionhalls
ADD CONSTRAINT exhibitionhalls_hallrecord_fkey FOREIGN KEY (hallrecord) REFERENCES public.artifactscore(artregistry);


ALTER TABLE ONLY public.lightandradiationreadings
ADD CONSTRAINT lightandradiationreadings_envreadref_fkey FOREIGN KEY (envreadref) REFERENCES public.environmentalreadingscore(envreadregistry);


ALTER TABLE ONLY public.riskassessments
ADD CONSTRAINT riskassessments_artrefconcerned_fkey FOREIGN KEY (artrefconcerned) REFERENCES public.artifactscore(artregistry);


ALTER TABLE ONLY public.riskassessments
ADD CONSTRAINT riskassessments_hallrefconcerned_fkey FOREIGN KEY (hallrefconcerned) REFERENCES public.exhibitionhalls(hallrecord);


ALTER TABLE ONLY public.sensitivitydata
ADD CONSTRAINT sensitivitydata_artref_fkey FOREIGN KEY (artref) REFERENCES public.artifactscore(artregistry);


ALTER TABLE ONLY public.showcases
ADD CONSTRAINT showcases_hallref_fkey FOREIGN KEY (hallref) REFERENCES public.exhibitionhalls(hallrecord);


ALTER TABLE ONLY public.surfaceandphysicalreadings
ADD CONSTRAINT surfaceandphysicalreadings_envreadref_fkey FOREIGN KEY (envreadref) REFERENCES public.environmentalreadingscore(envreadregistry);


ALTER TABLE ONLY public.usagerecords
ADD CONSTRAINT usagerecords_artrefused_fkey FOREIGN KEY (artrefused) REFERENCES public.artifactscore(artregistry);


ALTER TABLE ONLY public.usagerecords
ADD CONSTRAINT usagerecords_sensdatalink_fkey FOREIGN KEY (sensdatalink) REFERENCES public.sensitivitydata(sensitivityregistry);


ALTER TABLE ONLY public.usagerecords
ADD CONSTRAINT usagerecords_showcaserefused_fkey FOREIGN KEY (showcaserefused) REFERENCES public.showcases(showcasereg);


GRANT ALL ON SCHEMA public TO PUBLIC;

