
ALTER TABLE ONLY public.equipment
ADD CONSTRAINT equipment_pkey PRIMARY KEY (equipregistry);


ALTER TABLE ONLY public.personnel
ADD CONSTRAINT personnel_pkey PRIMARY KEY (crewregistry);


ALTER TABLE ONLY public.projects
ADD CONSTRAINT projects_pkey PRIMARY KEY (arcregistry);


ALTER TABLE ONLY public.scanconservation
ADD CONSTRAINT scanconservation_pkey PRIMARY KEY (cureregistry);


ALTER TABLE ONLY public.scanenvironment
ADD CONSTRAINT scanenvironment_pkey PRIMARY KEY (airregistry);


ALTER TABLE ONLY public.scanfeatures
ADD CONSTRAINT scanfeatures_pkey PRIMARY KEY (traitregistry);


ALTER TABLE ONLY public.scanmesh
ADD CONSTRAINT scanmesh_pkey PRIMARY KEY (facetregistry);


ALTER TABLE ONLY public.scanpointcloud
ADD CONSTRAINT scanpointcloud_pkey PRIMARY KEY (cloudregistry);


ALTER TABLE ONLY public.scanprocessing
ADD CONSTRAINT scanprocessing_pkey PRIMARY KEY (flowregistry);


ALTER TABLE ONLY public.scanqc
ADD CONSTRAINT scanqc_pkey PRIMARY KEY (qualregistry);


ALTER TABLE ONLY public.scanregistration
ADD CONSTRAINT scanregistration_pkey PRIMARY KEY (logregistry);


ALTER TABLE ONLY public.scans
ADD CONSTRAINT scans_pkey PRIMARY KEY (questregistry);


ALTER TABLE ONLY public.scanspatial
ADD CONSTRAINT scanspatial_pkey PRIMARY KEY (domainregistry);


ALTER TABLE ONLY public.sites
ADD CONSTRAINT sites_pkey PRIMARY KEY (zoneregistry);


ALTER TABLE ONLY public.scanconservation
ADD CONSTRAINT scanconservation_arcref_fkey FOREIGN KEY (arcref) REFERENCES public.projects(arcregistry);


ALTER TABLE ONLY public.scanconservation
ADD CONSTRAINT scanconservation_zoneref_fkey FOREIGN KEY (zoneref) REFERENCES public.sites(zoneregistry);


ALTER TABLE ONLY public.scanenvironment
ADD CONSTRAINT scanenvironment_equipref_fkey FOREIGN KEY (equipref) REFERENCES public.equipment(equipregistry);


ALTER TABLE ONLY public.scanenvironment
ADD CONSTRAINT scanenvironment_zoneref_fkey FOREIGN KEY (zoneref) REFERENCES public.sites(zoneregistry);


ALTER TABLE ONLY public.scanfeatures
ADD CONSTRAINT scanfeatures_equipref_fkey FOREIGN KEY (equipref) REFERENCES public.equipment(equipregistry);


ALTER TABLE ONLY public.scanfeatures
ADD CONSTRAINT scanfeatures_zoneref_fkey FOREIGN KEY (zoneref) REFERENCES public.sites(zoneregistry);


ALTER TABLE ONLY public.scanmesh
ADD CONSTRAINT scanmesh_equipref_fkey FOREIGN KEY (equipref) REFERENCES public.equipment(equipregistry);


ALTER TABLE ONLY public.scanmesh
ADD CONSTRAINT scanmesh_zoneref_fkey FOREIGN KEY (zoneref) REFERENCES public.sites(zoneregistry);


ALTER TABLE ONLY public.scanpointcloud
ADD CONSTRAINT scanpointcloud_arcref_fkey FOREIGN KEY (arcref) REFERENCES public.projects(arcregistry);


ALTER TABLE ONLY public.scanpointcloud
ADD CONSTRAINT scanpointcloud_crewref_fkey FOREIGN KEY (crewref) REFERENCES public.personnel(crewregistry);


ALTER TABLE ONLY public.scanprocessing
ADD CONSTRAINT scanprocessing_equipref_fkey FOREIGN KEY (equipref) REFERENCES public.equipment(equipregistry);


ALTER TABLE ONLY public.scanprocessing
ADD CONSTRAINT scanprocessing_zoneref_fkey FOREIGN KEY (zoneref) REFERENCES public.sites(zoneregistry);


ALTER TABLE ONLY public.scanqc
ADD CONSTRAINT scanqc_arcref_fkey FOREIGN KEY (arcref) REFERENCES public.projects(arcregistry);


ALTER TABLE ONLY public.scanqc
ADD CONSTRAINT scanqc_crewref_fkey FOREIGN KEY (crewref) REFERENCES public.personnel(crewregistry);


ALTER TABLE ONLY public.scanregistration
ADD CONSTRAINT scanregistration_arcref_fkey FOREIGN KEY (arcref) REFERENCES public.projects(arcregistry);


ALTER TABLE ONLY public.scanregistration
ADD CONSTRAINT scanregistration_crewref_fkey FOREIGN KEY (crewref) REFERENCES public.personnel(crewregistry);


ALTER TABLE ONLY public.scans
ADD CONSTRAINT scans_arcref_fkey FOREIGN KEY (arcref) REFERENCES public.projects(arcregistry);


ALTER TABLE ONLY public.scans
ADD CONSTRAINT scans_crewref_fkey FOREIGN KEY (crewref) REFERENCES public.personnel(crewregistry);


ALTER TABLE ONLY public.scans
ADD CONSTRAINT scans_zoneref_fkey FOREIGN KEY (zoneref) REFERENCES public.sites(zoneregistry);


ALTER TABLE ONLY public.scanspatial
ADD CONSTRAINT scanspatial_arcref_fkey FOREIGN KEY (arcref) REFERENCES public.projects(arcregistry);


ALTER TABLE ONLY public.scanspatial
ADD CONSTRAINT scanspatial_crewref_fkey FOREIGN KEY (crewref) REFERENCES public.personnel(crewregistry);

