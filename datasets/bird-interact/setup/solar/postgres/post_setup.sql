ALTER TABLE ONLY public.alerts
ADD CONSTRAINT alerts_pkey PRIMARY KEY (alertreg);


ALTER TABLE ONLY public.electrical
ADD CONSTRAINT electrical_pkey PRIMARY KEY (elecregistry);


ALTER TABLE ONLY public.environment
ADD CONSTRAINT environment_pkey PRIMARY KEY (envregistry);


ALTER TABLE ONLY public.inverter
ADD CONSTRAINT inverter_pkey PRIMARY KEY (invertregistry);


ALTER TABLE ONLY public.maintenance
ADD CONSTRAINT maintenance_pkey PRIMARY KEY (maintregistry);


ALTER TABLE ONLY public.panel
ADD CONSTRAINT panel_pkey PRIMARY KEY (panemark);


ALTER TABLE ONLY public.performance
ADD CONSTRAINT performance_pkey PRIMARY KEY (perfregistry);


ALTER TABLE ONLY public.plant
ADD CONSTRAINT plant_pkey PRIMARY KEY (growregistry);


ALTER TABLE ONLY public.alerts
ADD CONSTRAINT alerts_compreg_fkey FOREIGN KEY (compreg) REFERENCES public.plant(growregistry);


ALTER TABLE ONLY public.alerts
ADD CONSTRAINT alerts_deviceref_fkey FOREIGN KEY (deviceref) REFERENCES public.panel(panemark);


ALTER TABLE ONLY public.alerts
ADD CONSTRAINT alerts_incidentref_fkey FOREIGN KEY (incidentref) REFERENCES public.performance(perfregistry);


ALTER TABLE ONLY public.electrical
ADD CONSTRAINT electrical_efflogref_fkey FOREIGN KEY (efflogref) REFERENCES public.performance(perfregistry);


ALTER TABLE ONLY public.electrical
ADD CONSTRAINT electrical_engyunitref_fkey FOREIGN KEY (engyunitref) REFERENCES public.panel(panemark);


ALTER TABLE ONLY public.environment
ADD CONSTRAINT environment_arearegistry_fkey FOREIGN KEY (arearegistry) REFERENCES public.plant(growregistry);


ALTER TABLE ONLY public.inverter
ADD CONSTRAINT inverter_siteref_fkey FOREIGN KEY (siteref) REFERENCES public.plant(growregistry);


ALTER TABLE ONLY public.maintenance
ADD CONSTRAINT maintenance_compref_fkey FOREIGN KEY (compref) REFERENCES public.panel(panemark);


ALTER TABLE ONLY public.maintenance
ADD CONSTRAINT maintenance_obsref_fkey FOREIGN KEY (obsref) REFERENCES public.performance(perfregistry);


ALTER TABLE ONLY public.maintenance
ADD CONSTRAINT maintenance_powerref_fkey FOREIGN KEY (powerref) REFERENCES public.plant(growregistry);


ALTER TABLE ONLY public.panel
ADD CONSTRAINT panel_hubregistry_fkey FOREIGN KEY (hubregistry) REFERENCES public.plant(growregistry);


ALTER TABLE ONLY public.performance
ADD CONSTRAINT performance_solmodref_fkey FOREIGN KEY (solmodref) REFERENCES public.panel(panemark);


GRANT ALL ON SCHEMA public TO PUBLIC;

