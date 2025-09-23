ALTER TABLE ONLY public.container
ADD CONSTRAINT container_pkey PRIMARY KEY (containregistry);


ALTER TABLE ONLY public.datalogger
ADD CONSTRAINT datalogger_pkey PRIMARY KEY (loggerreg);


ALTER TABLE ONLY public.regulatoryandmaintenance
ADD CONSTRAINT regulatoryandmaintenance_pkey PRIMARY KEY (regmaintregistry);


ALTER TABLE ONLY public.sensordata
ADD CONSTRAINT sensordata_pkey PRIMARY KEY (sensortrack);


ALTER TABLE ONLY public.shipments
ADD CONSTRAINT shipments_pkey PRIMARY KEY (shipmentregistry);


ALTER TABLE ONLY public.transportinfo
ADD CONSTRAINT transportinfo_pkey PRIMARY KEY (vehiclereg);


ALTER TABLE ONLY public.vaccinedetails
ADD CONSTRAINT vaccinedetails_pkey PRIMARY KEY (vacregistry);


ALTER TABLE ONLY public.container
ADD CONSTRAINT container_has_shipment FOREIGN KEY (shipown) REFERENCES public.shipments(shipmentregistry) ON DELETE CASCADE;


ALTER TABLE ONLY public.datalogger
ADD CONSTRAINT datalogger_links_container FOREIGN KEY (containlog) REFERENCES public.container(containregistry) ON DELETE CASCADE;


ALTER TABLE ONLY public.datalogger
ADD CONSTRAINT datalogger_links_shipment FOREIGN KEY (shiplog) REFERENCES public.shipments(shipmentregistry) ON DELETE SET NULL;


ALTER TABLE ONLY public.regulatoryandmaintenance
ADD CONSTRAINT maintenance_refs_shipment FOREIGN KEY (shipgov) REFERENCES public.shipments(shipmentregistry) ON DELETE CASCADE;


ALTER TABLE ONLY public.regulatoryandmaintenance
ADD CONSTRAINT maintenance_refs_vehicle FOREIGN KEY (vehgov) REFERENCES public.transportinfo(vehiclereg) ON DELETE CASCADE;


ALTER TABLE ONLY public.sensordata
ADD CONSTRAINT sensor_refs_container FOREIGN KEY (containlink) REFERENCES public.container(containregistry) ON DELETE CASCADE;


ALTER TABLE ONLY public.sensordata
ADD CONSTRAINT sensor_refs_vehicle FOREIGN KEY (vehsenseref) REFERENCES public.transportinfo(vehiclereg) ON DELETE SET NULL;


ALTER TABLE ONLY public.transportinfo
ADD CONSTRAINT transport_has_container FOREIGN KEY (containtransit) REFERENCES public.container(containregistry) ON DELETE SET NULL;


ALTER TABLE ONLY public.transportinfo
ADD CONSTRAINT transport_has_shipment FOREIGN KEY (shiptransit) REFERENCES public.shipments(shipmentregistry) ON DELETE SET NULL;


ALTER TABLE ONLY public.vaccinedetails
ADD CONSTRAINT vaccine_links_container FOREIGN KEY (containvac) REFERENCES public.container(containregistry) ON DELETE SET NULL;


ALTER TABLE ONLY public.vaccinedetails
ADD CONSTRAINT vaccine_links_shipment FOREIGN KEY (shipinject) REFERENCES public.shipments(shipmentregistry) ON DELETE CASCADE;


GRANT ALL ON SCHEMA public TO PUBLIC;

