ALTER TABLE ONLY public.cabinenvironment
ADD CONSTRAINT cabinenvironment_pkey PRIMARY KEY (cabinregistry);


ALTER TABLE ONLY public.chassisandvehicle
ADD CONSTRAINT chassisandvehicle_pkey PRIMARY KEY (chassisregistry);


ALTER TABLE ONLY public.communication
ADD CONSTRAINT communication_pkey PRIMARY KEY (commregistry);


ALTER TABLE ONLY public.engineandfluids
ADD CONSTRAINT engineandfluids_pkey PRIMARY KEY (engineregistry);


ALTER TABLE ONLY public.equipment
ADD CONSTRAINT equipment_pkey PRIMARY KEY (equipmentcode);


ALTER TABLE ONLY public.lightingandsafety
ADD CONSTRAINT lightingandsafety_pkey PRIMARY KEY (lightregistry);


ALTER TABLE ONLY public.location
ADD CONSTRAINT location_pkey PRIMARY KEY (locationregistry);


ALTER TABLE ONLY public.operationmaintenance
ADD CONSTRAINT operationmaintenance_pkey PRIMARY KEY (opmaintregistry);


ALTER TABLE ONLY public.powerbattery
ADD CONSTRAINT powerbattery_pkey PRIMARY KEY (powerbattregistry);


ALTER TABLE ONLY public.scientific
ADD CONSTRAINT scientific_pkey PRIMARY KEY (sciregistry);


ALTER TABLE ONLY public.thermalsolarwindandgrid
ADD CONSTRAINT thermalsolarwindandgrid_pkey PRIMARY KEY (thermalregistry);


ALTER TABLE ONLY public.transmission
ADD CONSTRAINT transmission_pkey PRIMARY KEY (transregistry);


ALTER TABLE ONLY public.waterandwaste
ADD CONSTRAINT waterandwaste_pkey PRIMARY KEY (waterregistry);


ALTER TABLE ONLY public.weatherandstructure
ADD CONSTRAINT weatherandstructure_pkey PRIMARY KEY (weatherregistry);


ALTER TABLE ONLY public.cabinenvironment
ADD CONSTRAINT cabinenvironment_cabincommref_fkey FOREIGN KEY (cabincommref) REFERENCES public.communication(commregistry);


ALTER TABLE ONLY public.cabinenvironment
ADD CONSTRAINT cabinenvironment_cabineqref_fkey FOREIGN KEY (cabineqref) REFERENCES public.equipment(equipmentcode);


ALTER TABLE ONLY public.cabinenvironment
ADD CONSTRAINT cabinenvironment_cabinlocref_fkey FOREIGN KEY (cabinlocref) REFERENCES public.location(locationregistry);


ALTER TABLE ONLY public.chassisandvehicle
ADD CONSTRAINT chassisandvehicle_chassisengref_fkey FOREIGN KEY (chassisengref) REFERENCES public.engineandfluids(engineregistry);


ALTER TABLE ONLY public.chassisandvehicle
ADD CONSTRAINT chassisandvehicle_chassiseqref_fkey FOREIGN KEY (chassiseqref) REFERENCES public.equipment(equipmentcode);


ALTER TABLE ONLY public.chassisandvehicle
ADD CONSTRAINT chassisandvehicle_chassistransref_fkey FOREIGN KEY (chassistransref) REFERENCES public.transmission(transregistry);


ALTER TABLE ONLY public.communication
ADD CONSTRAINT communication_commeqref_fkey FOREIGN KEY (commeqref) REFERENCES public.equipment(equipmentcode);


ALTER TABLE ONLY public.communication
ADD CONSTRAINT communication_commlocref_fkey FOREIGN KEY (commlocref) REFERENCES public.location(locationregistry);


ALTER TABLE ONLY public.engineandfluids
ADD CONSTRAINT engineandfluids_engfluidseqref_fkey FOREIGN KEY (engfluidseqref) REFERENCES public.equipment(equipmentcode);


ALTER TABLE ONLY public.engineandfluids
ADD CONSTRAINT engineandfluids_engfluidspbref_fkey FOREIGN KEY (engfluidspbref) REFERENCES public.powerbattery(powerbattregistry);


ALTER TABLE ONLY public.lightingandsafety
ADD CONSTRAINT lightingandsafety_lighteqref_fkey FOREIGN KEY (lighteqref) REFERENCES public.equipment(equipmentcode);


ALTER TABLE ONLY public.location
ADD CONSTRAINT location_loceqref_fkey FOREIGN KEY (loceqref) REFERENCES public.equipment(equipmentcode);


ALTER TABLE ONLY public.operationmaintenance
ADD CONSTRAINT operationmaintenance_opmainteqref_fkey FOREIGN KEY (opmainteqref) REFERENCES public.equipment(equipmentcode);


ALTER TABLE ONLY public.operationmaintenance
ADD CONSTRAINT operationmaintenance_opmaintlocref_fkey FOREIGN KEY (opmaintlocref) REFERENCES public.location(locationregistry);


ALTER TABLE ONLY public.powerbattery
ADD CONSTRAINT powerbattery_pwrbatteqref_fkey FOREIGN KEY (pwrbatteqref) REFERENCES public.equipment(equipmentcode);


ALTER TABLE ONLY public.scientific
ADD CONSTRAINT scientific_scieqref_fkey FOREIGN KEY (scieqref) REFERENCES public.equipment(equipmentcode);


ALTER TABLE ONLY public.thermalsolarwindandgrid
ADD CONSTRAINT thermalsolarwindandgrid_thermalcommref_fkey FOREIGN KEY (thermalcommref) REFERENCES public.communication(commregistry);


ALTER TABLE ONLY public.thermalsolarwindandgrid
ADD CONSTRAINT thermalsolarwindandgrid_thermaleqref_fkey FOREIGN KEY (thermaleqref) REFERENCES public.equipment(equipmentcode);


ALTER TABLE ONLY public.thermalsolarwindandgrid
ADD CONSTRAINT thermalsolarwindandgrid_thermalpowerref_fkey FOREIGN KEY (thermalpowerref) REFERENCES public.powerbattery(powerbattregistry);


ALTER TABLE ONLY public.transmission
ADD CONSTRAINT transmission_transengfluidsref_fkey FOREIGN KEY (transengfluidsref) REFERENCES public.engineandfluids(engineregistry);


ALTER TABLE ONLY public.transmission
ADD CONSTRAINT transmission_transeqref_fkey FOREIGN KEY (transeqref) REFERENCES public.equipment(equipmentcode);


ALTER TABLE ONLY public.waterandwaste
ADD CONSTRAINT waterandwaste_watereqref_fkey FOREIGN KEY (watereqref) REFERENCES public.equipment(equipmentcode);


ALTER TABLE ONLY public.weatherandstructure
ADD CONSTRAINT weatherandstructure_weatherlocref_fkey FOREIGN KEY (weatherlocref) REFERENCES public.location(locationregistry);


ALTER TABLE ONLY public.weatherandstructure
ADD CONSTRAINT weatherandstructure_weatheropmaintref_fkey FOREIGN KEY (weatheropmaintref) REFERENCES public.operationmaintenance(opmaintregistry);


GRANT ALL ON SCHEMA public TO PUBLIC;

