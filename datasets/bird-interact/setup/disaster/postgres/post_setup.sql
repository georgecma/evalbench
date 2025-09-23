ALTER TABLE ONLY public.beneficiariesandassessments
ADD CONSTRAINT beneficiariesandassessments_pkey PRIMARY KEY (beneregistry);


ALTER TABLE ONLY public.coordinationandevaluation
ADD CONSTRAINT coordinationandevaluation_pkey PRIMARY KEY (coordevalregistry);


ALTER TABLE ONLY public.disasterevents
ADD CONSTRAINT disasterevents_pkey PRIMARY KEY (distregistry);


ALTER TABLE ONLY public.distributionhubs
ADD CONSTRAINT distributionhubs_pkey PRIMARY KEY (hubregistry);


ALTER TABLE ONLY public.environmentandhealth
ADD CONSTRAINT environmentandhealth_pkey PRIMARY KEY (envhealthregistry);


ALTER TABLE ONLY public.financials
ADD CONSTRAINT financials_pkey PRIMARY KEY (financeregistry);


ALTER TABLE ONLY public.humanresources
ADD CONSTRAINT humanresources_pkey PRIMARY KEY (hrregistry);


ALTER TABLE ONLY public.operations
ADD CONSTRAINT operations_pkey PRIMARY KEY (opsregistry);


ALTER TABLE ONLY public.supplies
ADD CONSTRAINT supplies_pkey PRIMARY KEY (supplyregistry);


ALTER TABLE ONLY public.transportation
ADD CONSTRAINT transportation_pkey PRIMARY KEY (transportregistry);


ALTER TABLE ONLY public.beneficiariesandassessments
ADD CONSTRAINT beneficiariesandassessments_benedistref_fkey FOREIGN KEY (benedistref) REFERENCES public.disasterevents(distregistry);


ALTER TABLE ONLY public.beneficiariesandassessments
ADD CONSTRAINT beneficiariesandassessments_beneopsref_fkey FOREIGN KEY (beneopsref) REFERENCES public.operations(opsregistry);


ALTER TABLE ONLY public.coordinationandevaluation
ADD CONSTRAINT coordinationandevaluation_coorddistref_fkey FOREIGN KEY (coorddistref) REFERENCES public.disasterevents(distregistry);


ALTER TABLE ONLY public.coordinationandevaluation
ADD CONSTRAINT coordinationandevaluation_coordopsref_fkey FOREIGN KEY (coordopsref) REFERENCES public.operations(opsregistry);


ALTER TABLE ONLY public.distributionhubs
ADD CONSTRAINT distributionhubs_disteventref_fkey FOREIGN KEY (disteventref) REFERENCES public.disasterevents(distregistry);


ALTER TABLE ONLY public.environmentandhealth
ADD CONSTRAINT environmentandhealth_envdistref_fkey FOREIGN KEY (envdistref) REFERENCES public.disasterevents(distregistry);


ALTER TABLE ONLY public.financials
ADD CONSTRAINT financials_findistref_fkey FOREIGN KEY (findistref) REFERENCES public.disasterevents(distregistry);


ALTER TABLE ONLY public.financials
ADD CONSTRAINT financials_finopsref_fkey FOREIGN KEY (finopsref) REFERENCES public.operations(opsregistry);


ALTER TABLE ONLY public.humanresources
ADD CONSTRAINT humanresources_hrdistref_fkey FOREIGN KEY (hrdistref) REFERENCES public.disasterevents(distregistry);


ALTER TABLE ONLY public.humanresources
ADD CONSTRAINT humanresources_hropsref_fkey FOREIGN KEY (hropsref) REFERENCES public.operations(opsregistry);


ALTER TABLE ONLY public.operations
ADD CONSTRAINT operations_opsdistref_fkey FOREIGN KEY (opsdistref) REFERENCES public.disasterevents(distregistry);


ALTER TABLE ONLY public.operations
ADD CONSTRAINT operations_opshubref_fkey FOREIGN KEY (opshubref) REFERENCES public.distributionhubs(hubregistry);


ALTER TABLE ONLY public.supplies
ADD CONSTRAINT supplies_supplydistref_fkey FOREIGN KEY (supplydistref) REFERENCES public.disasterevents(distregistry);


ALTER TABLE ONLY public.supplies
ADD CONSTRAINT supplies_supplyhubref_fkey FOREIGN KEY (supplyhubref) REFERENCES public.distributionhubs(hubregistry);


ALTER TABLE ONLY public.transportation
ADD CONSTRAINT transportation_transportdistref_fkey FOREIGN KEY (transportdistref) REFERENCES public.disasterevents(distregistry);


ALTER TABLE ONLY public.transportation
ADD CONSTRAINT transportation_transporthubref_fkey FOREIGN KEY (transporthubref) REFERENCES public.distributionhubs(hubregistry);


ALTER TABLE ONLY public.transportation
ADD CONSTRAINT transportation_transportsupref_fkey FOREIGN KEY (transportsupref) REFERENCES public.supplies(supplyregistry);


GRANT ALL ON SCHEMA public TO PUBLIC;

