ALTER TABLE ONLY public.assessmentbasics
ADD CONSTRAINT assessmentbasics_pkey PRIMARY KEY (abkey);


ALTER TABLE ONLY public.assessmentsocialanddiagnosis
ADD CONSTRAINT assessmentsocialanddiagnosis_pkey PRIMARY KEY (asdkey);


ALTER TABLE ONLY public.assessmentsymptomsandrisk
ADD CONSTRAINT assessmentsymptomsandrisk_pkey PRIMARY KEY (asrkey);


ALTER TABLE ONLY public.clinicians
ADD CONSTRAINT clinicians_pkey PRIMARY KEY (clinkey);


ALTER TABLE ONLY public.encounters
ADD CONSTRAINT encounters_pkey PRIMARY KEY (enckey);


ALTER TABLE ONLY public.facilities
ADD CONSTRAINT facilities_pkey PRIMARY KEY (fackey);


ALTER TABLE ONLY public.patients
ADD CONSTRAINT patients_pkey PRIMARY KEY (patkey);


ALTER TABLE ONLY public.treatmentbasics
ADD CONSTRAINT treatmentbasics_pkey PRIMARY KEY (txkey);


ALTER TABLE ONLY public.treatmentoutcomes
ADD CONSTRAINT treatmentoutcomes_pkey PRIMARY KEY (txoutkey);


ALTER TABLE ONLY public.assessmentsocialanddiagnosis
ADD CONSTRAINT fk_assess_socdiag FOREIGN KEY (asdkey) REFERENCES public.assessmentbasics(abkey);


ALTER TABLE ONLY public.assessmentsymptomsandrisk
ADD CONSTRAINT fk_assess_symrisk FOREIGN KEY (asrkey) REFERENCES public.assessmentbasics(abkey);


ALTER TABLE ONLY public.assessmentbasics
ADD CONSTRAINT fk_assessment_patient FOREIGN KEY (patownerref) REFERENCES public.patients(patkey);


ALTER TABLE ONLY public.clinicians
ADD CONSTRAINT fk_clinician_facility FOREIGN KEY (facconnect) REFERENCES public.facilities(fackey);


ALTER TABLE ONLY public.encounters
ADD CONSTRAINT fk_encounter_assessment FOREIGN KEY (abref) REFERENCES public.assessmentbasics(abkey);


ALTER TABLE ONLY public.encounters
ADD CONSTRAINT fk_encounter_patient FOREIGN KEY (patref) REFERENCES public.patients(patkey);


ALTER TABLE ONLY public.treatmentoutcomes
ADD CONSTRAINT fk_outcomes_treatment FOREIGN KEY (txref) REFERENCES public.treatmentbasics(txkey);


ALTER TABLE ONLY public.patients
ADD CONSTRAINT fk_patient_primaryclinician FOREIGN KEY (clinleadref) REFERENCES public.clinicians(clinkey);


ALTER TABLE ONLY public.treatmentbasics
ADD CONSTRAINT fk_treatment_encounter FOREIGN KEY (encref) REFERENCES public.encounters(enckey);


GRANT ALL ON SCHEMA public TO PUBLIC;

