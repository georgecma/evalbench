ALTER TABLE ONLY public.auditandcompliance
ADD CONSTRAINT auditandcompliance_pkey PRIMARY KEY (audittrace);


ALTER TABLE ONLY public.compliance
ADD CONSTRAINT compliance_pkey PRIMARY KEY (compliancetrace);


ALTER TABLE ONLY public.dataflow
ADD CONSTRAINT dataflow_pkey PRIMARY KEY (recordregistry);


ALTER TABLE ONLY public.dataprofile
ADD CONSTRAINT dataprofile_pkey PRIMARY KEY (profiletrace);


ALTER TABLE ONLY public.riskmanagement
ADD CONSTRAINT riskmanagement_pkey PRIMARY KEY (risktrace);


ALTER TABLE ONLY public.securityprofile
ADD CONSTRAINT securityprofile_pkey PRIMARY KEY (securitytrace);


ALTER TABLE ONLY public.vendormanagement
ADD CONSTRAINT vendormanagement_pkey PRIMARY KEY (vendortrace);


ALTER TABLE ONLY public.auditandcompliance
ADD CONSTRAINT enforce_audit_compliance FOREIGN KEY (compjoin) REFERENCES public.compliance(compliancetrace);


ALTER TABLE ONLY public.auditandcompliance
ADD CONSTRAINT enforce_audit_profile FOREIGN KEY (profjoin) REFERENCES public.dataprofile(profiletrace);


ALTER TABLE ONLY public.auditandcompliance
ADD CONSTRAINT enforce_audit_vendor FOREIGN KEY (vendjoin) REFERENCES public.vendormanagement(vendortrace);


ALTER TABLE ONLY public.compliance
ADD CONSTRAINT enforce_compliance_risk FOREIGN KEY (risktie) REFERENCES public.riskmanagement(risktrace);


ALTER TABLE ONLY public.compliance
ADD CONSTRAINT enforce_compliance_vendor FOREIGN KEY (vendortie) REFERENCES public.vendormanagement(vendortrace);


ALTER TABLE ONLY public.dataprofile
ADD CONSTRAINT enforce_profile_flow FOREIGN KEY (flowsign) REFERENCES public.dataflow(recordregistry);


ALTER TABLE ONLY public.dataprofile
ADD CONSTRAINT enforce_profile_risk FOREIGN KEY (riskjoin) REFERENCES public.riskmanagement(risktrace);


ALTER TABLE ONLY public.riskmanagement
ADD CONSTRAINT enforce_risk_flow FOREIGN KEY (flowlink) REFERENCES public.dataflow(recordregistry);


ALTER TABLE ONLY public.securityprofile
ADD CONSTRAINT enforce_security_flow FOREIGN KEY (flowkey) REFERENCES public.dataflow(recordregistry);


ALTER TABLE ONLY public.securityprofile
ADD CONSTRAINT enforce_security_profile FOREIGN KEY (profilekey) REFERENCES public.dataprofile(profiletrace);


ALTER TABLE ONLY public.securityprofile
ADD CONSTRAINT enforce_security_risk FOREIGN KEY (riskkey) REFERENCES public.riskmanagement(risktrace);


ALTER TABLE ONLY public.vendormanagement
ADD CONSTRAINT enforce_vendor_risk FOREIGN KEY (riskassoc) REFERENCES public.riskmanagement(risktrace);


ALTER TABLE ONLY public.vendormanagement
ADD CONSTRAINT enforce_vendor_security FOREIGN KEY (secjoin) REFERENCES public.securityprofile(securitytrace);


GRANT ALL ON SCHEMA public TO PUBLIC;

