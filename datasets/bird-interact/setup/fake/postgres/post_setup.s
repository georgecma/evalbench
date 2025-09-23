ALTER TABLE ONLY public.account
ADD CONSTRAINT account_pkey PRIMARY KEY (accindex);


ALTER TABLE ONLY public.contentbehavior
ADD CONSTRAINT contentbehavior_pkey PRIMARY KEY (cntref);


ALTER TABLE ONLY public.messaginganalysis
ADD CONSTRAINT messaginganalysis_pkey PRIMARY KEY (msgkey);


ALTER TABLE ONLY public.moderationaction
ADD CONSTRAINT moderationaction_pkey PRIMARY KEY (modactkey);


ALTER TABLE ONLY public.networkmetrics
ADD CONSTRAINT networkmetrics_pkey PRIMARY KEY (netkey);


ALTER TABLE ONLY public.profile
ADD CONSTRAINT profile_pkey PRIMARY KEY (profkey);


ALTER TABLE ONLY public.securitydetection
ADD CONSTRAINT securitydetection_pkey PRIMARY KEY (secdetkey);


ALTER TABLE ONLY public.sessionbehavior
ADD CONSTRAINT sessionbehavior_pkey PRIMARY KEY (sessref);


ALTER TABLE ONLY public.technicalinfo
ADD CONSTRAINT technicalinfo_pkey PRIMARY KEY (techkey);


ALTER TABLE ONLY public.contentbehavior
ADD CONSTRAINT contentbehavior_cntsessref_fkey FOREIGN KEY (cntsessref) REFERENCES public.sessionbehavior(sessref);


ALTER TABLE ONLY public.messaginganalysis
ADD CONSTRAINT messaginganalysis_msgcntref_fkey FOREIGN KEY (msgcntref) REFERENCES public.contentbehavior(cntref);


ALTER TABLE ONLY public.messaginganalysis
ADD CONSTRAINT messaginganalysis_msgnetref_fkey FOREIGN KEY (msgnetref) REFERENCES public.networkmetrics(netkey);


ALTER TABLE ONLY public.moderationaction
ADD CONSTRAINT moderationaction_macntref_fkey FOREIGN KEY (macntref) REFERENCES public.contentbehavior(cntref);


ALTER TABLE ONLY public.moderationaction
ADD CONSTRAINT moderationaction_masedetref_fkey FOREIGN KEY (masedetref) REFERENCES public.securitydetection(secdetkey);


ALTER TABLE ONLY public.networkmetrics
ADD CONSTRAINT networkmetrics_netsessref_fkey FOREIGN KEY (netsessref) REFERENCES public.sessionbehavior(sessref);


ALTER TABLE ONLY public.profile
ADD CONSTRAINT profile_profaccref_fkey FOREIGN KEY (profaccref) REFERENCES public.account(accindex);


ALTER TABLE ONLY public.securitydetection
ADD CONSTRAINT securitydetection_sectechref_fkey FOREIGN KEY (sectechref) REFERENCES public.technicalinfo(techkey);


ALTER TABLE ONLY public.sessionbehavior
ADD CONSTRAINT sessionbehavior_sessprofref_fkey FOREIGN KEY (sessprofref) REFERENCES public.profile(profkey);


ALTER TABLE ONLY public.technicalinfo
ADD CONSTRAINT technicalinfo_techmsgref_fkey FOREIGN KEY (techmsgref) REFERENCES public.messaginganalysis(msgkey);


ALTER TABLE ONLY public.technicalinfo
ADD CONSTRAINT technicalinfo_technetref_fkey FOREIGN KEY (technetref) REFERENCES public.networkmetrics(netkey);


GRANT ALL ON SCHEMA public TO PUBLIC;

