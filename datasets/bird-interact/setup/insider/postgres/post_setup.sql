ALTER TABLE ONLY public.advancedbehavior
ADD CONSTRAINT advancedbehavior_pkey PRIMARY KEY (abhvreg);


ALTER TABLE ONLY public.compliancecase
ADD CONSTRAINT compliancecase_pkey PRIMARY KEY (compreg);


ALTER TABLE ONLY public.enforcementactions
ADD CONSTRAINT enforcementactions_pkey PRIMARY KEY (enforcereg);


ALTER TABLE ONLY public.investigationdetails
ADD CONSTRAINT investigationdetails_pkey PRIMARY KEY (invdetreg);


ALTER TABLE ONLY public.sentimentandfundamentals
ADD CONSTRAINT sentimentandfundamentals_pkey PRIMARY KEY (sentreg);


ALTER TABLE ONLY public.trader
ADD CONSTRAINT trader_pkey PRIMARY KEY (tradereg);


ALTER TABLE ONLY public.transactionrecord
ADD CONSTRAINT transactionrecord_pkey PRIMARY KEY (transreg);


ALTER TABLE ONLY public.advancedbehavior
ADD CONSTRAINT advancedbehavior_translink_fkey FOREIGN KEY (translink) REFERENCES public.transactionrecord(transreg);


ALTER TABLE ONLY public.compliancecase
ADD CONSTRAINT compliancecase_abhvref_fkey FOREIGN KEY (abhvref) REFERENCES public.advancedbehavior(abhvreg);


ALTER TABLE ONLY public.compliancecase
ADD CONSTRAINT compliancecase_transref_fkey FOREIGN KEY (transref) REFERENCES public.transactionrecord(transreg);


ALTER TABLE ONLY public.enforcementactions
ADD CONSTRAINT enforcementactions_compref2_fkey FOREIGN KEY (compref2) REFERENCES public.compliancecase(compreg);


ALTER TABLE ONLY public.enforcementactions
ADD CONSTRAINT enforcementactions_invdetref_fkey FOREIGN KEY (invdetref) REFERENCES public.investigationdetails(invdetreg);


ALTER TABLE ONLY public.investigationdetails
ADD CONSTRAINT investigationdetails_compref_fkey FOREIGN KEY (compref) REFERENCES public.compliancecase(compreg);


ALTER TABLE ONLY public.investigationdetails
ADD CONSTRAINT investigationdetails_sentref_fkey FOREIGN KEY (sentref) REFERENCES public.sentimentandfundamentals(sentreg);


ALTER TABLE ONLY public.sentimentandfundamentals
ADD CONSTRAINT sentimentandfundamentals_transref_fkey FOREIGN KEY (transref) REFERENCES public.transactionrecord(transreg);


ALTER TABLE ONLY public.transactionrecord
ADD CONSTRAINT transactionrecord_trdref_fkey FOREIGN KEY (trdref) REFERENCES public.trader(tradereg);


GRANT ALL ON SCHEMA public TO PUBLIC;

