ALTER TABLE ONLY public.bank_and_transactions
ADD CONSTRAINT bank_and_transactions_pkey PRIMARY KEY (bankexpref);


ALTER TABLE ONLY public.core_record
ADD CONSTRAINT core_record_pkey PRIMARY KEY (coreregistry);


ALTER TABLE ONLY public.credit_accounts_and_history
ADD CONSTRAINT credit_accounts_and_history_pkey PRIMARY KEY (histcompref);


ALTER TABLE ONLY public.credit_and_compliance
ADD CONSTRAINT credit_and_compliance_pkey PRIMARY KEY (compbankref);


ALTER TABLE ONLY public.employment_and_income
ADD CONSTRAINT employment_and_income_pkey PRIMARY KEY (emplcoreref);


ALTER TABLE ONLY public.expenses_and_assets
ADD CONSTRAINT expenses_and_assets_pkey PRIMARY KEY (expemplref);


ALTER TABLE ONLY public.bank_and_transactions
ADD CONSTRAINT bank_and_transactions_bankexpref_fkey FOREIGN KEY (bankexpref) REFERENCES public.expenses_and_assets(expemplref);


ALTER TABLE ONLY public.credit_accounts_and_history
ADD CONSTRAINT credit_accounts_and_history_histcompref_fkey FOREIGN KEY (histcompref) REFERENCES public.credit_and_compliance(compbankref);


ALTER TABLE ONLY public.credit_and_compliance
ADD CONSTRAINT credit_and_compliance_compbankref_fkey FOREIGN KEY (compbankref) REFERENCES public.bank_and_transactions(bankexpref);


ALTER TABLE ONLY public.employment_and_income
ADD CONSTRAINT employment_and_income_emplcoreref_fkey FOREIGN KEY (emplcoreref) REFERENCES public.core_record(coreregistry);


ALTER TABLE ONLY public.expenses_and_assets
ADD CONSTRAINT expenses_and_assets_expemplref_fkey FOREIGN KEY (expemplref) REFERENCES public.employment_and_income(emplcoreref);


GRANT ALL ON SCHEMA public TO PUBLIC;

