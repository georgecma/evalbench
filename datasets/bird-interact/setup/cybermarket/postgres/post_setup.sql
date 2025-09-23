ALTER TABLE ONLY public.buyers
ADD CONSTRAINT buyers_pkey PRIMARY KEY (buyregistry);


ALTER TABLE ONLY public.communication
ADD CONSTRAINT communication_pkey PRIMARY KEY (commregistry);


ALTER TABLE ONLY public.investigation
ADD CONSTRAINT investigation_pkey PRIMARY KEY (investregistry);


ALTER TABLE ONLY public.markets
ADD CONSTRAINT markets_pkey PRIMARY KEY (mktregistry);


ALTER TABLE ONLY public.products
ADD CONSTRAINT products_pkey PRIMARY KEY (prodregistry);


ALTER TABLE ONLY public.riskanalysis
ADD CONSTRAINT riskanalysis_pkey PRIMARY KEY (riskregistry);


ALTER TABLE ONLY public.securitymonitoring
ADD CONSTRAINT securitymonitoring_pkey PRIMARY KEY (secmonregistry);


ALTER TABLE ONLY public.transactions
ADD CONSTRAINT transactions_pkey PRIMARY KEY (txregistry);


ALTER TABLE ONLY public.transactions
ADD CONSTRAINT transactions_rectag_key UNIQUE (rectag);


ALTER TABLE ONLY public.vendors
ADD CONSTRAINT vendors_pkey PRIMARY KEY (vendregistry);


ALTER TABLE ONLY public.buyers
ADD CONSTRAINT fk_buy_to_markets FOREIGN KEY (mktref) REFERENCES public.markets(mktregistry);


ALTER TABLE ONLY public.buyers
ADD CONSTRAINT fk_buy_to_vendors FOREIGN KEY (vendref) REFERENCES public.vendors(vendregistry);


ALTER TABLE ONLY public.communication
ADD CONSTRAINT fk_comm_to_products FOREIGN KEY (prodref) REFERENCES public.products(prodregistry);


ALTER TABLE ONLY public.communication
ADD CONSTRAINT fk_comm_to_tx FOREIGN KEY (txref) REFERENCES public.transactions(txregistry);


ALTER TABLE ONLY public.investigation
ADD CONSTRAINT fk_invest_to_risk FOREIGN KEY (riskref) REFERENCES public.riskanalysis(riskregistry);


ALTER TABLE ONLY public.investigation
ADD CONSTRAINT fk_invest_to_secmon FOREIGN KEY (secref) REFERENCES public.securitymonitoring(secmonregistry);


ALTER TABLE ONLY public.products
ADD CONSTRAINT fk_prod_to_buyers FOREIGN KEY (buyref) REFERENCES public.buyers(buyregistry);


ALTER TABLE ONLY public.products
ADD CONSTRAINT fk_prod_to_vendors FOREIGN KEY (vendref) REFERENCES public.vendors(vendregistry);


ALTER TABLE ONLY public.riskanalysis
ADD CONSTRAINT fk_risk_to_comm FOREIGN KEY (commref) REFERENCES public.communication(commregistry);


ALTER TABLE ONLY public.riskanalysis
ADD CONSTRAINT fk_risk_to_tx FOREIGN KEY (txref) REFERENCES public.transactions(txregistry);


ALTER TABLE ONLY public.securitymonitoring
ADD CONSTRAINT fk_secmon_to_comm FOREIGN KEY (commref) REFERENCES public.communication(commregistry);


ALTER TABLE ONLY public.securitymonitoring
ADD CONSTRAINT fk_secmon_to_risk FOREIGN KEY (riskref) REFERENCES public.riskanalysis(riskregistry);


ALTER TABLE ONLY public.transactions
ADD CONSTRAINT fk_tx_to_buyers FOREIGN KEY (buyref) REFERENCES public.buyers(buyregistry);


ALTER TABLE ONLY public.transactions
ADD CONSTRAINT fk_tx_to_markets FOREIGN KEY (mktref) REFERENCES public.markets(mktregistry);


ALTER TABLE ONLY public.transactions
ADD CONSTRAINT fk_tx_to_products FOREIGN KEY (prodref) REFERENCES public.products(prodregistry);


ALTER TABLE ONLY public.vendors
ADD CONSTRAINT fk_vend_to_markets FOREIGN KEY (mktref) REFERENCES public.markets(mktregistry);


GRANT ALL ON SCHEMA public TO PUBLIC;

