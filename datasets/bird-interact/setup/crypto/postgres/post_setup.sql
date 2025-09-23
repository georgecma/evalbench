

ALTER TABLE ONLY public.accountbalances
ADD CONSTRAINT accountbalances_pkey PRIMARY KEY (accountbalancesnode);


ALTER TABLE ONLY public.analyticsindicators
ADD CONSTRAINT analyticsindicators_pkey PRIMARY KEY (analyticsindicatorsnode);


ALTER TABLE ONLY public.fees
ADD CONSTRAINT fees_pkey PRIMARY KEY (feesnode);


ALTER TABLE ONLY public.marketdata
ADD CONSTRAINT marketdata_pkey PRIMARY KEY (marketdatanode);


ALTER TABLE ONLY public.marketstats
ADD CONSTRAINT marketstats_pkey PRIMARY KEY (marketstatsmark);


ALTER TABLE ONLY public.orderexecutions
ADD CONSTRAINT orderexecutions_pkey PRIMARY KEY (orderexecmark);


ALTER TABLE ONLY public.orders
ADD CONSTRAINT orders_pkey PRIMARY KEY (orderspivot);


ALTER TABLE ONLY public.orders
ADD CONSTRAINT orders_recordvault_key UNIQUE (recordvault);


ALTER TABLE ONLY public.riskandmargin
ADD CONSTRAINT riskandmargin_pkey PRIMARY KEY (riskandmarginpivot);


ALTER TABLE ONLY public.systemmonitoring
ADD CONSTRAINT systemmonitoring_pkey PRIMARY KEY (systemmonitoringpivot);


ALTER TABLE ONLY public.users
ADD CONSTRAINT users_pkey PRIMARY KEY (usersnode);


ALTER TABLE ONLY public.users
ADD CONSTRAINT users_userstamp_key UNIQUE (userstamp);


ALTER TABLE ONLY public.accountbalances
ADD CONSTRAINT accountbalances_usertag_fkey FOREIGN KEY (usertag) REFERENCES public.users(userstamp);


ALTER TABLE ONLY public.analyticsindicators
ADD CONSTRAINT analyticsindicators_mdataref_fkey FOREIGN KEY (mdataref) REFERENCES public.marketdata(marketdatanode);


ALTER TABLE ONLY public.analyticsindicators
ADD CONSTRAINT analyticsindicators_mstatsref_fkey FOREIGN KEY (mstatsref) REFERENCES public.marketstats(marketstatsmark);


ALTER TABLE ONLY public.fees
ADD CONSTRAINT fees_orderslink_fkey FOREIGN KEY (orderslink) REFERENCES public.orders(recordvault);


ALTER TABLE ONLY public.marketstats
ADD CONSTRAINT marketstats_mdlink_fkey FOREIGN KEY (mdlink) REFERENCES public.marketdata(marketdatanode);


ALTER TABLE ONLY public.orderexecutions
ADD CONSTRAINT orderexecutions_ordersmark_fkey FOREIGN KEY (ordersmark) REFERENCES public.orders(recordvault);


ALTER TABLE ONLY public.orders
ADD CONSTRAINT orders_userlink_fkey FOREIGN KEY (userlink) REFERENCES public.users(userstamp);


ALTER TABLE ONLY public.riskandmargin
ADD CONSTRAINT riskandmargin_ordervault_fkey FOREIGN KEY (ordervault) REFERENCES public.orders(recordvault);


ALTER TABLE ONLY public.systemmonitoring
ADD CONSTRAINT systemmonitoring_aitrack_fkey FOREIGN KEY (aitrack) REFERENCES public.analyticsindicators(analyticsindicatorsnode);


GRANT ALL ON SCHEMA public TO PUBLIC;
